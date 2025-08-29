# worker.py
import os
import io
import re
import json
import hashlib
import logging
import string
import traceback
from typing import List, Dict, Optional

from flask import Flask, request, jsonify
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import google.oauth2.credentials
from google.cloud import firestore

from programming_analyzer import analyze_programming_submission
from theory_analyzer import analyze_theory_submission
from utils import extract_text_from_file


# ----------------- Logging -----------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ----------------- Firestore -----------------
db = firestore.Client()

# ----------------- Flask -----------------
app = Flask(__name__)

# ----------------- Stopwords -----------------
_STOPWORDS = set([
    'the','a','an','and','or','to','of','in','on','for','with','by','from',
    'that','this','it','is','are','as','be','your','student','write','implement','print'
])

# ----------------- Helpers -----------------
def token_set(text: str) -> set:
    if not text:
        return set()
    t = text.lower()
    t = t.translate(str.maketrans(string.punctuation, ' '*len(string.punctuation)))
    toks = [w for w in t.split() if w and w not in _STOPWORDS]
    return set(toks)

def split_question_into_parts(question: str) -> List[str]:
    if not question or not question.strip():
        return [question or ""]
    lines = [l.rstrip() for l in question.splitlines() if l.strip()]
    parts = []
    current = []
    marker_regex = re.compile(
        r'^\s*(?:P?\s?\d+[:.\)]|Part\s*\d+[:.\)]|\([a-zA-Z0-9]\)|[a-zA-Z]\)|Q\d+[:.\)])',
        re.IGNORECASE
    )
    for line in lines:
        if marker_regex.match(line):
            if current:
                parts.append(" ".join(current).strip())
            cleaned = re.sub(marker_regex, '', line).strip()
            current = [cleaned]
        else:
            current.append(line)
    if current:
        parts.append(" ".join(current).strip())
    if len(parts) <= 1:
        return [question.strip()]
    return [p for p in parts if p] or [question.strip()]

# ----------------- Routes -----------------
@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({'status': 'ok'}), 200


def run_task_logic(payload: dict) -> dict:
    logger.info("Task payload keys: %s", list(payload.keys()))

    student_id = payload.get('student_id')
    course_id = payload.get('course_id')
    assignment_id = payload.get('assignment_id')
    domain = (payload.get('domain') or 'programming').lower()
    question = payload.get('question', '')
    attachments = payload.get('attachments', []) or []
    credentials_info = payload.get('credentials')

    if not (student_id and course_id and assignment_id and credentials_info is not None):
        logger.error("Missing required fields in payload")
        return {'error': 'Missing required fields'}, 400

    doc_id = f"{course_id}-{student_id}-{assignment_id}"
    doc_ref = db.collection('results').document(doc_id)

    # ----------------- Drive Auth -----------------
    try:
        creds = google.oauth2.credentials.Credentials(**credentials_info)
        drive_service = build('drive', 'v3', credentials=creds, cache_discovery=False)
    except Exception as e:
        logger.error(f"Drive auth failed: {e}")
        logger.error(traceback.format_exc())
        doc_ref.set({
            'course_id': course_id,
            'student_id': student_id,
            'assignment_id': assignment_id,
            'accuracy_score': 0.0,
            'justification': 'Worker failed to authenticate to Drive.',
            'debug_info': str(e),
            'file_hashes': [],
            'is_plagiarized': False
        })
        return {'error': 'drive auth failed'}, 500

    if len(attachments) == 0:
        logger.warning("No attachments found for assignment %s", assignment_id)
        doc_ref.set({
            'course_id': course_id,
            'student_id': student_id,
            'assignment_id': assignment_id,
            'accuracy_score': 0.0,
            'justification': "No submission found.",
            'debug_info': '',
            'file_hashes': [],
            'is_plagiarized': False
        })
        return {'status': 'no_attachments'}, 200

    # ----------------- Download Attachments -----------------
    ocr_texts, file_hashes, attachment_names = [], [], []
    try:
        for att in attachments:
            drive_file = att.get('driveFile') or att.get('drive_file') or att
            file_id = None
            file_title = None
            if isinstance(drive_file, dict):
                file_id = drive_file.get('id')
                file_title = drive_file.get('title') or drive_file.get('name')
            if not file_id:
                logger.warning("Attachment missing file_id, skipping: %s", att)
                continue

            request_media = drive_service.files().get_media(fileId=file_id)
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, request_media)
            done = False
            while not done:
                status, done = downloader.next_chunk()
            file_bytes = fh.getvalue()

            fhash = hashlib.sha256(file_bytes).hexdigest()
            file_hashes.append(fhash)

            try:
                meta = drive_service.files().get(fileId=file_id, fields='mimeType,name').execute()
                mime_type = meta.get('mimeType')
                if not file_title:
                    file_title = meta.get('name')
            except Exception as e:
                logger.error(f"Metadata fetch failed for {file_id}: {e}")
                mime_type = None

            try:
                text = extract_text_from_file(file_bytes, mime_type)
            except Exception as e:
                logger.error(f"Text extraction failed for {file_title}: {e}")
                logger.error(traceback.format_exc())
                text = ""
            ocr_texts.append(text)
            attachment_names.append(file_title or f"file_{file_id}")
    except Exception as e:
        logger.error(f"Attachment download failed: {e}")
        logger.error(traceback.format_exc())
        doc_ref.set({
            'course_id': course_id,
            'student_id': student_id,
            'assignment_id': assignment_id,
            'accuracy_score': 0.0,
            'justification': 'Worker failed while downloading attachments.',
            'debug_info': str(e),
            'file_hashes': file_hashes,
            'is_plagiarized': False
        })
        return {'error': 'download_failed'}, 500

    # ----------------- Exact File Plagiarism -----------------
    try:
        for fhash in file_hashes:
            q = (
                db.collection('results')
                .where(filter={"field_path": "assignment_id", "op_string": "==", "value": assignment_id})
                .where(filter={"field_path": "file_hashes", "op_string": "array_contains", "value": fhash})
                .limit(1)
            )
            docs = list(q.stream())
            if docs:
                prev = docs[0].to_dict()
                prev_student = prev.get('student_id')
                if prev_student and prev_student != student_id:
                    logger.warning("Plagiarism detected: %s matches %s", student_id, prev_student)
                    doc_ref.set({
                        'course_id': course_id,
                        'student_id': student_id,
                        'assignment_id': assignment_id,
                        'accuracy_score': 0.0,
                        'justification': 'Plagiarism detected (exact file match).',
                        'debug_info': json.dumps({
                            'matched_student': prev_student,
                            'matched_doc_id': docs[0].id,
                            'matched_hash': fhash
                        }),
                        'file_hashes': file_hashes,
                        'is_plagiarized': True
                    })
                    return {'status': 'plagiarism_detected'}, 200
    except Exception as e:
        logger.error(f"Plagiarism check failed: {e}")
        logger.error(traceback.format_exc())

    # ----------------- Match Attachments to Question Parts -----------------
    question_parts = split_question_into_parts(question)
    num_parts = len(question_parts)

    mapping = {}
    if len(ocr_texts) == num_parts:
        for i in range(num_parts):
            mapping[i] = i
    else:
        q_tokens = [token_set(qp) for qp in question_parts]
        for i, txt in enumerate(ocr_texts):
            p_tokens = token_set(txt)
            best_j, best_score = 0, -1.0
            for j, qt in enumerate(q_tokens):
                score = len(p_tokens & qt) / max(1, len(qt)) if qt else 0.0
                if score > best_score:
                    best_score, best_j = score, j
            mapping[i] = best_j

    # ----------------- Analyze Submissions -----------------
    part_results: Dict[int, dict] = {}
    part_sources: Dict[int, Optional[int]] = {}

    for att_idx, part_idx in mapping.items():
        try:
            ocr_text = ocr_texts[att_idx]
            if not ocr_text.strip():
                result = {
                    'score': 0.0,
                    'justification': f'Attachment {attachment_names[att_idx]} OCR empty.'
                }
            else:
                if domain == 'theory':
                    result = analyze_theory_submission(
                        question_parts[part_idx] if part_idx < len(question_parts) else question,
                        ocr_text
                    )
                else:
                    result = analyze_programming_submission(
                        question_parts[part_idx] if part_idx < len(question_parts) else question,
                        ocr_text
                    )
        except Exception as e:
            logger.error(f"Analyzer failed for attachment {att_idx}: {e}")
            logger.error(traceback.format_exc())
            result = {'score': 0.0, 'justification': f'Analyzer error: {e}'}

        score = float(result.get('score', 0.0))
        if part_idx not in part_results or score > float(part_results[part_idx].get('score', 0.0)):
            part_results[part_idx] = result
            part_sources[part_idx] = att_idx

    # ----------------- Fill Missing Parts -----------------
    for j in range(num_parts):
        if j not in part_results:
            part_results[j] = {'score': 0.0, 'justification': f'Part {j+1}: No submission found.'}
            part_sources[j] = None

    # ----------------- Final Score -----------------
    part_scores = [float(part_results[j].get('score', 0.0)) for j in range(num_parts)]
    final_score = (sum(part_scores) / len(part_scores)) if part_scores else 0.0

    per_part_justifications = []
    for j in range(num_parts):
        just = part_results[j].get('justification', '')
        src_idx = part_sources.get(j)
        src_name = attachment_names[src_idx] if (src_idx is not None and src_idx < len(attachment_names)) else None
        header = f"P{j+1}"
        if src_name:
            header += f" (from {src_name})"
        per_part_justifications.append(f"{header}: {just}")

    final_justification = " | ".join(per_part_justifications)

    result_payload = {
        'course_id': course_id,
        'student_id': student_id,
        'assignment_id': assignment_id,
        'accuracy_score': final_score,
        'justification': final_justification,
        'part_results': part_results,
        'part_sources': part_sources,
        'file_hashes': file_hashes,
        'is_plagiarized': False
    }

    try:
        doc_ref.set(result_payload)
    except Exception as e:
        logger.error(f"Firestore write failed: {e}")
        logger.error(traceback.format_exc())
        return {'error': 'db_write_failed', 'detail': str(e)}, 500

    logger.info(f"Task completed: {student_id}-{assignment_id}, score={final_score}")
    return {'status': 'processed', 'score': final_score}, 200


@app.route('/task', methods=['POST'])
def handle_task():
    try:
        payload = request.get_json(force=True)
        result, status = run_task_logic(payload)
        return jsonify(result), status
    except Exception as e:
        logger.error(f"Task failed: {e}")
        logger.error(traceback.format_exc())
        return jsonify({'error': str(e)}), 500


# ----------------- Main -----------------
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080)), debug=False)
