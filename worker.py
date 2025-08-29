# worker.py
import os
import io
import re
import json
import hashlib
import logging
import string
from typing import List, Dict, Optional

from flask import Flask, request, jsonify
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import google.oauth2.credentials
from google.cloud import firestore

from programming_analyzer import analyze_programming_submission
from theory_analyzer import analyze_theory_submission
from utils import extract_text_from_file

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

db = firestore.Client()
app = Flask(__name__)

_STOPWORDS = set([
    'the','a','an','and','or','to','of','in','on','for','with','by','from',
    'that','this','it','is','are','as','be','your','student','write','implement','print'
])

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
    marker_regex = re.compile(r'^\s*(?:P?\s?\d+[:.\)]|Part\s*\d+[:.\)]|\([a-zA-Z0-9]\)|[a-zA-Z]\)|Q\d+[:.\)])', re.IGNORECASE)
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
    parts = [p for p in parts if p]
    return parts if parts else [question.strip()]

@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({'status': 'ok'}), 200

@app.route('/process_task', methods=['POST'])
def process_task():
    payload = request.get_json(force=True)
    logger.info("Task payload keys: %s", list(payload.keys()))
    student_id = payload.get('student_id')
    course_id = payload.get('course_id')
    assignment_id = payload.get('assignment_id')
    domain = (payload.get('domain') or 'programming').lower()
    question = payload.get('question', '')
    attachments = payload.get('attachments', []) or []
    credentials_info = payload.get('credentials')

    if not (student_id and course_id and assignment_id and credentials_info is not None):
        return jsonify({'error': 'Missing required fields'}), 400

    doc_id = f"{course_id}-{student_id}-{assignment_id}"
    doc_ref = db.collection('results').document(doc_id)

    try:
        creds = google.oauth2.credentials.Credentials(**credentials_info)
        drive_service = build('drive', 'v3', credentials=creds, cache_discovery=False)
    except Exception as e:
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
        return jsonify({'error': 'drive auth failed'}), 500

    if len(attachments) == 0:
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
        return jsonify({'status': 'no_attachments'}), 200

    ocr_texts = []
    file_hashes = []
    attachment_names = []
    try:
        for att in attachments:
            drive_file = att.get('driveFile') or att.get('drive_file') or att
            file_id = None
            file_title = None
            if isinstance(drive_file, dict):
                file_id = drive_file.get('id')
                file_title = drive_file.get('title') or drive_file.get('name')
            if not file_id:
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
            except Exception:
                mime_type = None

            try:
                text = extract_text_from_file(file_bytes, mime_type)
            except Exception:
                text = ""
            ocr_texts.append(text)
            attachment_names.append(file_title or f"file_{file_id}")
    except Exception as e:
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
        return jsonify({'error': 'download_failed'}), 500

    # exact-file plagiarism
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
                    doc_ref.set({
                        'course_id': course_id,
                        'student_id': student_id,
                        'assignment_id': assignment_id,
                        'accuracy_score': 0.0,
                        'justification': 'Plagiarism detected (exact file match).',
                        'debug_info': json.dumps({'matched_student': prev_student, 'matched_doc_id': docs[0].id, 'matched_hash': fhash}),
                        'file_hashes': file_hashes,
                        'is_plagiarized': True
                    })
                    return jsonify({'status': 'plagiarism_detected'}), 200
    except Exception:
        pass

    # map attachments to parts
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
            best_j = 0
            best_score = -1.0
            for j, qt in enumerate(q_tokens):
                score = len(p_tokens & qt) / max(1, len(qt)) if qt else 0.0
                if score > best_score:
                    best_score = score
                    best_j = j
            mapping[i] = best_j

    part_results: Dict[int, dict] = {}
    part_sources: Dict[int, Optional[int]] = {}

    for att_idx, part_idx in mapping.items():
        try:
            ocr_text = ocr_texts[att_idx]
            if not ocr_text.strip():
                result = {'score': 0.0, 'justification': f'Attachment {attachment_names[att_idx] if att_idx < len(attachment_names) else att_idx}: OCR empty.'}
            else:
                if domain == 'theory':
                    result = analyze_theory_submission(question_parts[part_idx] if part_idx < len(question_parts) else question, ocr_text)
                else:
                    # programming analyzer expects a single-part question and the code blob for that attachment
                    result = analyze_programming_submission(question_parts[part_idx] if part_idx < len(question_parts) else question, ocr_text)
        except Exception as e:
            result = {'score': 0.0, 'justification': f'Analyzer error: {e}'}
        score = float(result.get('score', 0.0))
        if part_idx not in part_results or score > float(part_results[part_idx].get('score', 0.0)):
            part_results[part_idx] = result
            part_sources[part_idx] = att_idx

    for j in range(num_parts):
        if j not in part_results:
            part_results[j] = {'score': 0.0, 'justification': f'Part {j+1}: No submission found.'}
            part_sources[j] = None

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

    payload = {
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
        doc_ref.set(payload)
    except Exception as e:
        return jsonify({'error': 'db_write_failed', 'detail': str(e)}), 500

    return jsonify({'status': 'processed', 'score': final_score}), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080)), debug=False)


