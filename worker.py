"""
worker.py

Flask worker that:
- Accepts task payloads from app engine (POST /process_task)
- Downloads attachments from Google Drive using the provided user credentials
- Runs OCR on attachments (via utils.extract_text_from_file)
- Maps attachments to question parts, calls analyzers per-part
- Detects exact-file plagiarism via SHA256 hash
- Writes a single result document to Firestore with id: {course_id}-{student_id}-{assignment_id}

Requires:
- google-auth, google-api-python-client, google-cloud-firestore
- flask
- your local modules: programming_analyzer, theory_analyzer, utils
"""

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

# local project modules (make sure these exist)
from programming_analyzer import analyze_programming_submission
from theory_analyzer import analyze_theory_submission
from utils import extract_text_from_file  # must return text given bytes and mime type

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Firestore client (ensure GOOGLE_APPLICATION_CREDENTIALS or environment is set appropriately)
db = firestore.Client()

app = Flask(__name__)

# -------------------------
# Helper text processing
# -------------------------
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
    """
    Heuristic splitter for multi-part assignment questions.
    Returns a list of parts (strings). If no explicit markers found, returns [question].
    """
    if not question or not question.strip():
        return [question or ""]
    # Break into lines, try to detect markers at line starts.
    lines = [l.rstrip() for l in question.splitlines() if l.strip()]
    parts = []
    current = []
    marker_regex = re.compile(r'^\s*(?:P?\s?\d+[:.\)]|Part\s*\d+[:.\)]|\([a-zA-Z0-9]\)|[a-zA-Z]\)|Q\d+[:.\)])', re.IGNORECASE)

    for line in lines:
        if marker_regex.match(line):
            # start of a new part
            if current:
                parts.append(" ".join(current).strip())
            # remove leading marker tokens
            cleaned = re.sub(marker_regex, '', line).strip()
            current = [cleaned]
        else:
            current.append(line)
    if current:
        parts.append(" ".join(current).strip())

    # if we didn't find meaningful parts, return the whole question as single part
    if len(parts) <= 1:
        return [question.strip()]
    # filter empty
    parts = [p for p in parts if p]
    return parts if parts else [question.strip()]

# -------------------------
# Main processing endpoint
# -------------------------
@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({'status': 'ok'}), 200

@app.route('/process_task', methods=['POST'])
def process_task():
    """
    Expected JSON body (example from app.py):
    {
      "student_id": "s123",
      "course_id": "c101",
      "assignment_id": "a202",
      "domain": "programming" or "theory",
      "question": "<teacher question text>",
      "attachments": [ { "driveFile": {"id": "xxx", "title": "P1.cpp"} , ... }, ... ],
      "credentials": { <google oauth2 credentials dict> }
    }
    """
    payload = request.get_json(force=True)
    logger.info("Received task payload: %s", {k: payload.get(k) for k in ['student_id','course_id','assignment_id','domain']})

    student_id = payload.get('student_id')
    course_id = payload.get('course_id')
    assignment_id = payload.get('assignment_id')
    domain = (payload.get('domain') or 'programming').lower()
    question = payload.get('question', '')
    attachments = payload.get('attachments', []) or []
    credentials_info = payload.get('credentials')

    if not (student_id and course_id and assignment_id and credentials_info is not None):
        logger.error("Missing required fields in payload.")
        return jsonify({'error': 'Missing required fields'}), 400

    doc_id = f"{course_id}-{student_id}-{assignment_id}"
    doc_ref = db.collection('results').document(doc_id)

    try:
        # Construct Drive service using the user's credentials passed with the task.
        creds = google.oauth2.credentials.Credentials(**credentials_info)
        drive_service = build('drive', 'v3', credentials=creds, cache_discovery=False)
    except Exception as e:
        logger.exception("Failed to build drive service: %s", e)
        # Save a failure doc so UI knows something happened
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

    # If no attachments, explicit 0-result and return
    if len(attachments) == 0:
        logger.info("No attachments for student %s -> writing zero doc.", student_id)
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

    # Download attachments and run OCR
    ocr_texts = []        # list of OCR text, each item maps to attachments order
    file_hashes = []      # for plagiarism detection
    attachment_names = [] # keep titles for filename-based mapping hint
    try:
        for att in attachments:
            # handle both Classroom-style {driveFile: {id: ..., title: ...}} and raw drive file dicts
            drive_file = att.get('driveFile') or att.get('drive_file') or att
            file_id = None
            file_title = None
            if isinstance(drive_file, dict):
                file_id = drive_file.get('id')
                file_title = drive_file.get('title') or drive_file.get('name')
            if not file_id:
                logger.warning("Skipping attachment with no drive file id: %s", drive_file)
                continue

            # download file bytes
            request_media = drive_service.files().get_media(fileId=file_id)
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, request_media)
            done = False
            while not done:
                status, done = downloader.next_chunk()
            file_bytes = fh.getvalue()

            # compute file hash for plagiarism detection
            file_hash = hashlib.sha256(file_bytes).hexdigest()
            file_hashes.append(file_hash)

            # get mime type (try a metadata call)
            try:
                meta = drive_service.files().get(fileId=file_id, fields='mimeType,name').execute()
                mime_type = meta.get('mimeType')
                if not file_title:
                    file_title = meta.get('name')
            except Exception:
                mime_type = None

            # Extract text (utils.extract_text_from_file should handle mime types)
            try:
                text = extract_text_from_file(file_bytes, mime_type)
            except Exception as e:
                logger.exception("OCR failed for file %s: %s", file_id, e)
                text = ""  # continue; analyzer will treat missing text as no submission
            ocr_texts.append(text)
            attachment_names.append(file_title or f"file_{file_id}")

    except Exception as e:
        logger.exception("Failed while downloading attachments: %s", e)
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

    # Quick exact-file plagiarism check: if any previous submission for the same assignment has this file_hash, mark plagiarized.
    try:
        plag_detected = False
        plag_info = None
        for fhash in file_hashes:
            q = db.collection('results').where('assignment_id', '==', assignment_id).where('file_hashes', 'array_contains', fhash).limit(1)
            docs = list(q.stream())
            if docs:
                prev = docs[0].to_dict()
                prev_student = prev.get('student_id')
                if prev_student and prev_student != student_id:
                    plag_detected = True
                    plag_info = {
                        'matched_student': prev_student,
                        'matched_doc_id': docs[0].id,
                        'matched_hash': fhash
                    }
                    break
        if plag_detected:
            logger.info("Plagiarism detected for %s -> matched %s", student_id, plag_info)
            doc_ref.set({
                'course_id': course_id,
                'student_id': student_id,
                'assignment_id': assignment_id,
                'accuracy_score': 0.0,
                'justification': 'Plagiarism detected (exact file match).',
                'debug_info': json.dumps(plag_info),
                'file_hashes': file_hashes,
                'is_plagiarized': True
            })
            return jsonify({'status': 'plagiarism_detected'}), 200
    except Exception as e:
        logger.exception("Plagiarism check failed: %s", e)
        # proceed without failing - not a blocker

    # Map attachments -> question parts
    question_parts = split_question_into_parts(question)
    num_parts = len(question_parts)
    logger.info("Detected %d question parts.", num_parts)

    # If the number of attachments equals number of parts, do a straight 1:1 mapping by order.
    mapping = {}           # attachment_index -> part_index
    assigned_parts = set()
    if len(ocr_texts) == num_parts:
        for i in range(num_parts):
            mapping[i] = i
            assigned_parts.add(i)
    else:
        # Use token overlap to map each attachment to the best-fitting part
        q_tokens = [token_set(qp) for qp in question_parts]
        for i, txt in enumerate(ocr_texts):
            p_tokens = token_set(txt)
            best_score = -1.0
            best_j = 0
            for j, qt in enumerate(q_tokens):
                score = len(p_tokens & qt) / max(1, len(qt))
                if score > best_score:
                    best_score = score
                    best_j = j
            mapping[i] = best_j
            assigned_parts.add(best_j)

    # Now call analyzers per mapped item; for programming pass the *single part* as question
    part_results: Dict[int, dict] = {}
    part_sources: Dict[int, Optional[int]] = {}

    for att_index, part_index in mapping.items():
        try:
            ocr_text = ocr_texts[att_index]
            # If the OCR is empty, treat as no submission for that attachment
            if not ocr_text.strip():
                result = {'score': 0.0, 'justification': f'Attachment {attachment_names[att_index] or att_index}: OCR returned empty.'}
            else:
                # Choose analyzer
                if domain == 'theory':
                    # theory analyzer expects the question + ocr
                    result = analyze_theory_submission(question_parts[part_index] if part_index < len(question_parts) else question, ocr_text)
                else:
                    # programming analyzer expects the specific question part text and the code for that attachment
                    result = analyze_programming_submission(question_parts[part_index] if part_index < len(question_parts) else question, ocr_text)
        except Exception as e:
            logger.exception("Analyzer failed for attachment %s: %s", att_index, e)
            result = {'score': 0.0, 'justification': f'Analyzer error: {str(e)}'}

        score = float(result.get('score', 0.0))
        # If multiple attachments map to the same part, keep the one with the highest score
        if part_index not in part_results or score > float(part_results[part_index].get('score', 0.0)):
            part_results[part_index] = result
            part_sources[part_index] = att_index

    # For any question parts that never received a mapped result, add explicit 0
    for j in range(num_parts):
        if j not in part_results:
            part_results[j] = {'score': 0.0, 'justification': f'Part {j+1}: No submission found.'}
            part_sources[j] = None

    # Aggregate final score and build concise justification
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

    # Save result doc
    try:
        doc_payload = {
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
        doc_ref.set(doc_payload)
    except Exception as e:
        logger.exception("Failed to write result to Firestore: %s", e)
        return jsonify({'error': 'db_write_failed'}), 500

    logger.info("Processed student %s assignment %s => score %.3f", student_id, assignment_id, final_score)
    return jsonify({'status': 'processed', 'score': final_score}), 200


if __name__ == '__main__':
    # When running on the VM, let gunicorn or your supervisor run this. For local testing:
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080)), debug=False)
