import os
import io
import json
import hashlib
import google.auth
from google.cloud import firestore
import google.oauth2.credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from flask import Flask, request

from utils import extract_text_from_file
from theory_analyzer import analyze_theory_submission
from programming_analyzer import analyze_programming_submission

fs_credentials, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/datastore"])
db = firestore.Client(credentials=fs_credentials, database="cortex-ai")
app = Flask(__name__)

@app.route('/process_task', methods=['POST'])
def process_task():
    task_data = request.get_json()
    
    student_id = task_data['student_id']
    course_id = task_data['course_id']
    assignment_id = task_data['assignment_id']
    attachments = task_data.get('attachments', [])  # list of attachment objects
    doc_id = f"{course_id}-{student_id}-{assignment_id}"
    doc_ref = db.collection('results').document(doc_id)

    try:
        user_credentials = google.oauth2.credentials.Credentials(**task_data['credentials'])
        drive_service = build('drive', 'v3', credentials=user_credentials)

        if not attachments:
            # No attachments -> store zero and return
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
            return "OK", 200

        ocr_texts = []
        file_hashes = []
        # Plagiarism early check: if any file identical to other student's file -> mark plagiarized
        for att in attachments:
            # attachments from Classroom are often like {'driveFile': {'id': '...', 'title': '...'}, ...}
            drive_file = att.get('driveFile') or att.get('drive_file') or att
            file_id = drive_file.get('id')
            if not file_id:
                continue

            request_file = drive_service.files().get_media(fileId=file_id)
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, request_file)
            done = False
            while not done:
                status, done = downloader.next_chunk()
            file_bytes = fh.getvalue()
            file_hash = hashlib.sha256(file_bytes).hexdigest()
            file_hashes.append(file_hash)

            # OCR
            mime_type = drive_service.files().get(fileId=file_id, fields='mimeType').execute().get('mimeType')
            text = extract_text_from_file(file_bytes, mime_type)
            ocr_texts.append(text)

            # check per-file plagiarism
            query = db.collection('results').where('assignment_id', '==', assignment_id).where('file_hash', '==', file_hash).limit(1)
            existing_docs = list(query.stream())
            if existing_docs:
                original_doc = existing_docs[0].to_dict()
                original_student_id = original_doc.get('student_id')
                if original_student_id != student_id:
                    # mark plagiarized and save doc
                    doc_ref.set({
                        'course_id': course_id, 'student_id': student_id, 'assignment_id': assignment_id,
                        'accuracy_score': 0.0, 'justification': 'Plagiarism Detected.',
                        'debug_info': f"File is identical to a submission by student ID: {original_student_id}",
                        'file_hash': file_hash, 'is_plagiarized': True
                    })
                    return "OK", 200

        # Combine all OCR texts (separators keep file boundaries)
        combined_code = "\n\n--- FILE BREAK ---\n\n".join(ocr_texts)

        domain = task_data.get('domain', 'programming')
        question = task_data.get('question', '')

        final_score = 0.0
        final_justification = "No analysis performed."
        debug_info = ""

        if domain.lower() == 'theory':
            # For multiple attachments, grade each answer and choose the average
            scores = []
            justs = []
            for text in ocr_texts:
                res = analyze_theory_submission(question, text)
                scores.append(float(res.get('score', 0.0)))
                justs.append(res.get('justification', ''))
            final_score = sum(scores) / len(scores) if scores else 0.0
            final_justification = " | ".join(justs)
        else:
            # programming: pass combined code to improved analyzer that will map programs->parts
            res = analyze_programming_submission(question, combined_code)
            final_score = float(res.get('score', 0.0))
            final_justification = res.get('justification', '')
            debug_info = res.get('debug_info', '')

        # Save the result using student-level doc id (consistent with app.py)
        doc_ref.set({
            'course_id': course_id, 'student_id': student_id, 'assignment_id': assignment_id,
            'accuracy_score': final_score, 'justification': final_justification,
            'debug_info': debug_info, 'file_hashes': file_hashes, 'is_plagiarized': False
        })

    except Exception as e:
        # try to save an error doc for debugging
        doc_ref.set({
            'course_id': course_id, 'student_id': student_id, 'assignment_id': assignment_id,
            'accuracy_score': 0.0, 'justification': 'Internal worker error during analysis.',
            'debug_info': str(e), 'file_hashes': file_hashes if 'file_hashes' in locals() else [], 'is_plagiarized': False
        })
        return f"ERROR: {e}", 500

    return "OK", 200


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)


