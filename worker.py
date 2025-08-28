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

@app.route('/process_attachment_task', methods=['POST'])
def process_attachment_task():
    task_data = request.get_json()
    
    student_id = task_data['student_id']
    course_id = task_data['course_id']
    assignment_id = task_data['assignment_id']
    drive_file = task_data['drive_file']
    
    doc_id = f"{course_id}-{student_id}-{assignment_id}-{drive_file['id']}"
    doc_ref = db.collection('results').document(doc_id)

    try:
        user_credentials = google.oauth2.credentials.Credentials(**task_data['credentials'])
        drive_service = build('drive', 'v3', credentials=user_credentials)
        
        # --- PLAGIARISM CHECK LOGIC ---
        file_id = drive_file['id']
        request_file = drive_service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request_file)
        done = False
        while not done: status, done = downloader.next_chunk()
        file_bytes = fh.getvalue()
        
        file_hash = hashlib.sha256(file_bytes).hexdigest()

        query = db.collection('results').where('assignment_id', '==', assignment_id).where('file_hash', '==', file_hash).limit(1)
        existing_docs = list(query.stream())

        if existing_docs:
            original_doc = existing_docs[0].to_dict()
            original_student_id = original_doc.get('student_id')
            if original_student_id != student_id:
                doc_ref.set({
                    'course_id': course_id, 'student_id': student_id, 'assignment_id': assignment_id,
                    'accuracy_score': 0.0, 'justification': 'Plagiarism Detected.',
                    'debug_info': f"File is identical to a submission by student ID: {original_student_id}",
                    'file_hash': file_hash, 'is_plagiarized': True
                })
                return "OK", 200

        # --- END PLAGIARISM CHECK ---

        domain = task_data['domain']
        question = task_data['question']
        mime_type = drive_service.files().get(fileId=file_id, fields='mimeType').execute().get('mimeType')
        
        ocr_text = extract_text_from_file(file_bytes, mime_type)
        
        # (The rest of the analysis logic remains the same)

    except Exception as e:
        # (Error handling is the same)
            
    # Save the result, now including the file_hash
    doc_ref.set({
        'course_id': course_id, 'student_id': student_id, 'assignment_id': assignment_id,
        'accuracy_score': final_score, 'justification': final_justification,
        'debug_info': debug_info, 'file_hash': file_hash, 'is_plagiarized': False
    })
    
    return "OK", 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
