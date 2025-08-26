# worker.py
import os
import io
import json
import google.auth
from google.cloud import firestore
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from flask import Flask, request

# --- Local Module Imports ---
from utils import extract_text_from_file
from theory_analyzer import analyze_theory_submission
from programming_analyzer import analyze_programming_submission

# --- Initialization ---
# These clients will now AUTOMATICALLY use the VM's powerful service account
db = firestore.Client(database="cortex-ai")
credentials, _ = google.auth.default()
drive_service = build('drive', 'v3', credentials=credentials)

app = Flask(__name__)

@app.route('/process_task', methods=['POST'])
def process_task():
    task_data = request.get_json()
    
    # We no longer need to rebuild credentials here
    
    student_id = task_data['student_id']
    course_id = task_data['course_id']
    assignment_id = task_data['assignment_id']
    domain = task_data['domain']
    question = task_data['question']
    sub = task_data['submission_details']

    attachments = sub.get('assignmentSubmission', {}).get('attachments', [])
    total_score, count, justifications = 0.0, 0, []

    for attachment in attachments:
        try:
            drive_file = attachment.get('driveFile')
            if not drive_file: continue
            
            file_id = drive_file['id']
            # The global drive_service is used here
            mime_type = drive_service.files().get(fileId=file_id, fields='mimeType').execute().get('mimeType')
            
            request_file = drive_service.files().get_media(fileId=file_id)
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, request_file)
            done = False
            while not done: status, done = downloader.next_chunk()
            
            ocr_text = extract_text_from_file(fh.getvalue(), mime_type)
            if not ocr_text: continue
            
            result = {}
            if domain == 'theory':
                result = analyze_theory_submission(question, ocr_text)
            elif domain == 'programming':
                result = analyze_programming_submission(question, ocr_text)
            
            total_score += result.get('score', 0.0)
            justifications.append(result.get('justification', ''))
            count += 1
        except Exception as e:
            print(f"Worker failed on attachment for student {student_id}. Error: {e}")
            
    final_score = total_score / count if count > 0 else 0.0
    final_justification = " | ".join(justifications) if justifications else "No processable attachments found."

    doc_id = f"{course_id}-{student_id}-{assignment_id}"
    doc_ref = db.collection('results').document(doc_id)
    doc_ref.set({
        'course_id': course_id, 'student_id': student_id, 'assignment_id': assignment_id,
        'accuracy_score': final_score, 'justification': final_justification
    })
    
    return "OK", 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
