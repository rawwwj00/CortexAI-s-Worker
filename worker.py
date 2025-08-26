import os
import io
import json
import google.auth
from google.cloud import firestore
import google.oauth2.credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from flask import Flask, request

from utils import extract_text_from_file
from theory_analyzer import analyze_theory_submission
from programming_analyzer import analyze_programming_submission

# Explicitly use the VM's Service Account for Firestore
fs_credentials, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/datastore"])
db = firestore.Client(credentials=fs_credentials, database="cortex-ai")
app = Flask(__name__)

@app.route('/process_attachment_task', methods=['POST'])
def process_attachment_task():
    task_data = request.get_json()
    
    # Get all identifiers first. If any of these are missing, the task will fail and retry.
    student_id = task_data['student_id']
    course_id = task_data['course_id']
    assignment_id = task_data['assignment_id']
    drive_file = task_data['drive_file']
    
    # Define the unique document ID for this attachment's result
    doc_id = f"{course_id}-{student_id}-{assignment_id}-{drive_file['id']}"
    doc_ref = db.collection('results').document(doc_id)

    # Set default values in case of failure
    final_score = 0.0
    final_justification = "Attachment failed to process due to a critical error."
    debug_info = "An unexpected error occurred in the worker."

    try:
        # Rebuild user credentials and Drive service inside the try block
        user_credentials = google.oauth2.credentials.Credentials(**task_data['credentials'])
        drive_service = build('drive', 'v3', credentials=user_credentials)
        
        domain = task_data['domain']
        question = task_data['question']
        file_id = drive_file['id']
        
        mime_type = drive_service.files().get(fileId=file_id, fields='mimeType').execute().get('mimeType')
        
        request_file = drive_service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request_file)
        done = False
        while not done: status, done = downloader.next_chunk()
        
        ocr_text = extract_text_from_file(fh.getvalue(), mime_type)
        if ocr_text and ocr_text != "Unsupported File Type":
            result = {}
            if domain == 'theory':
                result = analyze_theory_submission(question, ocr_text)
            elif domain == 'programming':
                result = analyze_programming_submission(question, ocr_text)
            
            # If everything succeeds, update the variables with the good results
            final_score = result.get('score', 0.0)
            final_justification = result.get('justification', 'AI analysis failed.')
            debug_info = result.get('debug_info', 'No debug info from analyzer.')
        else:
            final_justification = "Unsupported file type or empty file."
            debug_info = f"MIME Type: {mime_type}. File was empty or could not be read."

    except Exception as e:
        print(f"Worker failed on attachment for student {student_id}. Error: {e}")
        # The default justification is already set, so we just update the debug info
        debug_info = f"Critical error in worker: {e}"
            
    # Save the result to Firestore, whether it succeeded or failed
    doc_ref.set({
        'course_id': course_id, 'student_id': student_id, 'assignment_id': assignment_id,
        'accuracy_score': final_score, 
        'justification': final_justification,
        'debug_info': debug_info
    })
    
    return "OK", 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
