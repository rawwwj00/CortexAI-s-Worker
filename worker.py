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
    # ... (code to get task_data and build drive_service is the same) ...
    
    final_score = 0.0
    final_justification = "Failed to process attachment."
    debug_info = "No debug information generated."

    try:
        # ... (code to download file and get ocr_text is the same) ...
        
        if ocr_text and ocr_text != "Unsupported File Type":
            result = {}
            if domain == 'theory':
                result = analyze_theory_submission(question, ocr_text)
            elif domain == 'programming':
                result = analyze_programming_submission(question, ocr_text)
            
            final_score = result.get('score', 0.0)
            final_justification = result.get('justification', 'AI analysis failed.')
            debug_info = result.get('debug_info', 'No debug info from analyzer.')
        else:
            final_justification = "Unsupported file type or empty file."
            debug_info = f"MIME Type: {mime_type}. File was empty or could not be read."

    except Exception as e:
        final_justification = "Attachment failed to process due to a critical error."
        debug_info = f"Critical error in worker: {e}"
            
    # Save a result for THIS ATTACHMENT, now including the debug info
    unique_part = drive_file['id']
    doc_id = f"{course_id}-{student_id}-{assignment_id}-{unique_part}"
    doc_ref = db.collection('results').document(doc_id)
    doc_ref.set({
        'course_id': course_id, 'student_id': student_id, 'assignment_id': assignment_id,
        'accuracy_score': final_score, 
        'justification': final_justification,
        'debug_info': debug_info # <-- NEW FIELD
    })
    
    return "OK", 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)



