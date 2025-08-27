import os
import io
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

@app.route('/process_task', methods=['POST'])
def process_task():
    task_data = request.get_json()
    
    # --- Extract data from the incoming task ---
    credentials = task_data['credentials']
    student_id = task_data['student_id']
    course_id = task_data['course_id']
    assignment_id = task_data['assignment_id']
    domain = task_data['domain']
    question = task_data['question']
    attachments = task_data.get('attachments', []) # Expect a list of attachments

    user_credentials = google.oauth2.credentials.Credentials(**credentials)
    drive_service = build('drive', 'v3', credentials=user_credentials)
    
    final_score = 0.0
    final_justification = "No valid files found in submission."
    combined_ocr_text = []

    # --- MODIFIED: Loop through all attachments for the student ---
    for attachment in attachments:
        try:
            drive_file = attachment.get('driveFile')
            if not drive_file: continue
            
            file_id = drive_file['id']
            mime_type = drive_service.files().get(fileId=file_id, fields='mimeType').execute().get('mimeType')
            
            request_file = drive_service.files().get_media(fileId=file_id)
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, request_file)
            done = False
            while not done: status, done = downloader.next_chunk()
            
            ocr_text = extract_text_from_file(fh.getvalue(), mime_type)
            if ocr_text and ocr_text != "Unsupported File Type":
                combined_ocr_text.append(ocr_text)

        except Exception as e:
            print(f"Worker failed on one attachment for student {student_id}. Error: {e}")
            continue # Skip this attachment and move to the next

    # --- MODIFIED: Analyze the combined text from all files ---
    if combined_ocr_text:
        # Join all extracted text into one block for a single analysis
        full_submission_text = "\n\n--- Next File ---\n\n".join(combined_ocr_text)
        
        result = {}
        if domain == 'theory':
            result = analyze_theory_submission(question, full_submission_text)
        elif domain == 'programming':
            result = analyze_programming_submission(question, full_submission_text)
        
        final_score = result.get('score', 0.0)
        final_justification = result.get('justification', 'AI analysis failed.')

    # --- MODIFIED: Save a single, final result for the student ---
    doc_id = f"{course_id}-{student_id}-{assignment_id}"
    doc_ref = db.collection('results').document(doc_id)
    doc_ref.set({
        'course_id': course_id, 
        'student_id': student_id, 
        'assignment_id': assignment_id,
        'accuracy_score': final_score, 
        'justification': final_justification
    })
    
    return "OK", 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
