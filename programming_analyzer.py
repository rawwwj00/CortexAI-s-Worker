import os
import json
import re
import docker
import tempfile
import google.generativeai as genai

# Configure the Gemini API client at the module level
try:
    # This will use the GEMINI_API_KEY from the systemd environment
    genai.configure(api_key=os.environ.get("GEMINI_API_KEY"))
    programming_model = genai.GenerativeModel('gemini-1.5-flash-latest')
except Exception as e:
    programming_model = None
@app.route('/process_attachment_task', methods=['POST'])

def process_attachment_task():
    task_data = request.get_json()
    
    user_credentials = google.oauth2.credentials.Credentials(**task_data['credentials'])
    drive_service = build('drive', 'v3', credentials=user_credentials)
    
    student_id = task_data['student_id']
    course_id = task_data['course_id']
    assignment_id = task_data['assignment_id']
    domain = task_data['domain']
    question = task_data['question']
    drive_file = task_data['drive_file']
    
    final_score = 0.0
    final_justification = "Failed to process attachment."

    try:
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
            
            final_score = result.get('score', 0.0)
            final_justification = result.get('justification', 'AI analysis failed.')
        else:
            final_justification = "Unsupported file type or empty file."

    except Exception as e:
        print(f"Worker failed on attachment for student {student_id}. Error: {e}")
        final_justification = "Attachment failed to process due to a critical error."
            
    # Save a result for THIS ATTACHMENT
    # Note: We need a unique doc_id for each attachment
    unique_part = drive_file['id']
    doc_id = f"{course_id}-{student_id}-{assignment_id}-{unique_part}"
    doc_ref = db.collection('results').document(doc_id)
    doc_ref.set({
        'course_id': course_id, 'student_id': student_id, 'assignment_id': assignment_id,
        'accuracy_score': final_score, 'justification': final_justification
    })
    
    return "OK", 200


def analyze_programming_submission(question: str, ocr_code: str) -> dict:
    """
    A fully automated pipeline to analyze a programming submission that may
    contain one or more separate programs. The final score is an average.
    """
    if not programming_model or not ocr_code:
        return {'score': 0.0, 'justification': 'Missing model or student code.'}

    programs = _split_programs(ocr_code)
    if not programs:
        return {'score': 0.0, 'justification': 'No valid programs were found in the submission.'}

    total_score, all_justifications, program_count = 0.0, [], len(programs)

    for i, program_code in enumerate(programs):
        try:
            language = _detect_language(program_code)
            fixed_code = _fix_code(program_code, language)
            test_cases = _generate_test_cases(question, language)
            if not test_cases:
                all_justifications.append(f"P{i+1}: Could not generate test cases.")
                continue

            passed_cases = _run_code_in_docker(fixed_code, language, test_cases)
            score = passed_cases / len(test_cases) if test_cases else 0.0
            
            total_score += score
            all_justifications.append(f"P{i+1}: Passed {passed_cases}/{len(test_cases)} tests.")
        except Exception as e:
            print(f"A critical error occurred during analysis of program {i+1}: {e}")
            all_justifications.append(f"P{i+1}: Analysis failed with a critical error.")
            continue
    
    average_score = total_score / program_count if program_count > 0 else 0.0
    final_justification = " | ".join(all_justifications)
    return {'score': average_score, 'justification': final_justification}

def _detect_language(code: str) -> str:
    """Detects the programming language of the given code snippet."""
    prompt = f"Detect the programming language of the following code. Respond with a single word only from this list: Python, Java, C, C++. \n\nCode:\n```\n{code}\n```"
    response = programming_model.generate_content(prompt)
    return response.text.strip().lower()

def _fix_code(code: str, language: str) -> str:
    """Corrects OCR'd code using an AI model and cleans the output."""
    prompt = f"The following {language} code was extracted from an image using OCR and may contain errors. Please correct it so it is a runnable program. Provide only the corrected code with no explanations.\n\nOCR'd Code:\n```\n{code}\n```"
    response = programming_model.generate_content(prompt)
    cleaned_text = response.text.strip()
    if cleaned_text.startswith("```"):
        cleaned_text = cleaned_text[cleaned_text.find('\n') + 1:]
    if cleaned_text.endswith("```"):
        cleaned_text = cleaned_text[:-3].strip()
    return cleaned_text

def _generate_test_cases(question: str, language: str) -> list:
    """Generates test cases as a JSON object for a given question."""
    prompt = f"""
    Based on the following programming question, generate a list of 5 diverse test cases.
    Provide your response as a single, valid JSON object. The object should be a list of dictionaries,
    where each dictionary has "input" and "expected_output".
    Question: "{question}"
    """
    try:
        response = programming_model.generate_content(prompt)
        json_text = response.text.replace("```json", "").replace("```", "").strip()
        return json.loads(json_text)
    except Exception as e:
        print(f"Failed to generate or parse test cases: {e}")
        return []

def _run_code_in_docker(code: str, language: str, test_cases: list) -> int:
    """Runs student code in a sandboxed Docker container and checks it against test cases."""
    try:
        client = docker.from_env()
    except docker.errors.DockerException:
        print("CRITICAL: Docker daemon is not running on the worker VM.")
        return 0

    passed_count = 0
    for case in test_cases:
        sanitized_input = str(case.get('input', '')).replace("'", "'\\''")
        image, file_name, run_command = "", "", []

        if language == "python":
            image, file_name = "python:3.9-slim", "student_code.py"
            run_command = ["sh", "-c", f"echo '{sanitized_input}' | python -u /app/{file_name}"]
        elif language in ["c++", "c"]:
            image, file_name = "gcc:latest", f"student_code.{'cpp' if language == 'c++' else 'c'}"
            run_command = ["sh", "-c", f"g++ /app/{file_name} -o /app/program && echo '{sanitized_input}' | /app/program"]
        elif language == "java":
            image, file_name = "openjdk:17-slim-bullseye", "Main.java"
            run_command = ["sh", "-c", f"javac /app/{file_name} && echo '{sanitized_input}' | java -cp /app Main"]
        else:
            continue

        with tempfile.TemporaryDirectory() as temp_dir:
            script_path = os.path.join(temp_dir, file_name)
            with open(script_path, "w", encoding="utf-8") as f: f.write(code)
            
            try:
                container_output = client.containers.run(
                    image, command=run_command, volumes={temp_dir: {'bind': '/app', 'mode': 'rw'}},
                    working_dir="/app", remove=True, network_disabled=True, mem_limit='256m'
                ).decode('utf-8').strip()
                
                expected_output = str(case.get('expected_output', '')).strip()

                numbers_from_actual = re.findall(r'-?\d+\.?\d*', container_output)
                numbers_from_expected = re.findall(r'-?\d+\.?\d*', expected_output)

                if numbers_from_expected and numbers_from_actual == numbers_from_expected:
                    passed_count += 1
                # --- THIS IS THE CORRECTED LINE ---
                elif expected_output.lower() in container_output.lower():
                    passed_count += 1
            
            except docker.errors.ContainerError as e:
                print(f"Container error: {e.stderr.decode('utf-8')}")
                continue
            except Exception as e:
                print(f"An unknown execution error occurred: {e}")
                continue
    
    return passed_count

def _split_programs(ocr_text: str) -> list:
    """Uses the AI model to identify and separate multiple programs from a single block of text."""
    if not programming_model or not ocr_text.strip():
        return [ocr_text] # Fallback
    prompt = f"""
    The following text may contain one or more distinct computer programs.
    Separate each complete program into a JSON list of strings under the key "programs".
    If no valid code is found, return an empty list.

    Submission Text:
    "{ocr_text}"
    """
    try:
        response = programming_model.generate_content(prompt)
        json_text = response.text.replace("```json", "").replace("```", "").strip()
        return json.loads(json_text).get("programs", [])
    except Exception:
        return [ocr_text] # Fallback if AI fails

