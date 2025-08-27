import os
import json
import re
import docker
import tempfile
import google.generativeai as genai

# Configure the Gemini API client at the module level
try:
    genai.configure(api_key=os.environ.get("GEMINI_API_KEY"))
    programming_model = genai.GenerativeModel('gemini-1.5-pro-latest')
except Exception as e:
    programming_model = None


# --- HELPER FUNCTIONS ---

def _extract_numbers(text: str) -> list:
    return re.findall(r'-?\d+\.?\d*', text)

def _check_if_code_takes_input(code: str, language: str) -> bool:
    prompt = f"Does the following {language} code read from standard input (e.g., using `cin`, `input()`, `Scanner`, `scanf`)? Respond with only the word 'yes' or 'no'."
    try:
        response = programming_model.generate_content(prompt)
        return response.text.strip().lower() == 'yes'
    except Exception as e:
        print(f"Could not check for input: {e}")
        return True

def _analyze_code_conceptually(question: str, code: str, language: str) -> str:
    """Uses AI to review code that doesn't take input and returns a text justification."""
    prompt = f"""
    As an expert programming instructor, briefly evaluate the following student's code based on the assignment question.
    This program does not take standard input, so evaluate it conceptually on correctness and implementation.
    Provide a one-sentence justification for its quality.

    Assignment Question: "{question}"
    Student's {language} Code:
    ```
    {code}
    ```
    """
    try:
        response = programming_model.generate_content(prompt)
        return response.text.strip()
    except Exception as e:
        print(f"Failed to analyze code conceptually: {e}")
        return 'AI conceptual analysis failed.'

def _detect_language(code: str) -> str:
    prompt = f"Detect the programming language of the following code. Respond with a single word only from this list: Python, Java, C, C++. \n\nCode:\n```\n{code}\n```"
    response = programming_model.generate_content(prompt)
    return response.text.strip().lower()

def _fix_code(code: str, language: str) -> str:
    prompt = f"The following {language} code was extracted from an image using OCR. Please correct it so it is a runnable program. Provide only the corrected code with no explanations.\n\nOCR'd Code:\n```\n{code}\n```"
    response = programming_model.generate_content(prompt)
    cleaned_text = response.text.strip()
    if cleaned_text.startswith("```"):
        cleaned_text = cleaned_text[cleaned_text.find('\n') + 1:]
    if cleaned_text.endswith("```"):
        cleaned_text = cleaned_text[:-3].strip()
    return cleaned_text

def _generate_test_cases(question: str, language: str) -> list:
    prompt = f'Based on the following programming question, generate 5 diverse test cases as a JSON list of dictionaries, where each dictionary has "input" and "expected_output".\n\nQuestion: "{question}"'
    try:
        response = programming_model.generate_content(prompt)
        json_text = response.text.replace("```json", "").replace("```", "").strip()
        return json.loads(json_text)
    except Exception as e:
        print(f"Failed to generate or parse test cases: {e}")
        return []

def _run_code_in_docker(code: str, language: str, test_cases: list) -> str:
    """Runs code in Docker and returns a text justification of the test results."""
    try:
        client = docker.from_env()
    except docker.errors.DockerException:
        return "Docker is not running on the worker VM."

    passed_count = 0
    for case in test_cases:
        # ... (Docker run logic remains the same) ...
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
        else: continue
        with tempfile.TemporaryDirectory() as temp_dir:
            script_path = os.path.join(temp_dir, file_name)
            with open(script_path, "w", encoding="utf-8") as f: f.write(code)
            try:
                container_output = client.containers.run(
                    image, command=run_command, volumes={temp_dir: {'bind': '/app', 'mode': 'rw'}},
                    working_dir="/app", remove=True, network_disabled=True, mem_limit='256m'
                ).decode('utf-8')
                expected_output = str(case.get('expected_output', ''))
                numbers_from_actual = _extract_numbers(container_output)
                numbers_from_expected = _extract_numbers(expected_output)
                if numbers_from_expected and numbers_from_actual == numbers_from_expected:
                    passed_count += 1
                elif expected_output.strip().lower() in container_output.strip().lower():
                    passed_count += 1
            except Exception:
                continue
    
    return f"Passed {passed_count}/{len(test_cases)} test cases."

def _split_programs(ocr_text: str) -> list:
    if not programming_model or not ocr_text.strip(): return [ocr_text]
    prompt = f'The following text may contain one or more distinct computer programs. Separate each complete program into a JSON list of strings under the key "programs".\n\nSubmission Text:\n"{ocr_text}"'
    try:
        response = programming_model.generate_content(prompt)
        json_text = response.text.replace("```json", "").replace("```", "").strip()
        return json.loads(json_text).get("programs", [])
    except Exception:
        return [ocr_text]

def _perform_holistic_review(question: str, preliminary_results: str) -> dict:
    """Takes preliminary analysis and gives a final, holistic score and justification."""
    prompt = f"""
    As an expert Computer Science professor, your task is to provide a final grade for a student's submission.
    I have already performed a preliminary analysis of each program the student submitted. Your job is to synthesize these notes into a final, holistic score and justification.

    Provide your response as a single, valid JSON object with "score" (a float from 0.0 to 1.0) and "justification".
    - "score": A final, overall score based on whether the preliminary results collectively meet all requirements of the question.
    - "justification": A final, overall summary explaining the score. Please check the question may have multiple parts which analysis engine treated as separate programs due to which they are mentioned as P1 - P2 etc and 
written a line that one part of all parts are done by the student ignoring that line in each program take the P1 - Leftover lines as output of first part of the question and so on. so taking that in mind analyze the justification such that P1: result belongs to first part of the question 
(don't consider line that only one part of all part is done) and so on like this.

    ---
    Assignment Question:
    "{question}"
    ---
    Preliminary Analysis of Student's Code:
    "{preliminary_results}"
    ---
    """
    try:
        response = programming_model.generate_content(prompt)
        json_text = response.text.replace("```json", "").replace("```", "").strip()
        return json.loads(json_text)
    except Exception as e:
        print(f"Holistic review failed: {e}")
        return {'score': 0.0, 'justification': 'AI failed to perform the final review.'}


# --- MAIN ANALYSIS FUNCTION ---

def analyze_programming_submission(question: str, ocr_code: str) -> dict:
    if not programming_model or not ocr_code:
        return {'score': 0.0, 'justification': 'Missing model or student code.'}

    # Stage 1: Perform preliminary analysis on each program
    programs = _split_programs(ocr_code)
    if not programs:
        return {'score': 0.0, 'justification': 'No valid programs were found.'}

    preliminary_results = []
    for i, program_code in enumerate(programs):
        try:
            language = _detect_language(program_code)
            fixed_code = _fix_code(program_code, language)
            
            takes_input = _check_if_code_takes_input(fixed_code, language)
            
            if takes_input:
                test_cases = _generate_test_cases(question, language)
                if not test_cases:
                    result = "Could not generate test cases."
                else:
                    result = _run_code_in_docker(fixed_code, language, test_cases)
            else:
                result = _analyze_code_conceptually(question, fixed_code, language)
            
            preliminary_results.append(f"Program {i+1}: {result}")
        except Exception as e:
            preliminary_results.append(f"Program {i+1}: Failed with a critical error: {e}")
            continue
    
    # Stage 2: Perform a final, holistic review of the preliminary results
    combined_preliminary_results = "\n".join(preliminary_results)
    
    final_result = _perform_holistic_review(question, combined_preliminary_results)

    # We add the preliminary results as the debug_info for transparency
    final_result['debug_info'] = combined_preliminary_results
    
    return final_result


