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


# --- HELPER FUNCTIONS (DEFINED FIRST) ---

def _extract_numbers(text: str) -> list:
    """Finds all integer and floating-point numbers in a string and returns them as a list of strings."""
    return re.findall(r'-?\d+\.?\d*', text)

def _check_if_code_takes_input(code: str, language: str) -> bool:
    """Uses AI to determine if a code snippet reads from standard input."""
    prompt = f"Does the following {language} code read from standard input (e.g., using `cin`, `input()`, `Scanner`, `scanf`)? Respond with only the word 'yes' or 'no'."
    try:
        response = programming_model.generate_content(prompt)
        return response.text.strip().lower() == 'yes'
    except Exception as e:
        print(f"Could not check for input: {e}")
        return True

def _analyze_code_conceptually(question: str, code: str, language: str) -> dict:
    """Uses AI to review code that doesn't take input based on its logic and correctness."""
    prompt = f"""
    As an expert programming instructor, your task is to evaluate a student's code based on the assignment question.
    This program does not take standard input, so you must evaluate it conceptually.

    Provide your response as a single, valid JSON object with "score" (a float from 0.0 to 1.0) and "justification".
    - "score": Grade based on correctness, efficiency, and adherence to the question.
    - "justification": A brief, one-sentence explanation for your score.

    ---
    Assignment Question: "{question}"
    ---
    Student's {language} Code:
    ```
    {code}
    ```
    ---
    """
    try:
        response = programming_model.generate_content(prompt)
        json_text = response.text.replace("```json", "").replace("```", "").strip()
        return json.loads(json_text)
    except Exception as e:
        print(f"Failed to analyze code conceptually: {e}")
        return {'score': 0.0, 'justification': 'AI conceptual analysis failed.'}

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
    prompt = f'Based on the following programming question, generate a list of 5 diverse test cases. Provide your response as a single, valid JSON object. The object should be a list of dictionaries, where each dictionary has "input" and "expected_output".\n\nQuestion: "{question}"'
    try:
        response = programming_model.generate_content(prompt)
        json_text = response.text.replace("```json", "").replace("```", "").strip()
        return json.loads(json_text)
    except Exception as e:
        print(f"Failed to generate or parse test cases: {e}")
        return []

def _run_code_in_docker(code: str, language: str, test_cases: list) -> int:
    """Runs student code in a sandboxed Docker container with robust checking."""
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
                ).decode('utf-8')
                
                expected_output = str(case.get('expected_output', ''))
                
                numbers_from_actual = _extract_numbers(container_output)
                numbers_from_expected = _extract_numbers(expected_output)
                
                if numbers_from_expected and numbers_from_actual == numbers_from_expected:
                    passed_count += 1
                elif expected_output.strip().lower() in container_output.strip().lower():
                    passed_count += 1
            
            except docker.errors.ContainerError as e:
                print(f"Container error: {e.stderr.decode('utf-8')}")
                continue
            except Exception as e:
                print(f"An unknown execution error occurred: {e}")
                continue
    
    return passed_count

def _split_programs(ocr_text: str) -> list:
    if not programming_model or not ocr_text.strip(): return [ocr_text]
    prompt = f'The following text may contain one or more distinct computer programs. Separate each complete program into a JSON list of strings under the key "programs". If no valid code is found, return an empty list.\n\nSubmission Text:\n"{ocr_text}"'
    try:
        response = programming_model.generate_content(prompt)
        json_text = response.text.replace("```json", "").replace("```", "").strip()
        return json.loads(json_text).get("programs", [])
    except Exception:
        return [ocr_text]

# --- MAIN ANALYSIS FUNCTION (NOW WITH CONDITIONAL LOGIC) ---

def analyze_programming_submission(question: str, ocr_code: str) -> dict:
    if not programming_model or not ocr_code:
        return {'score': 0.0, 'justification': 'Missing model or student code.'}

    programs = _split_programs(ocr_code)
    if not programs:
        return {'score': 0.0, 'justification': 'No valid programs were found in the submission.'}

    total_score, all_justifications = 0.0, []

    for i, program_code in enumerate(programs):
        justification_prefix = f"P{i+1}"
        try:
            language = _detect_language(program_code)
            fixed_code = _fix_code(program_code, language)
            
            takes_input = _check_if_code_takes_input(fixed_code, language)
            
            if takes_input:
                test_cases = _generate_test_cases(question, language)
                if not test_cases:
                    all_justifications.append(f"{justification_prefix}: Could not generate test cases.")
                    continue
                passed_cases = _run_code_in_docker(fixed_code, language, test_cases)
                score = passed_cases / len(test_cases) if test_cases else 0.0
                all_justifications.append(f"{justification_prefix}: Passed {passed_cases}/{len(test_cases)} tests.")
            else:
                conceptual_result = _analyze_code_conceptually(question, fixed_code, language)
                score = conceptual_result.get('score', 0.0)
                justification = conceptual_result.get('justification', 'AI analysis failed.')
                all_justifications.append(f"{justification_prefix}: {justification}")

            total_score += score
        except Exception as e:
            print(f"A critical error occurred during analysis of program {i+1}: {e}")
            all_justifications.append(f"{justification_prefix}: Analysis failed with a critical error.")
            continue
    
    average_score = total_score / len(programs) if programs else 0.0
    final_justification = " | ".join(all_justifications)
    return {'score': average_score, 'justification': final_justification}
