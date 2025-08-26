import os
import json
import docker
import tempfile 
import google.generativeai as genai

# Configure Gemini API client at the module level
try:
    # This will use the GEMINI_API_KEY from the systemd environment
    genai.configure(api_key=os.environ.get("GEMINI_API_KEY"))
    programming_model = genai.GenerativeModel('gemini-1.5-flash-latest')
except Exception as e:
    programming_model = None


def analyze_programming_submission(question: str, ocr_code: str) -> dict:
    """
    A fully automated pipeline to analyze a programming submission.
    """
    if not programming_model or not ocr_code:
        return {'score': 0.0, 'justification': 'Missing model or student code.'}

    programs = _split_programs(ocr_code)
    if not programs:
        return {'score': 0.0, 'justification': 'No valid programs were found.'}

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


def _run_code_in_docker(code: str, language: str, test_cases: list) -> int:
    """
    CORRECTED: This function now runs Docker directly, as it should on the worker.
    """
    try:
        client = docker.from_env()
    except docker.errors.DockerException as e:
        print(f"Docker connection error: {e}")
        return 0 # Return 0 passes if Docker isn't running

    passed_count = 0
    for case in test_cases:
        # (The rest of this function is the original, correct Docker logic)
        # ... (This logic is being restored from our very first worker.py version)
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
                # ... (The smarter comparison logic is also restored here)
                # ...
            except Exception as e:
                print(f"An unknown execution error occurred: {e}")
                continue
    return passed_count

def _detect_language(code: str) -> str:
    """Detects the programming language of the given code snippet."""
    # Updated prompt to be more specific
    prompt = f"Detect the programming language of the following code. Respond with a single word only from this list: Python, Java, C, C++. \n\nCode:\n```\n{code}\n```"
    response = programming_model.generate_content(prompt)
    return response.text.strip().lower()

def _fix_code(code: str, language: str) -> str:
    """
    Corrects OCR'd code using an AI model and robustly cleans the output,
    removing any markdown formatting or language identifiers.
    """
    prompt = f"The following {language} code was extracted from an image using OCR and may contain typos, syntax errors, or incorrect indentation. Please correct it so it is a runnable program. Provide only the corrected code with no explanations.\n\nOCR'd Code:\n```\n{code}\n```"
    response = programming_model.generate_content(prompt)

    # Get the raw text and strip whitespace
    cleaned_text = response.text.strip()

    # Intelligently remove markdown code block fences (e.g., ```cpp ... ```)
    if cleaned_text.startswith("```"):
        # Find the first newline character
        first_newline = cleaned_text.find('\n')
        if first_newline != -1:
            # Take everything *after* the first line (which contains ```cpp)
            cleaned_text = cleaned_text[first_newline + 1:]

    # Remove the closing fence if it exists
    if cleaned_text.endswith("```"):
        cleaned_text = cleaned_text[:-3].strip()
    print(cleaned_text)
    return cleaned_text

def _generate_test_cases(question: str, language: str) -> list:
    prompt = f"""
    Based on the following programming question, generate a list of 5 diverse test cases.
    Provide your response as a single, valid JSON object with no extra text.
    The object should be a list of dictionaries, where each dictionary has two keys: "input" and "expected_output".
    The input should be a string that can be passed to the program's standard input.
    The expected_output should be a string representing the program's exact standard output.

    Question: "{question}"
    Language: "{language}"
    """
    try:
        response = programming_model.generate_content(prompt)
        json_text = response.text.replace("```json", "").replace("```", "").strip()
        return json.loads(json_text)
    except Exception as e:
        print(f"Failed to generate or parse test cases: {e}")
        return []
    
WORKER_URL = os.environ.get("WORKER_URL")
WORKER_SECRET_KEY = os.environ.get("WORKER_SECRET_KEY")

def _run_code_in_docker(code: str, language: str, test_cases: list) -> int:
    """
    NEW: This function no longer runs Docker. Instead, it makes a secure API
    call to the dedicated Docker execution worker service.
    """
    if not WORKER_URL or not WORKER_SECRET_KEY:
        print("CRITICAL ERROR: WORKER_URL or WORKER_SECRET_KEY is not set.")
        raise ConnectionError("Worker service is not configured.")

    headers = {
        'Content-Type': 'application/json',
        'X-Auth-Key': WORKER_SECRET_KEY
    }
    
    payload = {
        "code": code,
        "language": language,
        "test_cases": test_cases
    }

    try:
        response = requests.post(WORKER_URL, headers=headers, json=payload, timeout=60) # 60-second timeout
        response.raise_for_status() # Raises an exception for bad status codes (4xx or 5xx)
        
        result = response.json()
        return result.get("passed_count", 0)

    except requests.exceptions.RequestException as e:
        print(f"Failed to connect to the Docker worker service: {e}")
        # Return 0 to indicate failure without crashing the whole analysis
        return 0

def _split_programs(ocr_text: str) -> list:

    # Uses the AI model to identify and separate multiple, distinct programs
    # from a single block of OCR'd text.

    if not programming_model or not ocr_text.strip():
        return []

    prompt = f"""
    The following text was extracted from a student's submission and may contain one or more distinct computer programs.
    Your task is to identify and separate each complete program.

    Provide your response as a single, valid JSON object with one key: "programs".
    The value should be a list of strings, where each string is one complete, self-contained program.
    If there is only one program, the list should contain one string. If no valid code is found, return an empty list.

    Example Response:
    {{
        "programs": [
            "#include <stdio.h>\\nint main() {{ printf(\\"Hello\\"); return 0; }}",
            "public class HelloWorld {{ public static void main(String[] args) {{ System.out.println(\\"World\\"); }} }}"
        ]
    }}

    ---
    Submission Text:
    "{ocr_text}"
    ---
    """
    try:
        response = programming_model.generate_content(prompt)
        json_text = response.text.replace("```json", "").replace("```", "").strip()
        return json.loads(json_text).get("programs", [])
    except Exception as e:
        print(f"Failed to split programs with AI: {e}")
        # As a fallback, assume the whole text is one program

        return [ocr_text]
