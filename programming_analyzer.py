import os
import json
import requests
import google.generativeai as genai

# --- AI Model and API Configuration ---
print("Configuring Gemini API client for Programming Analysis...")
try:
    genai.configure(api_key=os.environ.get("GEMINI_API_KEY"))
    programming_model = genai.GenerativeModel('gemini-1.5-flash-latest')
    print("Gemini model for programming analysis configured successfully.")
except Exception as e:
    print(f"CRITICAL ERROR: Failed to configure Gemini model: {e}")
    programming_model = None


def analyze_programming_submission(question: str, ocr_code: str) -> dict:
    """
    A fully automated pipeline to analyze a programming submission that may
    contain one or more separate programs. The final score is an average.
    """
    if not programming_model or not ocr_code:
        return {'score': 0.0, 'justification': 'Missing model or student code.'}

    # Step 1: Split the OCR'd text into individual programs
    programs = _split_programs(ocr_code)
    if not programs:
        return {'score': 0.0, 'justification': 'No valid programs were found in the submission.'}

    total_score = 0.0
    all_justifications = []
    program_count = len(programs)

    # Step 2: Analyze each program individually
    for i, program_code in enumerate(programs):
        print(f"--- Analyzing Program {i+1}/{program_count} ---")
        try:
            language = _detect_language(program_code)
            print(f"Detected language: {language}")

            fixed_code = _fix_code(program_code, language)
            print("Corrected Code...")

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
    
    # Step 3: Calculate the average score and combine justifications
    average_score = total_score / program_count if program_count > 0 else 0.0
    final_justification = " | ".join(all_justifications)

    return {'score': average_score, 'justification': final_justification}

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