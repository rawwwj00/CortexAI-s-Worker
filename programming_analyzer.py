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
    print(f"CRITICAL: Failed to configure Gemini API. AI features will be disabled. Error: {e}")
    programming_model = None


# --- HELPER FUNCTIONS ---

def compare_outputs(actual_output: str, expected_output: str) -> bool:
    """
    Compares two program outputs using a multi-stage, robust strategy.
    Returns True if the outputs are considered equivalent, False otherwise.
    """
    # 1. Normalize both strings: remove leading/trailing whitespace and make lowercase.
    norm_actual = actual_output.strip().lower()
    norm_expected = expected_output.strip().lower()

    # Strategy 1: Exact Match after Normalization
    if norm_actual == norm_expected:
        return True

    # Strategy 2: Compare Extracted Numbers
    actual_nums = re.findall(r'-?\d+\.?\d*', norm_actual)
    expected_nums = re.findall(r'-?\d+\.?\d*', norm_expected)
    if expected_nums and actual_nums == expected_nums:
        return True
        
    # Strategy 3: Lenient Substring Check (Fallback)
    if norm_expected in norm_actual:
        return True

    return False

def _check_for_input_statically(code: str, language: str) -> bool:
    """
    Statically and reliably checks for standard input keywords.
    This is much faster and more accurate than using an AI call.
    """
    language = language.lower()
    input_keywords = {
        "python": ["input("],
        "java": ["new Scanner(System.in)", "System.in.read"],
        "c++": ["std::cin", "cin >>", "scanf"],
        "c": ["scanf", "getchar"]
    }
    
    if language in input_keywords:
        for keyword in input_keywords[language]:
            if keyword in code:
                return True
    return False

def _split_submission_into_parts(question: str, code: str) -> list:
    """
    Uses AI to intelligently split a single code submission into a list of separate
    implementations based on the requirements of the assignment question.
    """
    prompt = f"""
    Based on the assignment question, the student was required to provide multiple function implementations.
    Analyze the student's code and split it into a list of strings, where each string is a complete, runnable program
    representing one of the required implementations.

    Provide your response as a single, valid JSON object with one key: "programs". The value should be a list of code strings.

    ---
    Assignment Question: "{question}"
    ---
    Student's Code:
    ```
    {code}
    ```
    ---
    """
    try:
        response = programming_model.generate_content(prompt)
        json_text = response.text.replace("```json", "").replace("```", "").strip()
        return json.loads(json_text).get("programs", [code])
    except Exception as e:
        print(f"AI could not split submission into parts, analyzing as a whole. Error: {e}")
        return [code]

def _run_code_in_docker(code: str, language: str, test_cases: list) -> int:
    """Runs student code in a sandboxed Docker container and checks outputs."""
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
            image, file_name = "python:3.9-slim", "script.py"
            run_command = ["sh", "-c", f"echo '{sanitized_input}' | python -u /app/{file_name}"]
        elif language in ["c++", "c"]:
            image, file_name = "gcc:latest", f"program.{'cpp' if language == 'c++' else 'c'}"
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
                    image, 
                    command=run_command, 
                    volumes={temp_dir: {'bind': '/app', 'mode': 'ro'}}, # SECURITY: Mount as read-only
                    working_dir="/app", 
                    remove=True, 
                    network_disabled=True, 
                    mem_limit='256m'
                ).decode('utf-8')
                
                # Use the robust comparison function
                if compare_outputs(container_output, str(case.get('expected_output', ''))):
                    passed_count += 1
            
            except docker.errors.ContainerError as e:
                print(f"Container runtime error: {e.stderr.decode('utf-8')}")
                continue
            except Exception as e:
                print(f"An unknown Docker execution error occurred: {e}")
                continue
    
    return passed_count

# --- (Other AI helper functions: _detect_language, _fix_code, etc. remain the same) ---
def _detect_language(code: str) -> str:
    prompt = f"Detect the programming language of the following code. Respond with a single word only from this list: Python, Java, C, C++. \n\nCode:\n```\n{code}\n```"
    response = programming_model.generate_content(prompt)
    return response.text.strip().lower()

def _fix_code(code: str, language: str) -> str:
    prompt = f"The following {language} code was extracted from an image using OCR and may contain errors. Please correct it so it is a runnable program. Provide only the corrected code with no explanations.\n\nOCR'd Code:\n```\n{code}\n```"
    response = programming_model.generate_content(prompt)
    cleaned_text = response.text.strip()
    if cleaned_text.startswith("```"):
        cleaned_text = cleaned_text[cleaned_text.find('\n') + 1:]
    if cleaned_text.endswith("```"):
        cleaned_text = cleaned_text[:-3].strip()
    return cleaned_text
    
def _generate_test_cases(question: str, code_snippet: str, language: str) -> list:
    """
    Generates test cases for a specific code snippet, using the overall question for context.
    """
    prompt = f'''
    You are a test case generator for a single function. Based on the provided {language} code snippet and the original assignment question, generate 5 diverse test cases. The code reads from standard input. 
    
    Provide your response as a single, valid JSON object. The object should be a list of dictionaries, where each dictionary has an "input" key and an "expected_output" key.

    ---
    Original Assignment Question: "{question}"
    ---
    Code Snippet to Test:
    ```
    {code_snippet}
    ```
    ---
    '''
    try:
        response = programming_model.generate_content(prompt)
        json_text = response.text.replace("```json", "").replace("```", "").strip()
        return json.loads(json_text)
    except Exception as e:
        print(f"Failed to generate or parse test cases: {e}")
        return []
        
def _analyze_code_conceptually(question: str, code: str, language: str) -> dict:
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

# --- MAIN ANALYSIS FUNCTION ---

def analyze_programming_submission(question: str, ocr_code: str) -> dict:
    if not programming_model or not ocr_code:
        return {'score': 0.0, 'justification': 'Missing Gemini model or student code.'}

    # 1. Intelligently split the submission into parts based on the question
    program_parts = _split_submission_into_parts(question, ocr_code)
    if not program_parts:
        return {'score': 0.0, 'justification': 'No valid programs were found in the submission.'}

    total_score, all_justifications = 0.0, []

    # 2. Loop through each part and analyze it down the correct path
    for i, program_code in enumerate(program_parts):
        justification_prefix = f"Part {i+1}"
        try:
            language = _detect_language(program_code)
            fixed_code = _fix_code(program_code, language)
            
            # 3. Use the fast, reliable static check for input
            takes_input = _check_for_input_statically(fixed_code, language)
            
            if takes_input:
                # DYNAMIC PATH: Generate tests and run in Docker
                test_cases = _generate_test_cases(question, language)
                if not test_cases:
                    all_justifications.append(f"{justification_prefix}: Could not generate test cases.")
                    continue
                passed_cases = _run_code_in_docker(fixed_code, language, test_cases)
                score = passed_cases / len(test_cases) if test_cases else 0.0
                all_justifications.append(f"{justification_prefix}: Passed {passed_cases}/{len(test_cases)} tests.")
            else:
                # CONCEPTUAL PATH: AI review for non-input code
                conceptual_result = _analyze_code_conceptually(question, fixed_code, language)
                score = conceptual_result.get('score', 0.0)
                justification = conceptual_result.get('justification', 'AI analysis failed.')
                all_justifications.append(f"{justification_prefix}: {justification}")

            total_score += score
        except Exception as e:
            print(f"A critical error occurred during analysis of program part {i+1}: {e}")
            all_justifications.append(f"{justification_prefix}: Analysis failed with a critical error.")
            continue
    
    # 4. Calculate the final average score
    average_score = total_score / len(program_parts) if program_parts else 0.0
    final_justification = " | ".join(all_justifications)
    
    return {'score': average_score, 'justification': final_justification}

