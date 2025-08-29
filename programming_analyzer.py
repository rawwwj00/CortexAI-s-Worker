import os
import json
import re
import docker
import tempfile
import google.generativeai as genai
from collections import defaultdict
import string

# Configure the Gemini API client at the module level
try:
    genai.configure(api_key=os.environ.get("GEMINI_API_KEY"))
    programming_model = genai.GenerativeModel('gemini-1.5-pro-latest')
except Exception as e:
    print(f"CRITICAL: Failed to configure Gemini API. AI features will be disabled. Error: {e}")
    programming_model = None

_STOPWORDS = {
    'the','a','an','and','or','to','of','in','on','for','with','by','from',
    'that','this','these','those','it','is','are','as','be','your','student',
    'write','implement','print'
}
# --- HELPER FUNCTIONS ---
def _token_set(text: str):
    text = text.lower()
    text = text.translate(str.maketrans(string.punctuation, ' '*len(string.punctuation)))
    toks = [t for t in text.split() if t and t not in _STOPWORDS]
    return set(toks)

def split_question_into_parts(question: str) -> list:
    """
    Attempts to split an assignment question into subparts (P1, (a), 1., A), etc).
    If no clear splits found, returns a single-element list [question].
    """
    if not question or not question.strip():
        return [question]

    # Common part markers e.g. "P1:", "1.", "(a)", "a)", "Part 1:", "Q1:", "P.1", "P1 -"
    pattern = re.compile(r'(^|\n)\s*(?:P?\s?\d+[:.\)]|Part\s*\d+[:.\)]|\([a-zA-Z0-9]\)|[a-zA-Z]\)|Q\d+[:.\)])', re.IGNORECASE)
    matches = list(pattern.finditer(question))
    if not matches:
        # Also try splitting by lines that look like separate tasks (lines starting with '-') or '---'
        lines = [l.strip() for l in question.splitlines() if l.strip()]
        if len(lines) > 1:
            return [" ".join(lines)]
        return [question]

    parts = []
    spans = [m.start() for m in matches] + [len(question)]
    for i in range(len(matches)):
        start = matches[i].start()
        end = spans[i+1]
        part_text = question[start:end].strip()
        # cleanup leading numbering
        part_text = re.sub(r'^\s*(?:P?\s?\d+[:.\)]|Part\s*\d+[:.\)]|\([a-zA-Z0-9]\)|[a-zA-Z]\)|Q\d+[:.\)])\s*', '', part_text, flags=re.IGNORECASE)
        parts.append(part_text.strip())
    # filter very short blanks
    parts = [p for p in parts if p]
    return parts if parts else [question]


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
    """
    Improved analysis: parse question into parts, split student's submission into program parts,
    map programs to question parts, score each part separately, count missing parts as zero.
    Returns {'score': average_score (0..1), 'justification': '...'}
    """
    if not programming_model or not ocr_code:
        return {'score': 0.0, 'justification': 'Missing Gemini model or student code.'}

    # 1) identify question parts
    question_parts = split_question_into_parts(question)
    if not question_parts:
        question_parts = [question]

    # 2) split student's submission into candidate programs (AI-based fallback)
    program_parts = _split_submission_into_parts(question, ocr_code)
    if not program_parts:
        return {'score': 0.0, 'justification': 'No valid programs were found in the submission.'}

    # 3) map each program to the best matching question part using token overlap
    q_tokens = [ _token_set(qp) for qp in question_parts ]
    program_tokens = [ _token_set(p) for p in program_parts ]

    # similarity scores matrix
    sim_matrix = [[len(program_tokens[i] & q_tokens[j]) / max(1, len(q_tokens[j])) for j in range(len(question_parts))] for i in range(len(program_parts))]

    # Greedy assignment: prefer best program->question matches, but allow at most one program per question part.
    assigned = {}   # question_index -> program_index
    program_assigned = {}  # program_index -> question_index
    # Flatten all pairs and sort by score desc
    pairs = []
    for i in range(len(program_parts)):
        for j in range(len(question_parts)):
            pairs.append((sim_matrix[i][j], i, j))
    pairs.sort(reverse=True, key=lambda x: x[0])

    for score, pi, qi in pairs:
        if pi in program_assigned or qi in assigned:
            continue
        # threshold: only accept if there is at least some token overlap, otherwise leave unmapped
        if score >= 0.05:
            assigned[qi] = pi
            program_assigned[pi] = qi

    # remaining unmapped programs (low-similarity) will be placed into unmatched bucket
    unmatched_program_indices = [i for i in range(len(program_parts)) if i not in program_assigned]

    # If there are still unassigned question parts and we have unmatched programs, assign them (best-effort)
    qi_list_unassigned = [qi for qi in range(len(question_parts)) if qi not in assigned]
    for qi, pi in zip(qi_list_unassigned, unmatched_program_indices):
        assigned[qi] = pi
        program_assigned[pi] = qi

    # 4) grade per question part
    part_scores = [0.0] * len(question_parts)
    part_justifications = ['No submission for this part.'] * len(question_parts)
    debug_notes = []

    # To handle duplicates mapping to same qi (shouldn't happen after assignment), we will handle only one per qi.
    for qi in range(len(question_parts)):
        if qi not in assigned:
            # explicit 0 for missing part
            part_scores[qi] = 0.0
            part_justifications[qi] = f"Part {qi+1}: No submission found for this required part."
            continue

        pi = assigned[qi]
        program_code = program_parts[pi]
        justification_prefix = f"Part {qi+1}"

        try:
            language = _detect_language(program_code)
            fixed_code = _fix_code(program_code, language)
            takes_input = _check_for_input_statically(fixed_code, language)

            if takes_input:
                test_cases = _generate_test_cases(question_parts[qi], fixed_code, language)
                if not test_cases:
                    part_scores[qi] = 0.0
                    part_justifications[qi] = f"{justification_prefix}: Could not generate test cases for this specific part."
                    continue
                passed_cases = _run_code_in_docker(fixed_code, language, test_cases)
                score = passed_cases / len(test_cases) if test_cases else 0.0
                part_scores[qi] = score
                part_justifications[qi] = f"{justification_prefix}: Passed {passed_cases}/{len(test_cases)} tests."
            else:
                conceptual_result = _analyze_code_conceptually(question_parts[qi], fixed_code, language)
                score = float(conceptual_result.get('score', 0.0))
                justification = conceptual_result.get('justification', 'AI conceptual analysis failed.')
                part_scores[qi] = score
                part_justifications[qi] = f"{justification_prefix}: {justification}"

        except Exception as e:
            part_scores[qi] = 0.0
            part_justifications[qi] = f"{justification_prefix}: Analysis failed with error: {e}"
            debug_notes.append(str(e))
            continue

    average_score = sum(part_scores) / len(part_scores) if part_scores else 0.0
    final_justification = " | ".join(part_justifications)
    debug_info = " | ".join(debug_notes) if debug_notes else ""

    return {'score': average_score, 'justification': final_justification, 'debug_info': debug_info}

