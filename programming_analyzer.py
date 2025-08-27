import os
import json
import google.generativeai as genai

# Configure the Gemini API client at the module level
try:
    genai.configure(api_key=os.environ.get("GEMINI_API_KEY"))
    # Using the most powerful model for the best analysis
    programming_model = genai.GenerativeModel('gemini-1.5-pro-latest')
except Exception as e:
    programming_model = None


# --- HELPER FUNCTIONS ---

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

def _split_programs(ocr_text: str) -> list:
    """Uses the AI model to identify and separate multiple programs from a single block of text."""
    if not programming_model or not ocr_text.strip(): return [ocr_text]
    prompt = f'The following text may contain one or more distinct computer programs. Separate each complete program into a JSON list of strings under the key "programs".\n\nSubmission Text:\n"{ocr_text}"'
    try:
        response = programming_model.generate_content(prompt)
        json_text = response.text.replace("```json", "").replace("```", "").strip()
        return json.loads(json_text).get("programs", [])
    except Exception:
        return [ocr_text]


# --- FINAL, HOLISTIC ANALYSIS FUNCTION ---

def analyze_programming_submission(question: str, ocr_code: str) -> dict:
    """
    Analyzes a programming submission holistically, evaluating all provided code
    against the full set of requirements in the assignment question.
    """
    debug_log = []
    if not programming_model or not ocr_code:
        return {'score': 0.0, 'justification': 'Missing model or student code.', 'debug_info': 'Model or code was empty.'}

    # Step 1: Split the submission into individual programs
    programs = _split_programs(ocr_code)
    debug_log.append(f"Found {len(programs)} program(s) in submission.")
    if not programs:
        return {'score': 0.0, 'justification': 'No valid programs were found.', 'debug_info': '\n'.join(debug_log)}

    # Step 2: Correct each program and build a combined code block for analysis
    corrected_programs = []
    for i, program_code in enumerate(programs):
        try:
            # For this type of question, we can assume a consistent language (e.g., C++)
            fixed_code = _fix_code(program_code, "c++")
            corrected_programs.append(f"--- Program {i+1} ---\n{fixed_code}\n")
        except Exception as e:
            debug_log.append(f"Could not correct Program {i+1}: {e}")
            continue
    
    combined_code = "\n".join(corrected_programs)
    debug_log.append(f"Combined and Corrected Code for Analysis:\n{combined_code}")

    # Step 3: Perform a single, holistic analysis of all the code
    prompt = f"""
    As an expert Computer Science professor, your task is to grade a student's submission.
    The student was asked to provide multiple C++ programs to fulfill several requirements.
    You must evaluate their entire submission holistically.

    Provide your response as a single, valid JSON object with "score" and "justification".
    - "score": A float from 0.0 to 1.0. The score should be 1.0 only if all requirements from the question are met correctly across all the provided programs. Give partial credit if some, but not all, requirements are met.
    - "justification": A brief, overall summary of the student's submission, explaining the reason for the score.

    ---
    Assignment Question:
    "{question}"
    ---
    Student's Entire Submission (all programs combined):
    ```cpp
    {combined_code}
    ```
    ---
    """
    try:
        response = programming_model.generate_content(prompt)
        json_text = response.text.replace("```json", "").replace("```", "").strip()
        result = json.loads(json_text)
        # Pass the debug log through to the final result
        result['debug_info'] = '\n'.join(debug_log)
        return result

    except Exception as e:
        debug_log.append(f"Holistic analysis failed: {e}")
        return {
            'score': 0.0,
            'justification': 'The AI failed to perform a holistic analysis of the submission.',
            'debug_info': '\n'.join(debug_log)
        }
