import os
import json
import google.generativeai as genai

# Configure the Gemini API client
try:
    genai.configure(api_key=os.environ.get("GEMINI_API_KEY"))
    grading_model = genai.GenerativeModel('gemini-1.5-flash-latest')
except Exception as e:
    grading_model = None

def _fix_ocr_text_with_gemini(ocr_text: str) -> str:
    """
    Uses Gemini to correct spelling and grammar mistakes from OCR text.
    It is designed to not change the core meaning of the original text.

    Args:
        ocr_text (str): The raw text extracted from a document.

    Returns:
        str: The corrected version of the text.
    """
    if not grading_model or not ocr_text: return ocr_text
    
    prompt = f"""
    The following text was extracted from a handwritten document using OCR and contains spelling mistakes and typos. 
    Please correct only the spelling and grammar to make it clean and readable. 
    IMPORTANT: Do NOT change the factual meaning of the sentences.
    Provide only the corrected text.

    Original Text:
    "{ocr_text}"
    """
    try:
        response = grading_model.generate_content(prompt)
        return response.text.strip()
    except Exception as e:
        return ocr_text

def analyze_theory_submission(question: str, student_answer_ocr: str) -> dict:
    """
    Grades a student's theoretical answer against a question using the Gemini model.

    Args:
        question (str): The assignment question.
        student_answer_ocr (str): The student's answer, extracted via OCR.

    Returns:
        dict: A dictionary containing the 'score' (0.0-1.0) and a 'justification'.
    """
    if not grading_model or not student_answer_ocr:
        return {'score': 0.0, 'justification': 'Missing model or student answer.'}

    corrected_answer = _fix_ocr_text_with_gemini(student_answer_ocr)

    prompt = f"""
    As an expert AI university professor, evaluate the student's answer strictly.
    Provide your response as a single, valid JSON object with two keys: "score" and "justification".
    - "score": A float between 0.0 (wrong) and 1.0 (perfect).
    - "justification": A brief, one-sentence explanation for the score.

    ---
    Question: "{question}"
    ---
    Student's Answer: "{corrected_answer}"
    ---
    """
    try:
        response = grading_model.generate_content(prompt)
        json_text = response.text.replace("```json", "").replace("```", "").strip()
        return json.loads(json_text)
    except Exception as e:
        return {'score': 0.0, 'justification': 'AI grading failed.'}