# theory_analyzer.py
import os
import json
import google.generativeai as genai

try:
    genai.configure(api_key=os.environ.get("GEMINI_API_KEY"))
    grading_model = genai.GenerativeModel('gemini-1.5-pro-latest')
except Exception as e:
    grading_model = None

def analyze_theory_submission(question: str, student_answer_ocr: str) -> dict:
    if not grading_model or not student_answer_ocr:
        return {'score': 0.0, 'justification': 'Missing model or student answer.'}

    prompt = f"""
    As an expert AI university professor, evaluate the student's answer strictly.
    Provide your response as a single, valid JSON object with two keys: "score" and "justification".
    - "score": A float between 0.0 (wrong) and 1.0 (perfect).
    - "justification": A brief, one-sentence explanation for the score.
    ---
    Question: "{question}"
    ---
    Student's Answer: "{student_answer_ocr}"
    ---
    """
    try:
        response = grading_model.generate_content(prompt)
        json_text = response.text.replace("```json", "").replace("```", "").strip()
        return json.loads(json_text)
    except Exception as e:
        return {'score': 0.0, 'justification': 'AI grading failed.'}
