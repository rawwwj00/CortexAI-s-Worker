# theory_analyzer.py
import os
import json

try:
    import google.generativeai as genai
    genai.configure(api_key=os.environ.get("GEMINI_API_KEY"))
    grading_model = genai.GenerativeModel('gemini-1.5-pro-latest')
except Exception as e:
    grading_model = None
    print(f"[theory_analyzer] Gemini not configured: {e}")

def analyze_theory_submission(question: str, student_answer_ocr: str) -> dict:
    """
    Returns: {'score': float(0..1), 'justification': str}
    Uses AI when available. Falls back to a simple heuristic if not.
    """
    if not student_answer_ocr or not student_answer_ocr.strip():
        return {'score': 0.0, 'justification': 'No answer submitted.'}

    if grading_model:
        prompt = f"""
You are a strict university instructor. Grade the student's short-answer against the question.
Return a single JSON object with "score" (0.0-1.0) and "justification" (one short sentence).
Question: "{question}"
Student Answer: "{student_answer_ocr}"
"""
        try:
            resp = grading_model.generate_content(prompt)
            text = getattr(resp, "text", "") or str(resp)
            cleaned = text.strip().replace("```json", "").replace("```", "").strip()
            parsed = json.loads(cleaned)
            # normalize parsed content
            s = float(parsed.get('score', 0.0))
            j = parsed.get('justification', '') or ''
            return {'score': max(0.0, min(1.0, s)), 'justification': j}
        except Exception:
            # fall through to heuristic below
            pass

    # Simple heuristic fallback: check overlap of keywords from question and answer
    q_words = set(w.lower().strip(".,;()[]") for w in (question or "").split() if len(w) > 2)
    a_words = set(w.lower().strip(".,;()[]") for w in (student_answer_ocr or "").split() if len(w) > 2)
    if not q_words:
        return {'score': 0.0, 'justification': 'Question not provided.'}
    overlap = len(q_words & a_words)
    score = min(1.0, overlap / max(1, len(q_words)))  # proportion of question keywords present
    justification = f"Keyword overlap: {overlap}/{len(q_words)}"
    return {'score': float(score), 'justification': justification}
