# programming_analyzer.py
"""
Deterministic programming analyzer for a single question-part + OCR'd code blob.

Public function:
  analyze_programming_submission(question: str, ocr_code: str) -> dict
Returns dict with keys:
  - score: float between 0.0 and 1.0
  - justification: short human-readable string
  - details: optional dict with debug/test info
"""

import os
import re
import json
import tempfile
import shutil
import string
from typing import List, Dict, Tuple, Optional

# docker is optional; if not installed the analyzer will fall back to conceptual scoring for stdin programs
try:
    import docker
    _DOCKER_AVAILABLE = True
except Exception:
    docker = None
    _DOCKER_AVAILABLE = False

# ---------------------------
# Small utilities
# ---------------------------
_STOPWORDS = set([
    'the','a','an','and','or','to','of','in','on','for','with','by','from',
    'that','this','it','is','are','as','be','your','student','write','implement','print'
])

def _token_set(text: str) -> set:
    if not text:
        return set()
    t = text.lower()
    # replace punctuation with spaces
    t = t.translate(str.maketrans(string.punctuation, ' ' * len(string.punctuation)))
    toks = [w for w in t.split() if w and w not in _STOPWORDS]
    return set(toks)

# ---------------------------
# Heuristic splitting of OCR blob into candidate programs
# ---------------------------
def _split_submission_into_parts(question: str, code_blob: str) -> List[str]:
    """
    Heuristic splitting:
      - split by obvious separators (---, ###, ====, '--- Page Break ---')
      - split by multiple blank lines
      - as a last resort return the whole blob as single candidate
    """
    if not code_blob or not code_blob.strip():
        return []

    separators = [r'\n-{3,}\n', r'\n#{3,}\n', r'\n={3,}\n', r'--- Page Break ---', r'FILE_BREAK', r'--- FILE BREAK ---']
    for sep in separators:
        parts = re.split(sep, code_blob, flags=re.IGNORECASE)
        parts = [p.strip() for p in parts if p and p.strip()]
        if len(parts) > 1:
            return parts

    # split by two or more newlines (paragraph separation)
    parts = [p.strip() for p in re.split(r'\n\s*\n', code_blob) if p.strip()]
    if len(parts) > 1:
        return parts

    # try to split when multiple 'def ' (python) or 'int main' (c/c++) occurrences exist
    if code_blob.count('\ndef ') >= 2 or code_blob.count('\nclass ') >= 2:
        blocks = [b.strip() for b in re.split(r'\n\s*\n', code_blob) if b.strip()]
        if len(blocks) > 1:
            return blocks

    if code_blob.lower().count('int main') + code_blob.lower().count('main(') >= 2:
        blocks = [b.strip() for b in re.split(r'\n\s*\n', code_blob) if b.strip()]
        if len(blocks) > 1:
            return blocks

    # fallback: single program
    return [code_blob.strip()]

# ---------------------------
# Language detection heuristics
# ---------------------------
def _detect_language(code: str) -> str:
    """
    Return one of: 'python', 'java', 'c++', 'c'
    Defaults to 'python' when ambiguous.
    """
    if not code:
        return 'python'
    lower = code.lower()
    # common tokens
    if re.search(r'\b(def|print\(|import\b|:\s*$)', lower):
        return 'python'
    if 'public static void main' in lower or 'system.out.println' in lower:
        return 'java'
    if '#include' in lower and ('std::' in lower or 'iostream' in lower or 'cin >>' in lower):
        return 'c++'
    if '#include' in lower and ('scanf(' in lower or 'printf(' in lower) and 'std::' not in lower:
        return 'c'
    # fallback default
    return 'python'

# ---------------------------
# Simple OCR cleanup (fix common OCR mistakes)
# ---------------------------
def _fix_code_simple(code: str) -> str:
    if not code:
        return code
    # common OCR substitutions
    fixes = [
        (r'ﬁ', 'fi'),
        (r'ﬂ', 'fl'),
        (r'“|”', '"'),
        (r'‘|’', "'"),
        (r'—', '-'),
        (r'–', '-'),
        (r'‚', ','),
    ]
    cleaned = code
    for pat, rep in fixes:
        cleaned = re.sub(pat, rep, cleaned)
    # remove stray non-ascii (but keep common whitespace and punctuation)
    cleaned = ''.join(ch for ch in cleaned if (31 < ord(ch) < 127) or ch in '\n\t\r')
    return cleaned

# ---------------------------
# Detect whether program expects stdin
# ---------------------------
def _has_stdin(code: str, language: str) -> bool:
    language = (language or '').lower()
    if language == 'python':
        return 'input(' in code or 'sys.stdin' in code
    if language == 'java':
        return 'scanner(' in code.lower() or 'system.in' in code.lower()
    if language in ('c++', 'c'):
        return any(tok in code for tok in ['scanf(', 'cin >>', 'gets(', 'fgets(', 'read('])
    # default heuristic
    return bool(re.search(r'\b(input|scanf|cin|readline|gets|Scanner)\b', code))

# ---------------------------
# Generate simple deterministic test cases for a single-part question
# ---------------------------
def _generate_test_cases_simple(question: str, language: str) -> List[Dict[str,str]]:
    """
    A tiny deterministic test-case generator:
      - If question mentions sum/add/multiply/average -> create small integer cases
      - If question mentions palindrome/string -> use string cases
      - If nothing detected -> return a couple generic cases
    Returns list of { "input": "...", "expected_output": "..." }.
    IMPORTANT: expected_output is a best-effort guess and may not match complex problems.
    """
    q = (question or "").lower()
    cases = []
    if any(k in q for k in ['sum', 'add', 'plus', 'addition', 'total']):
        cases = [
            {"input": "2 3", "expected_output": "5"},
            {"input": "0 5", "expected_output": "5"},
            {"input": "-1 5", "expected_output": "4"}
        ]
    elif any(k in q for k in ['multiply', 'product', 'times']):
        cases = [
            {"input": "2 3", "expected_output": "6"},
            {"input": "0 5", "expected_output": "0"},
            {"input": "-1 4", "expected_output": "-4"}
        ]
    elif any(k in q for k in ['palindrome', 'reverse', 'string']):
        cases = [
            {"input": "madam", "expected_output": "YES"},
            {"input": "hello", "expected_output": "NO"},
            {"input": "level", "expected_output": "YES"}
        ]
    else:
        # generic numeric cases and a simple single input case
        cases = [
            {"input": "2 3", "expected_output": ""},   # unknown expected output; running tests will still attempt
            {"input": "5", "expected_output": ""},
        ]
    # limit to 5
    return cases[:5]

# ---------------------------
# Helper to compare outputs loosely
# ---------------------------
def _compare_outputs(actual: str, expected: str) -> bool:
    if actual is None:
        actual = ''
    if expected is None:
        expected = ''
    a = actual.strip()
    e = expected.strip()
    if a == e:
        return True
    # compare numeric tokens
    a_nums = re.findall(r'-?\d+\.?\d*', a)
    e_nums = re.findall(r'-?\d+\.?\d*', e)
    if e_nums and a_nums == e_nums:
        return True
    # case-insensitive containment
    if e.lower() in a.lower() or a.lower() in e.lower():
        return True
    return False

# ---------------------------
# Run code in Docker (best-effort)
# ---------------------------
def _run_code_in_docker(code: str, language: str, test_cases: List[Dict[str,str]]) -> Tuple[int,int,List[Dict]]:
    """
    Attempts to run each test case in a secure docker container.
    Returns (passed_count, total_count, details_list).
    If docker not available, returns (0, total, []) and a note in details.
    """
    total = len(test_cases)
    passed = 0
    details = []

    if total == 0:
        return 0, 0, details

    if not _DOCKER_AVAILABLE:
        # can't run tests; return zeros and details explaining why
        for tc in test_cases:
            details.append({"input": tc.get("input"), "expected": tc.get("expected_output"), "actual": None, "status": "docker_unavailable"})
        return 0, total, details

    client = docker.from_env(timeout=60)

    for tc in test_cases:
        inp = str(tc.get('input', ''))
        expected = str(tc.get('expected_output', '')).strip()

        with tempfile.TemporaryDirectory() as td:
            # prepare file and image+commands
            if language == 'python':
                fname = "prog.py"
                with open(os.path.join(td, fname), "w", encoding="utf-8") as f:
                    f.write(code)
                image = "python:3.9-slim"
                # pipe input using printf; use sh -c to allow pipe
                cmd = ["sh", "-c", f"printf %s {json.dumps(inp)} | python /app/{fname}"]
            elif language in ('c++', 'c'):
                ext = "cpp" if language == 'c++' else 'c'
                fname = f"prog.{ext}"
                with open(os.path.join(td, fname), "w", encoding="utf-8") as f:
                    f.write(code)
                image = "gcc:12"  # should exist on Docker Hub
                # compile then run (ignore compile errors, attempt to run)
                cmd = ["sh", "-c", f"g++ /app/{fname} -o /app/program 2>/dev/null || true; printf %s {json.dumps(inp)} | /app/program"]
            elif language == 'java':
                fname = "Main.java"
                with open(os.path.join(td, fname), "w", encoding="utf-8") as f:
                    f.write(code)
                image = "openjdk:17-slim"
                cmd = ["sh", "-c", f"javac /app/{fname} 2>/dev/null || true; printf %s {json.dumps(inp)} | java -cp /app Main"]
            else:
                # unsupported language; mark as skipped
                details.append({"input": inp, "expected": expected, "actual": None, "status": "unsupported_language"})
                continue

            try:
                out = client.containers.run(
                    image=image,
                    command=cmd,
                    volumes={td: {'bind': '/app', 'mode': 'ro'}},
                    working_dir="/app",
                    remove=True,
                    network_disabled=True,
                    stdout=True,
                    stderr=True,
                    mem_limit='512m'
                )
                actual = out.decode('utf-8', errors='ignore') if isinstance(out, (bytes, bytearray)) else str(out)
                ok = _compare_outputs(actual, expected) if expected else True  # if expected blank, accept actual as pass (best-effort)
                details.append({"input": inp, "expected": expected, "actual": actual.strip(), "status": "pass" if ok else "fail"})
                if ok:
                    passed += 1
            except Exception as e:
                details.append({"input": inp, "expected": expected, "actual": str(e), "status": "error"})
                # continue with next test
                continue

    return passed, total, details

# ---------------------------
# Conceptual analysis fallback (no docker or non-stdin programs)
# ---------------------------
def _conceptual_score(question: str, code: str, language: str) -> Tuple[float, str]:
    """
    Simple heuristics:
      - If question mentions 'function' or expects multiple variants, check for function definitions and return/params
      - Look for keywords: 'def', 'return', 'void', parameter patterns like '(x)' or 'int x'
    Produces a score between 0 and 1 and a short justification.
    """
    q = (question or "").lower()
    c = (code or "").lower()

    # basic checks
    has_def = bool(re.search(r'\bdef\s+\w+\s*\(', c)) or bool(re.search(r'\bfunction\b', c))
    has_return = 'return ' in c or re.search(r'\breturn\b', c)
    has_params = bool(re.search(r'\(\s*[a-zA-Z0-9_]+\s*[,)]', c)) or bool(re.search(r'\(\s*\)', c)) == False and '(' in c

    # build score
    score = 0.0
    reasons = []
    if has_def or 'class ' in c or 'int main' in c or 'public static' in c:
        score += 0.4
        reasons.append('contains function/main')
    if has_return:
        score += 0.3
        reasons.append('uses return')
    if has_params:
        score += 0.2
        reasons.append('contains parameters')
    # small bonus for longer code (not too small)
    if len(c.splitlines()) > 3:
        score += 0.1

    # clamp
    score = max(0.0, min(1.0, score))
    justification = f"Conceptual check — {'; '.join(reasons) if reasons else 'no clear functions/returns/params found'}."
    return score, justification

# ---------------------------
# Public function
# ---------------------------
def analyze_programming_submission(question: str, ocr_code: str) -> Dict:
    """
    Analyze an OCR'd code blob for a single question part.
    Returns a dict: { 'score': float, 'justification': str, 'details': {...} }
    """
    details = {}
    try:
        if not ocr_code or not ocr_code.strip():
            return {'score': 0.0, 'justification': 'No code extracted from submission.', 'details': details}

        # 1) split into candidate programs
        candidates = _split_submission_into_parts(question, ocr_code)
        details['candidate_count'] = len(candidates)

        # 2) pick best candidate by token overlap with question
        q_tokens = _token_set(question)
        best_idx = 0
        best_score = -1.0
        for i, cand in enumerate(candidates):
            tok = _token_set(cand)
            if q_tokens:
                score = len(tok & q_tokens) / max(1, len(q_tokens))
            else:
                # if question tokens empty, prefer longer candidate
                score = min(1.0, len(cand.split()) / 200.0)
            # slight preference for longer candidates to avoid tiny fragments
            score += min(0.001 * len(cand.split()), 0.02)
            if score > best_score:
                best_score = score
                best_idx = i
        selected = candidates[best_idx]
        details['selected_candidate_index'] = best_idx
        details['token_overlap_score'] = float(best_score)

        # 3) clean code
        fixed = _fix_code_simple(selected)
        details['selected_length_chars'] = len(fixed)

        # 4) detect language
        lang = _detect_language(fixed)
        details['language'] = lang

        # 5) detect if program expects input
        expects_input = _has_stdin(fixed, lang)
        details['expects_input'] = expects_input

        # 6) If expects input -> generate test cases and run them in docker (if available)
        if expects_input:
            test_cases = _generate_test_cases_simple(question, lang)
            details['generated_testcases'] = test_cases
            if not test_cases:
                # cannot generate testcases -> fallback to conceptual scoring with penalty
                score, just = _conceptual_score(question, fixed, lang)
                score = score * 0.5  # penalty for missing test cases
                return {'score': float(score), 'justification': f'No test cases could be generated. {just}', 'details': details}
            passed, total, run_details = _run_code_in_docker(fixed, lang, test_cases)
            details['run_details'] = run_details
            if total == 0:
                return {'score': 0.0, 'justification': 'No runnable test cases.', 'details': details}
            score = passed / total
            justification = f'Passed {passed}/{total} test cases.'
            # if docker unavailable explain in details
            if not _DOCKER_AVAILABLE:
                justification = 'Docker not available to run tests; could not execute test cases.'
                return {'score': 0.0, 'justification': justification, 'details': details}
            return {'score': float(score), 'justification': justification, 'details': details}

        # 7) Otherwise, conceptual scoring
        score, justification = _conceptual_score(question, fixed, lang)
        return {'score': float(score), 'justification': justification, 'details': details}

    except Exception as e:
        return {'score': 0.0, 'justification': f'Analyzer internal error: {e}', 'details': details}
