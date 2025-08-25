# worker.py
import os
import tempfile
import docker
import re  # Make sure 're' is imported for regular expressions
from flask import Flask, request, jsonify

app = Flask(__name__)
app.secret_key = os.environ.get("WORKER_SECRET_KEY", "a-default-secret-key")

def run_code_in_docker(code: str, language: str, test_cases: list) -> dict:
    """
    Executes code in a sandboxed Docker container and checks it against test cases.
    """
    try:
        client = docker.from_env()
    except docker.errors.DockerException:
        return {"error": "Docker daemon is not running on the worker VM."}

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
                    image,
                    command=run_command,
                    volumes={temp_dir: {'bind': '/app', 'mode': 'rw'}},
                    working_dir="/app", remove=True, network_disabled=True, mem_limit='256m'
                ).decode('utf-8').strip()
                
                expected_output = str(case.get('expected_output', '')).strip()

                # === NEW: Smarter Comparison Logic ===
                # Find all numbers (including decimals and negatives) in both strings
                numbers_from_actual = re.findall(r'-?\d+\.?\d*', container_output)
                numbers_from_expected = re.findall(r'-?\d+\.?\d*', expected_output)

                # 1. Primary Check: If expected output contains numbers, compare the extracted numbers.
                if numbers_from_expected:
                    if numbers_from_actual == numbers_from_expected:
                        passed_count += 1
                        continue # Move to the next test case

                # 2. Fallback Check: If no numbers are involved, or if the numeric check fails,
                #    fall back to a case-insensitive string comparison.
                if container_output.lower() == expected_output.lower():
                    passed_count += 1
            
            except docker.errors.ContainerError as e:
                print(f"Container error: {e.stderr.decode('utf-8')}")
                continue
            except Exception as e:
                print(f"An unknown execution error occurred: {e}")
                continue
    
    return {"passed_count": passed_count}

@app.route('/execute', methods=['POST'])
def execute_code():
    """
    Flask endpoint to receive code execution requests. It authenticates the
    request, validates the payload, and calls the Docker execution function.
    """
    if request.headers.get('X-Auth-Key') != app.secret_key:
        return jsonify({"error": "Unauthorized"}), 401
        
    data = request.get_json()
    if not all(k in data for k in ['code', 'language', 'test_cases']):
        return jsonify({"error": "Missing required fields"}), 400

    result = run_code_in_docker(data['code'], data['language'], data['test_cases'])
    
    if "error" in result:
        return jsonify(result), 500
        
    return jsonify(result), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)