from flask import Flask, request, jsonify, render_template,send_from_directory
import requests
import os
from dotenv import load_dotenv
load_dotenv()

SECRET_KEY = os.getenv("TRIGGER_SECRET")
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")

REPO_NAME = "AlbatrossC/sppu-result-tracker"
WORKFLOW_FILE = "fetch.yml"
REF_BRANCH = "main"

app = Flask(__name__)

@app.route("/")
def home():
    return render_template("index.html")

@app.route("/api/trigger", methods=["POST"])
def serve_trigger_script():
    data = request.get_json()
    if not data or data.get("key") != SECRET_KEY:
        return jsonify({"error": "Unauthorized"}), 401

    headers = {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json"
    }

    payload = {
        "ref": REF_BRANCH
    }

    url = f"https://api.github.com/repos/{REPO_NAME}/actions/workflows/{WORKFLOW_FILE}/dispatches"
    response = requests.post(url, headers=headers, json=payload)

    if response.status_code == 204:
        return jsonify({"message": "GitHub Actions triggered!"})
    else:
        return jsonify({
            "error": "Failed to trigger workflow",
            "status_code": response.status_code,
            "details": response.text
        }), response.status_code
    
@app.route('/robots.txt')
def robots():
    return send_from_directory('.', 'robots.txt')

if __name__ == "__main__":
    app.run(debug=True)
