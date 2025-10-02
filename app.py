from flask import Flask, request, jsonify, render_template, send_from_directory
import requests
import os
import json
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime

load_dotenv()

app = Flask(__name__)

# Database connection using Neon
DATABASE_URL = os.getenv("DATABASE_URL")

# Renamed secrets for better security
WORKFLOW_SECRET = os.getenv("WORKFLOW_SECRET")
GH_API_TOKEN = os.getenv("GH_API_TOKEN")
ONESIGNAL_APP_ID = os.getenv("ONESIGNAL_APP_ID", "3a51df75-de87-467e-9d37-267b2b130a68")
ONESIGNAL_REST_API_KEY = os.getenv("ONESIGNAL_REST_API_KEY")

REPO_NAME = "AlbatrossC/sppu-result-tracker"
WORKFLOW_FILE = "fetch.yml"
REF_BRANCH = "main"


def get_db_connection():
    """Create a database connection"""
    return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)


@app.route("/")
def index():
    """Serve the main page"""
    return render_template('index.html')


@app.route("/api/courses")
def get_courses():
    """Get list of available courses from results table"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT DISTINCT course_name 
            FROM results 
            WHERE is_active = TRUE 
            ORDER BY course_name
        """)
        courses = [row['course_name'] for row in cur.fetchall()]
        cur.close()
        conn.close()
        return jsonify(courses)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/trigger", methods=["POST"])
def trigger_workflow():
    """Trigger GitHub Actions workflow to fetch SPPU results"""
    data = request.get_json()
    
    if not data or data.get("key") != WORKFLOW_SECRET:
        return jsonify({"error": "Unauthorized"}), 401

    headers = {
        "Authorization": f"Bearer {GH_API_TOKEN}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28"
    }

    payload = {"ref": REF_BRANCH}
    url = f"https://api.github.com/repos/{REPO_NAME}/actions/workflows/{WORKFLOW_FILE}/dispatches"
    
    try:
        response = requests.post(url, headers=headers, json=payload, timeout=10)

        if response.status_code == 204:
            return jsonify({
                "message": "GitHub Actions workflow triggered successfully!",
                "timestamp": datetime.now().isoformat()
            }), 200
        else:
            return jsonify({
                "error": "Failed to trigger workflow",
                "status_code": response.status_code,
                "details": response.text
            }), response.status_code
            
    except requests.exceptions.RequestException as e:
        return jsonify({
            "error": "Request failed",
            "details": str(e)
        }), 500

@app.route('/robots.txt')
def robots():
    return send_from_directory('.', 'robots.txt')


if __name__ == "__main__":
    app.run(debug=True)