from flask import Flask, request, jsonify, render_template, send_from_directory
import requests
import os
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime

load_dotenv()

app = Flask(__name__)

# Database connection using Neon
DATABASE_URL = os.getenv("DATABASE_URL")

# Renamed secrets for better security
WORKFLOW_SECRET = os.getenv("WORKFLOW_SECRET")  # Your custom secret key
GH_API_TOKEN = os.getenv("GH_API_TOKEN")  # GitHub Personal Access Token

REPO_NAME = "AlbatrossC/sppu-result-tracker"
WORKFLOW_FILE = "fetch.yml"
REF_BRANCH = "main"


def get_db_connection():
    """Create a database connection"""
    return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)


@app.route('/')
def index():
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Fetch initial active results (latest 10)
        cur.execute("""
            SELECT course_name, result_date, last_seen, is_active
            FROM results
            WHERE is_active = TRUE
            ORDER BY result_date DESC, last_seen DESC
            LIMIT 10
        """)
        
        results = cur.fetchall()
        cur.close()
        conn.close()
        
        return render_template("index.html", results=results)
    
    except Exception as e:
        return f"An error occurred: {e}", 500


@app.route("/api/trigger", methods=["POST"])
def trigger_workflow():
    """
    Trigger GitHub Actions workflow to fetch SPPU results.
    
    Usage:
    POST /api/trigger
    Body: {"key": "your-workflow-secret"}
    
    Returns:
    - 200: Workflow triggered successfully
    - 401: Unauthorized (wrong key)
    - 500: Failed to trigger workflow
    """
    data = request.get_json()
    
    # Verify the secret key
    if not data or data.get("key") != WORKFLOW_SECRET:
        return jsonify({"error": "Unauthorized"}), 401

    # GitHub API headers
    headers = {
        "Authorization": f"Bearer {GH_API_TOKEN}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28"
    }

    # Payload to trigger the workflow
    payload = {
        "ref": REF_BRANCH
    }

    # GitHub API endpoint to dispatch workflow
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


@app.route('/api/results', methods=["GET"])
def get_results():
    """
    Get current active results from the database.
    
    Optional query params:
    - limit: number of results to return (default: 50)
    """
    try:
        limit = request.args.get('limit', 50, type=int)
        
        conn = get_db_connection()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT course_name, result_date, last_seen, is_active
            FROM results
            WHERE is_active = TRUE
            ORDER BY result_date DESC, last_seen DESC
            LIMIT %s
        """, (limit,))
        
        results = cur.fetchall()
        cur.close()
        conn.close()
        
        return jsonify({
            "count": len(results),
            "results": results
        }), 200
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/robots.txt')
def robots():
    return send_from_directory('.', 'robots.txt')


if __name__ == "__main__":
    app.run(debug=True)