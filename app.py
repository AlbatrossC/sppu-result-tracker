from flask import Flask, request, jsonify, render_template, send_from_directory
import requests
import os
from dotenv import load_dotenv
from datetime import datetime
import psycopg2
from psycopg2.extras import RealDictCursor

load_dotenv()

app = Flask(__name__)

# 🔹 ENV Variables
DATABASE_URL = os.getenv("DATABASE_URL")
WORKFLOW_SECRET = os.getenv("WORKFLOW_SECRET")
GH_API_TOKEN = os.getenv("GH_API_TOKEN")

# 🔹 GitHub Config
REPO_NAME = "AlbatrossC/sppu-result-tracker"
WORKFLOW_FILE = "fetch.yml"
REF_BRANCH = "main"


# ----------------------------------------------------
# DB Connection Helper
# ----------------------------------------------------
def get_db():
    return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)


# ----------------------------------------------------
# HOME PAGE
# ----------------------------------------------------
@app.route("/")
def index():
    return render_template("index.html")


# ----------------------------------------------------
# Return all active results
# ----------------------------------------------------
@app.route("/api/results")
def get_results():
    try:
        conn = get_db()
        cur = conn.cursor()

        cur.execute("""
            SELECT course_name, result_date, is_active, last_seen
            FROM results
            WHERE is_active = TRUE
            ORDER BY result_date DESC;
        """)

        rows = cur.fetchall()
        cur.close()
        conn.close()

        return jsonify(rows)

    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ----------------------------------------------------
# Register Firebase FCM Token
# ----------------------------------------------------
@app.route("/api/register-fcm", methods=["POST"])
def register_fcm():
    try:
        token = request.json.get("token")

        if not token:
            return jsonify({"error": "Missing token"}), 400

        conn = get_db()
        cur = conn.cursor()

        cur.execute("""
            INSERT INTO fcm_tokens (token)
            VALUES (%s)
            ON CONFLICT (token) DO NOTHING;
        """, (token,))

        conn.commit()
        cur.close()
        conn.close()

        return jsonify({"message": "Token saved"}), 201

    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ----------------------------------------------------
# Trigger GitHub Action Workflow
# ----------------------------------------------------
@app.route("/api/trigger", methods=["POST"])
def trigger_workflow():
    data = request.get_json()

    if not data or data.get("key") != WORKFLOW_SECRET:
        return jsonify({"error": "Unauthorized"}), 401

    headers = {
        "Authorization": f"Bearer {GH_API_TOKEN}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28"
    }

    url = f"https://api.github.com/repos/{REPO_NAME}/actions/workflows/{WORKFLOW_FILE}/dispatches"
    payload = {"ref": REF_BRANCH}

    try:
        response = requests.post(url, headers=headers, json=payload, timeout=10)

        if response.status_code == 204:
            return jsonify({
                "message": "Workflow triggered successfully",
                "timestamp": datetime.now().isoformat()
            }), 200

        return jsonify({
            "error": "Failed to trigger workflow",
            "status_code": response.status_code,
            "details": response.text
        }), response.status_code

    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ----------------------------------------------------
# Serve Firebase Messaging Service Worker
# ----------------------------------------------------
@app.route("/firebase-messaging-sw.js")
def firebase_sw():
    return send_from_directory(".", "firebase-messaging-sw.js")


# ----------------------------------------------------
# Serve robots.txt
# ----------------------------------------------------
@app.route("/robots.txt")
def robots():
    return send_from_directory(".", "robots.txt")


# ----------------------------------------------------
# Run App
# ----------------------------------------------------
if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
