from flask import Flask, request, jsonify, render_template, send_from_directory
import requests
import os
from dotenv import load_dotenv
from datetime import datetime
import psycopg2
from psycopg2.extras import RealDictCursor
from pywebpush import webpush, WebPushException

load_dotenv()

app = Flask(__name__)

# ENV Variables
DATABASE_URL = os.getenv("DATABASE_URL")
VAPID_PUBLIC = os.getenv("VAPID_PUBLIC_KEY")
VAPID_PRIVATE = os.getenv("VAPID_PRIVATE_KEY")
VAPID_EMAIL = os.getenv("VAPID_EMAIL")

WORKFLOW_SECRET = os.getenv("WORKFLOW_SECRET")
GH_API_TOKEN = os.getenv("GH_API_TOKEN")

# GitHub Config
REPO_NAME = "AlbatrossC/sppu-result-tracker"
WORKFLOW_FILE = "fetch.yml"
REF_BRANCH = "main"


def get_db():
    return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)


@app.route("/")
def index():
    return render_template("index.html")


# 🔹 Return latest results
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


# 🔹 NEW — Return VAPID public key
@app.route("/api/public-key")
def public_key():
    return jsonify({"publicKey": VAPID_PUBLIC})


# 🔹 NEW — Save push subscription
@app.route("/api/subscribe", methods=["POST"])
def subscribe():
    data = request.json
    endpoint = data["endpoint"]
    p256dh = data["keys"]["p256dh"]
    auth_key = data["keys"]["auth"]

    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO push_subscriptions (endpoint, p256dh, auth)
        VALUES (%s, %s, %s)
        ON CONFLICT (endpoint) DO NOTHING;
    """, (endpoint, p256dh, auth_key))

    conn.commit()
    cur.close()
    conn.close()

    return jsonify({"message": "Subscribed"}), 201


# 🔹 GitHub trigger (unchanged)
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

    payload = {"ref": REF_BRANCH}
    url = f"https://api.github.com/repos/{REPO_NAME}/actions/workflows/{WORKFLOW_FILE}/dispatches"

    try:
        response = requests.post(url, headers=headers, json=payload, timeout=10)

        if response.status_code == 204:
            return jsonify({
                "message": "Workflow triggered successfully!",
                "timestamp": datetime.now().isoformat()
            }), 200

        return jsonify({
            "error": "Failed to trigger workflow",
            "status_code": response.status_code,
            "details": response.text
        }), response.status_code

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/service-worker.js")
def sw():
    return send_from_directory(".", "service-worker.js")


@app.route("/robots.txt")
def robots():
    return send_from_directory(".", "robots.txt")


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
