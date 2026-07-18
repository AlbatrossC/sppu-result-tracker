import hmac
import os
from contextlib import closing
from datetime import datetime, timezone

import psycopg2
import requests
from dotenv import load_dotenv
from flask import Flask, jsonify, render_template, request, send_from_directory
from psycopg2.extras import RealDictCursor

from src.settings import _validate_database_url


load_dotenv()

app = Flask(__name__)

DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
WORKFLOW_SECRET = os.getenv("WORKFLOW_SECRET", "").strip()
GH_API_TOKEN = os.getenv("GH_API_TOKEN", "").strip()
REPO_NAME = os.getenv("GH_REPO_NAME", "AlbatrossC/sppu-result-tracker").strip()
WORKFLOW_FILE = os.getenv("GH_WORKFLOW_FILE", "fetch.yml").strip()
REF_BRANCH = os.getenv("GH_REF_BRANCH", "main").strip()


def get_db():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not configured")
    _validate_database_url(DATABASE_URL)
    return psycopg2.connect(
        DATABASE_URL,
        cursor_factory=RealDictCursor,
        connect_timeout=10,
        application_name="sppu-result-tracker-web",
    )


@app.get("/")
def index():
    return render_template("index.html")


@app.get("/api/results")
def get_results():
    try:
        with closing(get_db()) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT course_name, result_date, is_active, last_seen
                    FROM results
                    WHERE is_active = TRUE
                    ORDER BY result_date DESC, course_name
                    """
                )
                rows = cursor.fetchall()
        response = jsonify(rows)
        response.headers["Cache-Control"] = "public, max-age=60"
        return response
    except Exception:
        app.logger.exception("Could not load active results")
        return jsonify({"error": "Results are temporarily unavailable"}), 503


@app.get("/api/health")
def get_health():
    try:
        with closing(get_db()) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT status, started_at, finished_at, parsed_count,
                           added_count, updated_count, removed_count
                    FROM tracker_runs
                    ORDER BY started_at DESC
                    LIMIT 1
                    """
                )
                latest = cursor.fetchone()
                cursor.execute(
                    """
                    SELECT finished_at
                    FROM tracker_runs
                    WHERE status = 'success'
                    ORDER BY started_at DESC
                    LIMIT 1
                    """
                )
                success = cursor.fetchone()
                cursor.execute("SELECT COUNT(*) AS count FROM results WHERE is_active = TRUE")
                active_count = cursor.fetchone()["count"]
                cursor.execute(
                    "SELECT COUNT(*) AS count FROM notification_outbox WHERE status = 'pending'"
                )
                pending_count = cursor.fetchone()["count"]
                cursor.execute("SELECT COUNT(*) AS count FROM notification_outbox WHERE status = 'dead'")
                dead_count = cursor.fetchone()["count"]

        last_success = success["finished_at"] if success else None
        stale = True
        if last_success:
            stale = (datetime.now(timezone.utc) - last_success).total_seconds() > 30 * 60
        payload = {
            "status": latest["status"] if latest else "not_started",
            "last_success": last_success,
            "last_run": latest,
            "stale": stale,
            "active_results": active_count,
            "pending_notifications": pending_count,
            "dead_notifications": dead_count,
        }
        response = jsonify(payload)
        response.headers["Cache-Control"] = "no-store"
        return response
    except Exception:
        app.logger.exception("Could not load tracker health")
        return jsonify({"error": "Tracker health is temporarily unavailable"}), 503


@app.post("/api/trigger")
def trigger_workflow():
    if not WORKFLOW_SECRET or not GH_API_TOKEN:
        app.logger.error("Workflow trigger environment variables are missing")
        return jsonify({"error": "Workflow trigger is not configured"}), 503

    data = request.get_json(silent=True) or {}
    supplied_key = str(data.get("key", ""))
    if not hmac.compare_digest(supplied_key, WORKFLOW_SECRET):
        return jsonify({"error": "Unauthorized"}), 401

    headers = {
        "Authorization": f"Bearer {GH_API_TOKEN}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    url = f"https://api.github.com/repos/{REPO_NAME}/actions/workflows/{WORKFLOW_FILE}/dispatches"

    try:
        response = requests.post(
            url,
            headers=headers,
            json={"ref": REF_BRANCH},
            timeout=(5, 10),
        )
        if response.status_code == 204:
            return jsonify(
                {
                    "message": "Workflow accepted",
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                }
            )

        app.logger.error("GitHub workflow dispatch failed with status %s", response.status_code)
        return jsonify({"error": "GitHub did not accept the workflow trigger"}), 502
    except requests.RequestException:
        app.logger.exception("GitHub workflow dispatch request failed")
        return jsonify({"error": "GitHub is temporarily unavailable"}), 502


@app.get("/robots.txt")
def robots():
    return send_from_directory(".", "robots.txt")


if __name__ == "__main__":
    app.run(
        debug=os.getenv("FLASK_DEBUG") == "1",
        host="0.0.0.0",
        port=int(os.getenv("PORT", "5000")),
    )
