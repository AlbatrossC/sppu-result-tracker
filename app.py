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


@app.route("/api/subscribe", methods=["POST"])
def subscribe_to_course():
    """Subscribe user to course notifications"""
    try:
        data = request.get_json()
        onesignal_id = data.get('onesignal_id')
        course_name = data.get('course_name')
        
        if not onesignal_id or not course_name:
            return jsonify({"error": "Missing onesignal_id or course_name"}), 400

        conn = get_db_connection()
        cur = conn.cursor()
        
        # Insert or update subscription
        cur.execute("""
            INSERT INTO subscriptions (user_id, course_name, push_subscription_json, active)
            VALUES (%s, %s, %s, TRUE)
            ON CONFLICT (user_id, course_name) 
            DO UPDATE SET 
                push_subscription_json = EXCLUDED.push_subscription_json,
                active = TRUE,
                updated_at = NOW()
        """, (onesignal_id, course_name, json.dumps({"onesignal_id": onesignal_id})))
        
        conn.commit()
        cur.close()
        conn.close()
        
        return jsonify({"message": f"Subscribed to {course_name} successfully"})
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/send-notifications", methods=["POST"])
def send_notifications():
    """Send notifications for new results (called from your workflow)"""
    try:
        data = request.get_json()
        if not data or data.get("key") != WORKFLOW_SECRET:
            return jsonify({"error": "Unauthorized"}), 401

        conn = get_db_connection()
        cur = conn.cursor()
        
        cur.execute("""
            SELECT DISTINCT course_name 
            FROM results_history 
            WHERE notification_sent = FALSE
        """)
        courses_with_updates = [row['course_name'] for row in cur.fetchall()]
        
        notifications_sent = 0
        
        for course_name in courses_with_updates:
            cur.execute("""
                SELECT push_subscription_json 
                FROM subscriptions 
                WHERE course_name = %s AND active = TRUE
            """, (course_name,))
            
            subscribers = cur.fetchall()
            onesignal_ids = []
            
            for subscriber in subscribers:
                if subscriber['push_subscription_json']:
                    sub_data = json.loads(subscriber['push_subscription_json'])
                    if 'onesignal_id' in sub_data:
                        onesignal_ids.append(sub_data['onesignal_id'])
            
            # Send notification via OneSignal
            if onesignal_ids and ONESIGNAL_REST_API_KEY:
                headers = {
                    "Content-Type": "application/json; charset=utf-8",
                    "Authorization": f"Basic {ONESIGNAL_REST_API_KEY}"
                }
                
                payload = {
                    "app_id": ONESIGNAL_APP_ID,
                    "include_player_ids": onesignal_ids,
                    "headings": {"en": f"New Results Available!"},
                    "contents": {"en": f"Results for {course_name} have been announced!"},
                    "url": "https://yourdomain.com"
                }
                
                try:
                    response = requests.post(
                        "https://onesignal.com/api/v1/notifications",
                        headers=headers,
                        json=payload
                    )
                    if response.status_code == 200:
                        notifications_sent += len(onesignal_ids)
                except Exception as e:
                    print(f"Error sending notification: {e}")
            
            cur.execute("""
                UPDATE results_history 
                SET notification_sent = TRUE 
                WHERE course_name = %s AND notification_sent = FALSE
            """, (course_name,))
        
        conn.commit()
        cur.close()
        conn.close()
        
        return jsonify({
            "message": f"Notifications processed for {len(courses_with_updates)} courses",
            "notifications_sent": notifications_sent
        })
        
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


@app.route('/OneSignalSDKWorker.js')
def onesignal_worker():
    """Serve OneSignal service worker"""
    return send_from_directory('.', 'OneSignalSDKWorker.js')


@app.route('/robots.txt')
def robots():
    return send_from_directory('.', 'robots.txt')


if __name__ == "__main__":
    app.run(debug=True)