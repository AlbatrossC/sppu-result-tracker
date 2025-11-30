import os
import json
import time
import jwt
import psycopg2
import requests
from datetime import datetime
from psycopg2.extras import RealDictCursor

# Environment variables
DATABASE_URL = os.getenv("DATABASE_URL")
DISCORD_WEBHOOK = os.getenv("DISCORD_WEBHOOK_URL")
FCM_JSON = json.loads(os.getenv("FCM_SERVICE_ACCOUNT_JSON"))

# Establish database connection with dict cursor for easy access
def db():
    try:
        conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
        print("✓ Database connected successfully")
        return conn
    except Exception as e:
        print(f"✗ Database connection failed: {e}")
        raise

# Send notification message to Discord webhook
def send_discord(msg):
    if not DISCORD_WEBHOOK:
        print("⚠ Discord webhook not configured")
        return
    
    try:
        response = requests.post(DISCORD_WEBHOOK, json={"content": msg}, timeout=10)
        if response.status_code == 204:
            print(f"✓ Discord notification sent: {msg[:50]}...")
        else:
            print(f"✗ Discord failed with status {response.status_code}: {response.text}")
    except Exception as e:
        print(f"✗ Discord notification error: {e}")

# Generate OAuth2 access token for FCM using service account credentials
def generate_access_token():
    try:
        now = int(time.time())
        payload = {
            "iss": FCM_JSON["client_email"],
            "scope": "https://www.googleapis.com/auth/firebase.messaging",
            "aud": FCM_JSON["token_uri"],
            "iat": now,
            "exp": now + 3600
        }
        signed = jwt.encode(payload, FCM_JSON["private_key"], algorithm="RS256")
        
        r = requests.post(FCM_JSON["token_uri"], data={
            "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
            "assertion": signed
        }, timeout=10)
        
        if r.status_code == 200:
            print("✓ FCM access token generated")
            return r.json()["access_token"]
        else:
            print(f"✗ Token generation failed: {r.status_code} - {r.text}")
            raise Exception("Failed to generate access token")
    except Exception as e:
        print(f"✗ Access token generation error: {e}")
        raise

# Send FCM push notification to a specific device token
def send_fcm(conn, cur, token, title, body):
    try:
        access_token = generate_access_token()
        url = "https://fcm.googleapis.com/v1/projects/sppu-result-tracker/messages:send"
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
        payload = {
            "message": {
                "token": token,
                "notification": {"title": title, "body": body},
                "android": {"priority": "high", "ttl": "0s"},
                "webpush": {"headers": {"Urgency": "high"}},
                "data": {"timestamp": datetime.now().isoformat()}
            }
        }
        
        r = requests.post(url, headers=headers, json=payload, timeout=10)
        
        # Remove invalid/unregistered tokens from database
        if r.status_code == 404 and "UNREGISTERED" in r.text:
            cur.execute("DELETE FROM fcm_tokens WHERE token=%s", (token,))
            conn.commit()
            print(f"🗑 Removed invalid token: {token[:20]}...")
            return
        
        if r.status_code == 200:
            print(f"✓ FCM sent to token: {token[:20]}...")
        else:
            print(f"✗ FCM failed ({r.status_code}): {r.text}")
    except Exception as e:
        print(f"✗ FCM send error for token {token[:20]}...: {e}")

# Main processing function to send notifications for result changes
def process():
    print(f"\n{'='*60}")
    print(f"🔔 Starting notification process at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}\n")
    
    try:
        conn = db()
        cur = conn.cursor()
        
        # Fetch unsent notifications ordered by ID
        cur.execute("SELECT * FROM results_history WHERE notification_sent=FALSE ORDER BY id ASC")
        rows = cur.fetchall()
        print(f"📋 Found {len(rows)} pending notification(s)")
        
        if not rows:
            print("✓ No pending notifications. Exiting.")
            cur.close()
            conn.close()
            return
        
        # Fetch all registered FCM tokens
        cur.execute("SELECT token FROM fcm_tokens")
        tokens = cur.fetchall()
        print(f"📱 Found {len(tokens)} registered device(s)\n")
        
        if not tokens:
            print("⚠ No FCM tokens registered. Skipping FCM notifications.")
        
        # Process each result change
        for idx, row in enumerate(rows, 1):
            print(f"\n--- Processing notification {idx}/{len(rows)} ---")
            print(f"📝 Course: {row['course_name']}")
            print(f"🔄 Change type: {row['change_type']}")
            
            course = row["course_name"]
            change_type = row["change_type"]
            
            # Generate appropriate notification title and message based on change type
            if change_type == "added":
                title = f"📢 {course}"
                msg = "Result has been declared!"
            elif change_type == "updated":
                title = f"📢 {course}"
                msg = "Result has been updated!"
            elif change_type == "removed":
                title = f"📢 {course}"
                msg = "Result has been removed!"
            else:
                title = f"📢 {course}"
                msg = "Result has been updated!"
            
            print(f"💬 Message: {msg}")
            
            # Send Discord notification
            discord_msg = f"📢 {course} - {msg}"
            send_discord(discord_msg)
            
            # Send FCM notifications to all registered tokens
            for t in tokens:
                send_fcm(conn, cur, t["token"], title, msg)
            
            # Mark notification as sent in database
            cur.execute("UPDATE results_history SET notification_sent=TRUE WHERE id=%s", (row["id"],))
            conn.commit()
            print(f"✓ Notification {idx} marked as sent")
        
        cur.close()
        conn.close()
        print(f"\n{'='*60}")
        print(f"✓ Notification process completed successfully")
        print(f"{'='*60}\n")
        
    except Exception as e:
        print(f"\n{'='*60}")
        print(f"✗ Fatal error in notification process: {e}")
        print(f"{'='*60}\n")
        raise

if __name__ == "__main__":
    process()