import os
import json
import time
import jwt
import psycopg2
import requests
from datetime import datetime
from psycopg2.extras import RealDictCursor

DATABASE_URL = os.getenv("DATABASE_URL")
DISCORD_WEBHOOK = os.getenv("DISCORD_WEBHOOK_URL")
FCM_JSON = json.loads(os.getenv("FCM_SERVICE_ACCOUNT_JSON"))

def db():
    return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)

def send_discord(msg):
    if DISCORD_WEBHOOK:
        requests.post(DISCORD_WEBHOOK, json={"content": msg})

def generate_access_token():
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
    })
    return r.json()["access_token"]

def send_fcm(conn, cur, token, title, body):
    access_token = generate_access_token()
    url = "https://fcm.googleapis.com/v1/projects/sppu-result-tracker/messages:send"

    headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}

    payload = {
        "message": {
            "token": token,
            "notification": {"title": title, "body": body},
            "android": {"priority": "high", "ttl": "0s"},
            "webpush": {"headers": {"Urgency": "high"}},
            "data": {"timestamp": datetime.now().isoformat()}
        }
    }

    r = requests.post(url, headers=headers, json=payload)

    if r.status_code == 404 and "UNREGISTERED" in r.text:
        cur.execute("DELETE FROM fcm_tokens WHERE token=%s", (token,))
        conn.commit()
        print("🗑 Removed invalid token:", token[:15])
        return

    print("✓ FCM ->", token[:25])

def process():
    conn = db()
    cur = conn.cursor()

    cur.execute("SELECT * FROM results_history WHERE notification_sent=FALSE ORDER BY id ASC")
    rows = cur.fetchall()

    cur.execute("SELECT token FROM fcm_tokens")
    tokens = cur.fetchall()

    for row in rows:
        title = "SPPU Result Update"
        course = row["course_name"]
        c = row["change_type"]

        msg = (
            f"{course} result declared!" if c=="added" else
            f"{course} result updated!" if c=="updated" else
            f"{course} result removed!" if c=="removed" else
            f"{course} updated."
        )

        send_discord(msg)

        for t in tokens:
            send_fcm(conn, cur, t["token"], title, msg)

        cur.execute("UPDATE results_history SET notification_sent=TRUE WHERE id=%s", (row["id"],))
        conn.commit()

    cur.close()
    conn.close()

if __name__ == "__main__":
    process()
