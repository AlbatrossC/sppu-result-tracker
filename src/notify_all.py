import os
import json
import time
import jwt  # pip install PyJWT
import psycopg2
import requests
from datetime import datetime

from psycopg2.extras import RealDictCursor

# ENV variables
DATABASE_URL = os.getenv("DATABASE_URL")
DISCORD_WEBHOOK = os.getenv("DISCORD_WEBHOOK_URL")

# Full service account JSON (string stored in GitHub Secrets or .env)
FCM_SERVICE_ACCOUNT = os.getenv("FCM_SERVICE_ACCOUNT_JSON")


# -----------------------------------
# Database Connection
# -----------------------------------
def db():
    return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)


# -----------------------------------
# Send Discord notification
# -----------------------------------
def send_discord(message):
    if not DISCORD_WEBHOOK:
        print("⚠ No Discord webhook configured.")
        return

    try:
        requests.post(DISCORD_WEBHOOK, json={"content": message})
        print("✓ Discord message sent")
    except Exception as e:
        print("❌ Discord error:", e)


# -----------------------------------
# FCM v1 Auth → Generate OAuth2 Access Token
# -----------------------------------
def generate_access_token():
    service_account = json.loads(FCM_SERVICE_ACCOUNT)

    now = int(time.time())
    expires = now + 3600  # valid 1 hour

    payload = {
        "iss": service_account["client_email"],
        "scope": "https://www.googleapis.com/auth/firebase.messaging",
        "aud": service_account["token_uri"],
        "iat": now,
        "exp": expires
    }

    # Sign with private key
    signed_jwt = jwt.encode(
        payload,
        service_account["private_key"],
        algorithm="RS256"
    )

    # Exchange JWT for OAuth2 token
    r = requests.post(service_account["token_uri"],
                      data={
                          "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
                          "assertion": signed_jwt
                      })

    token = r.json().get("access_token")
    if not token:
        raise Exception("Failed to generate access token")

    return token


# -----------------------------------
# Send Firebase Cloud Messaging push
# -----------------------------------
def send_fcm(token, title, body):
    access_token = generate_access_token()

    url = "https://fcm.googleapis.com/v1/projects/sppu-result-tracker/messages:send"

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json; UTF-8"
    }

    payload = {
        "message": {
            "token": token,
            "notification": {
                "title": title,
                "body": body
            },
            "data": {
                "click_action": "FLUTTER_NOTIFICATION_CLICK",
                "timestamp": datetime.now().isoformat()
            }
        }
    }

    response = requests.post(url, headers=headers, json=payload)

    if response.status_code != 200:
        print(f"❌ FCM error ({response.status_code}): {response.text}")
    else:
        print("✓ FCM sent →", token[:20], "...")


# -----------------------------------
# MAIN PROCESSING LOGIC
# -----------------------------------
def process():
    conn = db()
    cur = conn.cursor()

    # 1️⃣ Get all unsent history items
    cur.execute("""
        SELECT id, course_name, result_date, change_type
        FROM results_history
        WHERE notification_sent = FALSE
        ORDER BY id ASC;
    """)
    rows = cur.fetchall()

    if not rows:
        print("No new results to notify.")
        return

    # 2️⃣ Get all FCM tokens
    cur.execute("SELECT token FROM fcm_tokens;")
    tokens = cur.fetchall()

    # 3️⃣ Loop through new events
    for row in rows:
        course = row["course_name"]
        ctype = row["change_type"]

        # Build notification text
        if ctype == "added":
            msg = f"{course} result declared!"
        elif ctype == "updated":
            msg = f"{course} result updated!"
        elif ctype == "removed":
            msg = f"{course} result removed!"
        else:
            msg = f"{course} updated."

        discord_text = f"📢 {msg}"

        # 4️⃣ Send Discord
        send_discord(discord_text)

        # 5️⃣ Send Firebase push to ALL users
        for t in tokens:
            send_fcm(t["token"], "📢 SPPU Result Update", msg)

        # 6️⃣ Mark notification as sent
        cur.execute("""
            UPDATE results_history
            SET notification_sent = TRUE
            WHERE id = %s;
        """, (row["id"],))
        conn.commit()

        print(f"✓ Marked ID {row['id']} as sent")

    cur.close()
    conn.close()
    print("All notifications processed!")


if __name__ == "__main__":
    process()
