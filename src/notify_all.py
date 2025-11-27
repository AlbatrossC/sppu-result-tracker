import os
import json
import psycopg2
import requests
from psycopg2.extras import RealDictCursor
from pywebpush import webpush, WebPushException

# ENV Variables
DATABASE_URL = os.getenv("DATABASE_URL")
DISCORD_WEBHOOK = os.getenv("DISCORD_WEBHOOK_URL")
VAPID_PUBLIC = os.getenv("VAPID_PUBLIC_KEY")
VAPID_PRIVATE = os.getenv("VAPID_PRIVATE_KEY")
VAPID_EMAIL = os.getenv("VAPID_EMAIL")


# ---------------------------
# DB Connection
# ---------------------------
def db():
    return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)


# ---------------------------
# Discord Sender
# ---------------------------
def send_discord(message):
    if not DISCORD_WEBHOOK:
        print("⚠ No Discord webhook URL configured.")
        return

    try:
        requests.post(DISCORD_WEBHOOK, json={"content": message})
        print("✓ Discord notification sent")
    except Exception as e:
        print("❌ Discord send failed:", e)


# ---------------------------
# Web Push Sender
# ---------------------------
def send_push(subscription, data_json):
    try:
        webpush(
            subscription_info={
                "endpoint": subscription["endpoint"],
                "keys": {
                    "p256dh": subscription["p256dh"],
                    "auth": subscription["auth"]
                }
            },
            data=data_json,
            vapid_private_key=VAPID_PRIVATE,
            vapid_public_key=VAPID_PUBLIC,     # ✅ ADDED (required)
            vapid_claims={"sub": VAPID_EMAIL}
        )
        print("✓ Web Push sent →", subscription["endpoint"][:40])
    except WebPushException as e:
        print("❌ Web Push failed:", e)


# ---------------------------
# Main Logic (ONE PASS)
# ---------------------------
def process():
    conn = db()
    cur = conn.cursor()

    # 1. Get all unsent notifications
    cur.execute("""
        SELECT id, course_name, result_date, change_type
        FROM results_history
        WHERE notification_sent = FALSE
        ORDER BY id ASC;
    """)
    rows = cur.fetchall()

    if not rows:
        print("No notifications to process.")
        return

    # 2. Get all push subscribers
    cur.execute("SELECT * FROM push_subscriptions;")
    subscribers = cur.fetchall()

    # 3. Process each row ONCE
    for row in rows:
        course = row["course_name"]
        ctype = row["change_type"]

        # Build message for Discord
        if ctype == "added":
            discord_msg = f"📢 **{course}** result declared!"
            push_body = f"{course} result declared!"
        elif ctype == "updated":
            discord_msg = f"🔄 **{course}** result updated!"
            push_body = f"{course} result updated!"
        elif ctype == "removed":
            discord_msg = f"❌ **{course}** result removed!"
            push_body = f"{course} result removed!"
        else:
            discord_msg = f"ℹ Update for {course}"
            push_body = f"{course} updated"

        # 4. Send Discord Notification
        send_discord(discord_msg)

        # 5. Send Web Push Notification
        push_message = json.dumps({
            "title": "📢 SPPU Result Update",
            "body": push_body
        })

        for sub in subscribers:
            send_push(sub, push_message)

        # 6. Mark as sent ONCE
        cur.execute("""
            UPDATE results_history
            SET notification_sent = TRUE
            WHERE id = %s;
        """, (row["id"],))
        conn.commit()
        print(f"✓ Marked as sent → ID {row['id']}")

    cur.close()
    conn.close()
    print("All notifications processed successfully.")


if __name__ == "__main__":
    process()
