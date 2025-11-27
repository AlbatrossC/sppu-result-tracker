import os
import json
import psycopg2
from psycopg2.extras import RealDictCursor
from pywebpush import webpush, WebPushException

DATABASE_URL = os.getenv("DATABASE_URL")
VAPID_PUBLIC = os.getenv("VAPID_PUBLIC_KEY")
VAPID_PRIVATE = os.getenv("VAPID_PRIVATE_KEY")
VAPID_EMAIL = os.getenv("VAPID_EMAIL")

def db():
    return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)


def send_push(subscription, data_json):
    try:
        webpush(
            subscription_info={
                "endpoint": subscription["endpoint"],
                "keys": {
                    "p256dh": subscription["p256dh"],
                    "auth": subscription["auth"],
                },
            },
            data=data_json,
            vapid_private_key=VAPID_PRIVATE,
            vapid_claims={"sub": VAPID_EMAIL},
        )
        print(f"Push sent → {subscription['endpoint'][:30]}...")
    except WebPushException as e:
        print(f"❌ Push failed → {subscription['endpoint'][:30]}...: {e}")


def process_notifications():
    conn = db()
    cur = conn.cursor()

    # Fetch unsent notifications
    cur.execute("""
        SELECT id, course_name, result_date
        FROM results_history
        WHERE notification_sent = FALSE
        ORDER BY id ASC;
    """)
    notifications = cur.fetchall()

    if not notifications:
        print("No new web push notifications.")
        return

    # Fetch all subscribed users
    cur.execute("SELECT * FROM push_subscriptions;")
    subscribers = cur.fetchall()

    if not subscribers:
        print("⚠ No subscribers registered for web push.")
        return

    # Process notifications one-by-one
    for item in notifications:
        message = {
            "title": "📢 SPPU Result Update",
            "body": f"{item['course_name']} result declared!",
        }
        message_json = json.dumps(message)

        for sub in subscribers:
            send_push(sub, message_json)

        # Mark notification as sent
        cur.execute("""
            UPDATE results_history
            SET notification_sent = TRUE
            WHERE id = %s;
        """, (item["id"],))
        conn.commit()
        print(f"✔ Marked notification {item['id']} as sent.")

    cur.close()
    conn.close()
    print("Web push process complete.")


if __name__ == "__main__":
    process_notifications()
