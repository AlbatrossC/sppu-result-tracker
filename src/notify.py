import os
import psycopg2
import requests
from psycopg2.extras import RealDictCursor

DATABASE_URL = os.getenv("DATABASE_URL")
WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")

def send_discord_message(message: str):
    payload = {"content": message}
    response = requests.post(WEBHOOK_URL, json=payload)
    response.raise_for_status()

def process_notifications():
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    cursor.execute("""
        SELECT id, course_name, result_date, change_type
        FROM results_history
        WHERE notification_sent = FALSE
        ORDER BY id ASC;
    """)

    rows = cursor.fetchall()

    if not rows:
        print("No new notifications to send.")
        return

    for row in rows:
        course = row["course_name"]
        date = row["result_date"]
        change_type = row["change_type"]

        if change_type == "added":
            msg = f"📢 **{course}** result declared!"
        elif change_type == "updated":
            msg = f"🔄 **{course}** result updated!"
        elif change_type == "removed":
            msg = f"❌ **{course}** result removed!"
        else:
            msg = f"ℹ Update for {course}"

        send_discord_message(msg)

        cursor.execute("""
            UPDATE results_history
            SET notification_sent = TRUE
            WHERE id = %s;
        """, (row["id"],))

        conn.commit()

    cursor.close()
    conn.close()

if __name__ == "__main__":
    process_notifications()
    print("Notification process completed.")
