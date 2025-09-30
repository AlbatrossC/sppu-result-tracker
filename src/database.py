import os
import psycopg2
from psycopg2.extras import execute_batch
from datetime import datetime
from dotenv import load_dotenv

# Load environment
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

# Parse date safely
def parse_date(date_str: str) -> str:
    """Convert '25- August- 2025' → '2025-08-25'"""
    months = {
        'january': '01', 'jan': '01', 'february': '02', 'feb': '02',
        'march': '03', 'mar': '03', 'april': '04', 'apr': '04',
        'may': '05', 'june': '06', 'jun': '06', 'july': '07', 'jul': '07',
        'august': '08', 'aug': '08', 'september': '09', 'sep': '09', 'sept': '09',
        'october': '10', 'oct': '10', 'november': '11', 'nov': '11',
        'december': '12', 'dec': '12'
    }
    try:
        parts = [p.strip() for p in date_str.split('-')]
        if len(parts) == 3:
            day = parts[0].zfill(2)
            month = months.get(parts[1].lower())
            year = parts[2]
            if month:
                return f"{year}-{month}-{day}"
    except Exception as e:
        print(f"⚠️ Error parsing date '{date_str}': {e}")
    return None


def sync_database(json_data: list):
    """Sync scraper JSON with results + results_history in one transaction."""
    if not json_data:
        print("⚠️ No data provided to sync")
        return

    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()
    stats = {"added": 0, "removed": 0, "unchanged": 0}

    try:
        # Prepare scraper data
        json_records = {}
        for item in json_data:
            course_name = item.get("course_name", "").strip()
            date_str = item.get("result_date", "").strip()
            parsed_date = parse_date(date_str)
            if course_name and parsed_date:
                json_records[(course_name, parsed_date)] = True

        # Fetch current active records
        cursor.execute("""
            SELECT id, course_name, result_date::text 
            FROM results WHERE is_active=TRUE
        """)
        existing_records = {(row[1], row[2]): row[0] for row in cursor.fetchall()}

        json_keys = set(json_records.keys())
        existing_keys = set(existing_records.keys())

        to_add = json_keys - existing_keys
        to_remove = existing_keys - json_keys
        to_update = json_keys & existing_keys

        # -------------------
        # Insert new results
        # -------------------
        if to_add:
            execute_batch(cursor, """
                INSERT INTO results (course_name, result_date, last_seen, is_active)
                VALUES (%s, %s, NOW(), TRUE)
                ON CONFLICT (course_name, result_date)
                DO UPDATE SET last_seen=NOW(), is_active=TRUE
            """, [(c, d) for c, d in to_add], page_size=100)

            execute_batch(cursor, """
                INSERT INTO results_history (course_name, result_date, change_type, notification_sent)
                VALUES (%s, %s, 'added', FALSE)
            """, [(c, d) for c, d in to_add], page_size=100)

            stats["added"] = len(to_add)

        # -------------------
        # Update unchanged rows (refresh last_seen)
        # -------------------
        if to_update:
            execute_batch(cursor, """
                UPDATE results SET last_seen=NOW(), is_active=TRUE
                WHERE course_name=%s AND result_date=%s
            """, [(c, d) for c, d in to_update], page_size=100)

            stats["unchanged"] = len(to_update)

        # -------------------
        # Handle removed results
        # -------------------
        if to_remove:
            execute_batch(cursor, """
                UPDATE results SET is_active=FALSE, last_seen=NOW()
                WHERE course_name=%s AND result_date=%s
            """, [(c, d) for c, d in to_remove], page_size=100)

            execute_batch(cursor, """
                INSERT INTO results_history (course_name, result_date, change_type, notification_sent)
                VALUES (%s, %s, 'removed', FALSE)
            """, [(c, d) for c, d in to_remove], page_size=100)

            stats["removed"] = len(to_remove)

        conn.commit()
        print(f"✅ Database sync complete: {stats}")

    except Exception as e:
        conn.rollback()
        print(f"❌ Error syncing database: {e}")
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    import json
    with open("sppu_subjects.json", "r", encoding="utf-8") as f:
        data = json.load(f)
    sync_database(data)
