import os
import psycopg2
from psycopg2.extras import execute_batch
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

# Keep existing parse_date() function here
def parse_date(date_str: str) -> str:
    date_str = date_str.strip().replace(' ', '')
    months = {
        'january': '01', 'jan': '01', 'february': '02', 'feb': '02',
        'march': '03', 'mar': '03', 'april': '04', 'apr': '04',
        'may': '05', 'june': '06', 'jun': '06', 'july': '07', 'jul': '07',
        'august': '08', 'aug': '08', 'september': '09', 'sep': '09', 'sept': '09',
        'october': '10', 'oct': '10', 'november': '11', 'nov': '11', 'december': '12', 'dec': '12'
    }
    try:
        parts = date_str.split('-')
        if len(parts) == 3:
            day = parts[0].zfill(2)
            month = months.get(parts[1].lower(), None)
            year = parts[2]
            if month:
                return f"{year}-{month}-{day}"
    except Exception as e:
        print(f"Error parsing date '{date_str}': {e}")
    return None

def sync_database(json_data: list):
    """Sync JSON data with database in a single transaction."""
    if not json_data:
        print("No data provided to sync")
        return

    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()
    stats = {'added':0, 'removed':0, 'unchanged':0}

    try:
        # Prepare JSON data
        json_records = {}
        for item in json_data:
            course_name = item.get("course_name", "").strip()
            date_str = item.get("result_date", "").strip()
            parsed_date = parse_date(date_str)
            if course_name and parsed_date:
                json_records[(course_name, parsed_date)] = True

        # Get existing records
        cursor.execute("SELECT id, course_name, result_date::text FROM results WHERE is_active=TRUE")
        existing_records = {(row[1], row[2]): row[0] for row in cursor.fetchall()}

        json_keys = set(json_records.keys())
        existing_keys = set(existing_records.keys())

        to_add = json_keys - existing_keys
        to_remove = existing_keys - json_keys
        to_update = json_keys & existing_keys

        # Insert new
        if to_add:
            execute_batch(cursor, """
                INSERT INTO results (course_name, result_date, last_seen, is_active)
                VALUES (%s, %s, NOW(), TRUE)
                ON CONFLICT (course_name, result_date) DO UPDATE SET last_seen = NOW(), is_active = TRUE
            """, [(c,d) for c,d in to_add], page_size=100)
            stats['added'] = len(to_add)

        # Update last_seen for existing
        if to_update:
            execute_batch(cursor, """
                UPDATE results SET last_seen=NOW() WHERE course_name=%s AND result_date=%s
            """, [(c,d) for c,d in to_update], page_size=100)
            stats['unchanged'] = len(to_update)

        # Mark removed as inactive
        if to_remove:
            execute_batch(cursor, """
                UPDATE results SET is_active=FALSE WHERE course_name=%s AND result_date=%s
            """, [(c,d) for c,d in to_remove], page_size=100)
            stats['removed'] = len(to_remove)

        conn.commit()
        print(f"Database sync complete: {stats}")
    except Exception as e:
        conn.rollback()
        print(f"Error syncing database: {e}")
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    import json
    with open("sppu_subjects.json","r",encoding="utf-8") as f:
        data = json.load(f)
    sync_database(data)
