import os
import psycopg2
from psycopg2.extras import execute_batch
from dotenv import load_dotenv

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

def parse_date(date_str: str) -> str:
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
    except:
        pass
    return None

def sync_database(json_data: list):
    if not json_data:
        print("No data to sync.")
        return

    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()

    try:
        cursor.execute("TRUNCATE latest_scrape")

        latest = []
        for item in json_data:
            name = item.get("course_name", "").strip()
            raw = item.get("result_date", "").strip()
            d = parse_date(raw)
            if name and d:
                latest.append((name, d))

        if latest:
            execute_batch(cursor, """
                INSERT INTO latest_scrape (course_name, result_date)
                VALUES (%s, %s)
            """, latest, page_size=100)

        cursor.execute("SELECT course_name, result_date::text FROM previous_results")
        prev = {(row[0], row[1]) for row in cursor.fetchall()}

        cursor.execute("SELECT course_name, result_date::text FROM latest_scrape")
        now = {(row[0], row[1]) for row in cursor.fetchall()}

        new_items = now - prev
        removed_items = prev - now
        maybe_updated = now & prev

        updates = []
        for name, _ in maybe_updated:
            cursor.execute("""
                SELECT p.result_date::text, l.result_date::text
                FROM previous_results p
                JOIN latest_scrape l ON p.course_name=l.course_name
                WHERE p.course_name=%s
            """, (name,))
            row = cursor.fetchone()
            if row and row[0] != row[1]:
                updates.append((name, row[0], row[1]))

        if new_items:
            execute_batch(cursor, """
                INSERT INTO notifications (event_type, course_name, old_date, new_date)
                VALUES ('new', %s, NULL, %s)
            """, [(n, d) for n, d in new_items], page_size=100)

            execute_batch(cursor, """
                INSERT INTO history (course_name, result_date)
                VALUES (%s, %s)
            """, [(n, d) for n, d in new_items], page_size=100)

        if updates:
            execute_batch(cursor, """
                INSERT INTO notifications (event_type, course_name, old_date, new_date)
                VALUES ('updated', %s, %s, %s)
            """, [(n, o, nw) for n, o, nw in updates], page_size=100)

            execute_batch(cursor, """
                INSERT INTO history (course_name, result_date)
                VALUES (%s, %s)
            """, [(n, nw) for n, _, nw in updates], page_size=100)

        if removed_items:
            execute_batch(cursor, """
                INSERT INTO notifications (event_type, course_name, old_date, new_date)
                VALUES ('removed', %s, %s, NULL)
            """, [(n, d) for n, d in removed_items], page_size=100)

        cursor.execute("TRUNCATE previous_results")

        execute_batch(cursor, """
            INSERT INTO previous_results (course_name, result_date)
            VALUES (%s, %s)
        """, list(now), page_size=100)

        cursor.execute("TRUNCATE latest_scrape")

        conn.commit()
        print("Sync complete.")

    except Exception as e:
        conn.rollback()
        print("Error syncing:", e)
    finally:
        cursor.close()
        conn.close()
