import os
import psycopg2
from psycopg2.extras import execute_batch
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

# parse dates like “08- November- 2025”
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


def sync_database(scraped: list):
    if not scraped:
        print("[DB] No scraped items. Aborting.")
        return

    print(f"[DB] Starting sync. Items scraped: {len(scraped)}")

    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()

    now_ts = datetime.now()

    try:
        # Build cleaned list
        cleaned = []
        for item in scraped:
            name = item.get("course_name", "").strip()
            raw = item.get("result_date", "").strip()
            parsed = parse_date(raw)
            if name and parsed:
                cleaned.append((name, parsed))
        print(f"[DB] Valid cleaned records: {len(cleaned)}")
        if not cleaned:
            return

        # Track all names+dates scraped right now
        scraped_set = set(cleaned)

        # Fetch current snapshot
        cursor.execute("SELECT course_name, result_date::text FROM results WHERE is_active = TRUE")
        current_active = {(row[0], row[1]) for row in cursor.fetchall()}

        new_items = scraped_set - current_active
        removed_items = current_active - scraped_set

        # Detect updates (same name, different date)
        updates = []
        cursor.execute("SELECT course_name, result_date::text FROM results WHERE is_active = TRUE")
        live_map = {}
        for n, d in cursor.fetchall():
            if n not in live_map:
                live_map[n] = set()
            live_map[n].add(d)

        for (name, new_date) in scraped_set:
            if name in live_map and new_date not in live_map[name]:
                old_date = list(live_map[name])[0]
                if old_date != new_date:
                    updates.append((name, old_date, new_date))

        print(f"[DB] New: {len(new_items)}, Removed: {len(removed_items)}, Updated: {len(updates)}")

        # Mark removed results in results table
        for name, old_date in removed_items:
            cursor.execute("""
                UPDATE results
                SET is_active = FALSE, updated_at = %s
                WHERE course_name=%s AND result_date=%s AND is_active=TRUE
            """, (now_ts, name, old_date))

        # Insert new results into results table
        for name, new_date in new_items:
            cursor.execute("""
                INSERT INTO results (course_name, result_date, is_active, last_seen)
                VALUES (%s, %s, TRUE, %s)
                ON CONFLICT (course_name, result_date)
                DO UPDATE SET last_seen = EXCLUDED.last_seen, is_active = TRUE
            """, (name, new_date, now_ts))

        # Update changed dates
        for name, old_date, new_date in updates:
            cursor.execute("""
                UPDATE results
                SET is_active = FALSE, updated_at=%s
                WHERE course_name=%s AND result_date=%s AND is_active=TRUE
            """, (now_ts, name, old_date))

            cursor.execute("""
                INSERT INTO results (course_name, result_date, is_active, last_seen)
                VALUES (%s, %s, TRUE, %s)
                ON CONFLICT (course_name, result_date)
                DO UPDATE SET last_seen=EXCLUDED.last_seen, is_active=TRUE
            """, (name, new_date, now_ts))

        # Insert timeline UPSERT
        timeline_rows = []
        for name, d in scraped_set:
            timeline_rows.append((name, d))

        execute_batch(cursor, """
            INSERT INTO course_result_timeline 
                (course_name, result_date, first_seen, last_seen, times_appeared, is_currently_active)
            VALUES (%s, %s, NOW(), NOW(), 1, TRUE)
            ON CONFLICT (course_name, result_date)
            DO UPDATE SET
                last_seen = NOW(),
                times_appeared = course_result_timeline.times_appeared + 1,
                is_currently_active = TRUE;
        """, timeline_rows, page_size=100)

        # Mark timeline rows inactive if removed
        for name, old_date in removed_items:
            cursor.execute("""
                UPDATE course_result_timeline
                SET is_currently_active = FALSE
                WHERE course_name=%s AND result_date=%s
            """, (name, old_date))

        # Write results_history events
        hist_rows = []

        for n, d in new_items:
            hist_rows.append((n, d, 'added', None))

        for n, old, new in updates:
            hist_rows.append((n, new, 'updated', old))

        for n, d in removed_items:
            hist_rows.append((n, None, 'removed', d))

        if hist_rows:
            execute_batch(cursor, """
                INSERT INTO results_history (course_name, result_date, change_type, previous_date)
                VALUES (%s, %s, %s, %s)
            """, hist_rows, page_size=100)
            print(f"[DB] History inserted: {len(hist_rows)}")

        # Update last_seen in results for all scraped rows
        for name, date in scraped_set:
            cursor.execute("""
                UPDATE results SET last_seen=%s WHERE course_name=%s AND result_date=%s
            """, (now_ts, name, date))

        conn.commit()
        print("[DB] Sync completed successfully.")

    except Exception as e:
        conn.rollback()
        print("[DB] ERROR during sync:", e)

    finally:
        cursor.close()
        conn.close()
