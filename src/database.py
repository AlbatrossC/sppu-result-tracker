import os
import psycopg2
from psycopg2.extras import execute_batch
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")


# Parse scraped date strings like "08- November- 2025"
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

    print(f"[DB] Starting sync. Scraped items: {len(scraped)}\n")

    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()

    now_ts = datetime.now()

    try:
        # Clean scraped list
        cleaned = []
        for item in scraped:
            name = item.get("course_name", "").strip()
            raw = item.get("result_date", "").strip()
            parsed = parse_date(raw)
            if name and parsed:
                cleaned.append((name, parsed))

        print(f"[DB] Valid cleaned records: {len(cleaned)}\n")
        if not cleaned:
            return

        scraped_set = set(cleaned)

        # Load CURRENT active snapshot
        cursor.execute("""
            SELECT course_name, result_date::text, is_active
            FROM results
        """)
        rows = cursor.fetchall()

        active_map = {}
        for name, date, active in rows:
            if active:
                active_map.setdefault(name, set()).add(date)

        # Load timeline state
        cursor.execute("""
            SELECT course_name, result_date::text, is_currently_active
            FROM course_result_timeline
        """)
        timeline_rows = cursor.fetchall()

        timeline_active = {}
        for name, date, active in timeline_rows:
            if active:
                timeline_active.setdefault(name, set()).add(date)

        # Lists for final DB updates
        outputs = []
        history_events = []

        # -------------------------------------------------------
        # 1. Detect Updates (same course, date changed)
        # -------------------------------------------------------
        updates = []

        for course in active_map:
            old_dates = timeline_active.get(course, set())
            new_dates = {d for (c, d) in scraped_set if c == course}

            if len(old_dates) == 1 and len(new_dates) == 1:
                old_date = list(old_dates)[0]
                new_date = list(new_dates)[0]

                if old_date != new_date:
                    updates.append((course, old_date, new_date))

        # Apply updates
        for course, old_date, new_date in updates:
            outputs.append(f"[UPDATED] {course}: {old_date} → {new_date}")

            # deactivate old
            cursor.execute("""
                UPDATE results
                SET is_active=FALSE, updated_at=%s
                WHERE course_name=%s AND result_date=%s AND is_active=TRUE
            """, (now_ts, course, old_date))

            # activate / insert new
            cursor.execute("""
                INSERT INTO results (course_name, result_date, is_active, last_seen)
                VALUES (%s, %s, TRUE, %s)
                ON CONFLICT (course_name, result_date)
                DO UPDATE SET is_active=TRUE, last_seen=EXCLUDED.last_seen
            """, (course, new_date, now_ts))

            # timeline update
            cursor.execute("""
                UPDATE course_result_timeline
                SET is_currently_active = FALSE
                WHERE course_name=%s AND result_date=%s
            """, (course, old_date))

            cursor.execute("""
                INSERT INTO course_result_timeline
                    (course_name, result_date, first_seen, last_seen, times_appeared, is_currently_active)
                VALUES (%s, %s, NOW(), NOW(), 1, TRUE)
                ON CONFLICT (course_name, result_date)
                DO UPDATE SET 
                    last_seen = NOW(),
                    times_appeared = course_result_timeline.times_appeared + 1,
                    is_currently_active = TRUE
            """, (course, new_date))

            history_events.append((course, new_date, 'updated', old_date))

        # Remove updated items from further add/remove detection
        for course, old_date, new_date in updates:
            scraped_set.discard((course, new_date))
            if course in active_map:
                active_map[course].discard(old_date)

        # -------------------------------------------------------
        # 2. Detect NEW added items
        # -------------------------------------------------------
        current_active_pairs = {(c, d) for c in active_map for d in active_map[c]}
        new_items = scraped_set - current_active_pairs

        for course, date in new_items:
            outputs.append(f"[ADDED] {course} → {date}")

            cursor.execute("""
                INSERT INTO results (course_name, result_date, is_active, last_seen)
                VALUES (%s, %s, TRUE, %s)
                ON CONFLICT (course_name, result_date)
                DO UPDATE SET is_active=TRUE, last_seen=EXCLUDED.last_seen
            """, (course, date, now_ts))

            cursor.execute("""
                INSERT INTO course_result_timeline
                    (course_name, result_date, first_seen, last_seen, times_appeared, is_currently_active)
                VALUES (%s, %s, NOW(), NOW(), 1, TRUE)
                ON CONFLICT (course_name, result_date)
                DO UPDATE SET 
                    last_seen=NOW(),
                    times_appeared=course_result_timeline.times_appeared + 1,
                    is_currently_active=TRUE
            """, (course, date))

            history_events.append((course, date, 'added', None))

        # -------------------------------------------------------
        # 3. Detect REMOVED items
        # -------------------------------------------------------
        removed_items = current_active_pairs - scraped_set

        for course, date in removed_items:
            outputs.append(f"[REMOVED] {course} → {date}")

            cursor.execute("""
                UPDATE results
                SET is_active=FALSE, updated_at=%s
                WHERE course_name=%s AND result_date=%s AND is_active=TRUE
            """, (now_ts, course, date))

            cursor.execute("""
                UPDATE course_result_timeline
                SET is_currently_active = FALSE
                WHERE course_name=%s AND result_date=%s
            """, (course, date))

            history_events.append((course, None, 'removed', date))

        # -------------------------------------------------------
        # Insert history events
        # -------------------------------------------------------
        if history_events:
            execute_batch(cursor, """
                INSERT INTO results_history
                    (course_name, result_date, change_type, previous_date)
                VALUES (%s, %s, %s, %s)
            """, history_events, page_size=200)

        # -------------------------------------------------------
        # Update last_seen for all scraped items
        # -------------------------------------------------------
        for course, date in scraped_set:
            cursor.execute("""
                UPDATE results 
                SET last_seen=%s
                WHERE course_name=%s AND result_date=%s
            """, (now_ts, course, date))

        conn.commit()

        # Final log
        print("\n".join(outputs) if outputs else "No changes detected.")
        print("\n[DB] Sync completed successfully.\n")

    except Exception as e:
        conn.rollback()
        print("[DB ERROR]:", e)

    finally:
        cursor.close()
        conn.close()
