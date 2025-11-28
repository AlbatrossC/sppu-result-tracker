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
    except Exception:
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
        # ---------------------------
        # 1) CLEAN & STRUCTURE SCRAPED
        # ---------------------------
        cleaned = []
        scraped_map = {}  # course -> set(dates)

        for item in scraped:
            name = item.get("course_name", "").strip()
            raw = item.get("result_date", "").strip()
            parsed = parse_date(raw)
            if name and parsed:
                cleaned.append((name, parsed))
                scraped_map.setdefault(name, set()).add(parsed)

        print(f"[DB] Valid cleaned records: {len(cleaned)}\n")
        if not cleaned:
            return

        scraped_set = set(cleaned)  # set of (course, date)

        # ------------------------------------------
        # 2) LOAD CURRENT DB STATE IN MINIMUM QUERIES
        #    (join results and timeline so we don't make two round-trips)
        # ------------------------------------------
        cursor.execute("""
            SELECT r.course_name, r.result_date::text, r.is_active,
                   t.is_currently_active
            FROM results r
            LEFT JOIN course_result_timeline t
            ON r.course_name = t.course_name AND r.result_date = t.result_date
        """)
        rows = cursor.fetchall()

        active_map = {}    # course -> set(active dates from results)
        timeline_map = {}  # course -> set(dates currently active in timeline)

        for course_name, result_date, is_active, is_currently_active in rows:
            if is_active:
                active_map.setdefault(course_name, set()).add(result_date)
            if is_currently_active:
                timeline_map.setdefault(course_name, set()).add(result_date)

        # Precompute current active pairs
        current_active_pairs = {(c, d) for c in active_map for d in active_map[c]}

        outputs = []
        history_events = []

        # ------------------------------------------
        # 3) DETECT UPDATES (old_date -> new_date for same course)
        # ------------------------------------------
        updates = []
        # iterate over courses that exist in both scraped_map and timeline_map/active_map
        for course in scraped_map:
            if course not in timeline_map:
                continue
            old_dates = timeline_map.get(course, set())
            new_dates = scraped_map.get(course, set())

            if len(old_dates) == 1 and len(new_dates) == 1:
                old_date = next(iter(old_dates))
                new_date = next(iter(new_dates))
                if old_date != new_date:
                    updates.append((course, old_date, new_date))

        # Prepare bulk lists
        to_deactivate_results = []   # (now_ts, course, old_date)
        to_insert_results = []       # (course, new_date, now_ts)
        to_timeline_deactivate = []  # (course, old_date)
        to_timeline_upsert = []      # (course, date)
        # history_events appended below where appropriate

        # Apply updates in-memory (collect)
        for course, old_date, new_date in updates:
            outputs.append(f"[UPDATED] {course}: {old_date} → {new_date}")

            to_deactivate_results.append((now_ts, course, old_date))
            to_insert_results.append((course, new_date, now_ts))

            to_timeline_deactivate.append((course, old_date))
            to_timeline_upsert.append((course, new_date))

            history_events.append((course, new_date, 'updated', old_date))

            # remove updated items from further detection
            scraped_set.discard((course, new_date))
            if course in active_map:
                active_map[course].discard(old_date)

        # ------------------------------------------
        # 4) DETECT NEW ADDED ITEMS
        # ------------------------------------------
        new_items = scraped_set - current_active_pairs
        for course, date in new_items:
            outputs.append(f"[ADDED] {course} → {date}")
            to_insert_results.append((course, date, now_ts))
            to_timeline_upsert.append((course, date))
            history_events.append((course, date, 'added', None))

        # ------------------------------------------
        # 5) DETECT REMOVED ITEMS
        # ------------------------------------------
        removed_items = current_active_pairs - scraped_set
        for course, date in removed_items:
            outputs.append(f"[REMOVED] {course} → {date}")
            to_deactivate_results.append((now_ts, course, date))
            to_timeline_deactivate.append((course, date))
            history_events.append((course, None, 'removed', date))

        # ------------------------------------------
        # 6) EXECUTE BULK DB CHANGES (batched)
        # ------------------------------------------
        # deactivate results (set is_active FALSE)
        if to_deactivate_results:
            execute_batch(cursor, """
                UPDATE results
                SET is_active = FALSE, updated_at = %s
                WHERE course_name = %s AND result_date = %s AND is_active = TRUE
            """, to_deactivate_results, page_size=200)

        # insert/activate results (UPSERT)
        if to_insert_results:
            execute_batch(cursor, """
                INSERT INTO results (course_name, result_date, is_active, last_seen)
                VALUES (%s, %s, TRUE, %s)
                ON CONFLICT (course_name, result_date)
                DO UPDATE SET is_active = TRUE, last_seen = EXCLUDED.last_seen
            """, to_insert_results, page_size=200)

        # timeline deactivate
        if to_timeline_deactivate:
            execute_batch(cursor, """
                UPDATE course_result_timeline
                SET is_currently_active = FALSE
                WHERE course_name = %s AND result_date = %s
            """, to_timeline_deactivate, page_size=200)

        # timeline insert / update
        if to_timeline_upsert:
            execute_batch(cursor, """
                INSERT INTO course_result_timeline
                    (course_name, result_date, first_seen, last_seen, times_appeared, is_currently_active)
                VALUES (%s, %s, NOW(), NOW(), 1, TRUE)
                ON CONFLICT (course_name, result_date)
                DO UPDATE SET
                    last_seen = NOW(),
                    times_appeared = course_result_timeline.times_appeared + 1,
                    is_currently_active = TRUE
            """, to_timeline_upsert, page_size=200)

        # insert history events
        if history_events:
            execute_batch(cursor, """
                INSERT INTO results_history
                    (course_name, result_date, change_type, previous_date)
                VALUES (%s, %s, %s, %s)
            """, history_events, page_size=200)

        # update last_seen for all scraped items (batch)
        last_seen_updates = [(now_ts, c, d) for (c, d) in scraped_set]
        if last_seen_updates:
            execute_batch(cursor, """
                UPDATE results
                SET last_seen = %s
                WHERE course_name = %s AND result_date = %s
            """, last_seen_updates, page_size=200)

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
