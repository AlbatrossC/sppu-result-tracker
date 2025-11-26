# Debugging engine for timeline + history.
# NEVER modifies the results table.

import os
import psycopg2
from psycopg2.extras import execute_batch
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

def debug_sync():
    print("\n================ DEBUG SYNC ================\n")

    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()
    now_ts = datetime.now()

    try:
        # Load current truth from "results"
        cursor.execute("""
            SELECT course_name, result_date::text, is_active
            FROM results
        """)
        rows = cursor.fetchall()

        # Build maps
        active_map = {}
        inactive_map = {}

        for name, date, is_active in rows:
            if is_active:
                active_map.setdefault(name, set()).add(date)
            else:
                inactive_map.setdefault(name, set()).add(date)

        # Load current timeline
        cursor.execute("""
            SELECT course_name, result_date::text, is_currently_active
            FROM course_result_timeline
        """)
        timeline_rows = cursor.fetchall()

        timeline_active = {}
        timeline_inactive = {}

        for name, date, active in timeline_rows:
            if active:
                timeline_active.setdefault(name, set()).add(date)
            else:
                timeline_inactive.setdefault(name, set()).add(date)

        # Compute differences
        events = []    # for DB
        outputs = []   # for logging

        # ----- Detect updates (same course_name but different date) -----
        updates = []

        for course in active_map:
            current_dates = active_map.get(course, set())
            old_dates = timeline_active.get(course, set())

            # update = originally had 1 active date, now has a different one
            if len(current_dates) == 1 and len(old_dates) == 1:
                new_date = list(current_dates)[0]
                old_date = list(old_dates)[0]

                if new_date != old_date:
                    updates.append((course, old_date, new_date))

        # ----- Apply update events -----
        for course, old_date, new_date in updates:
            outputs.append(f"[UPDATED] {course}: {old_date} → {new_date}")

            # 1. deactivate old date
            cursor.execute("""
                UPDATE course_result_timeline
                SET is_currently_active = FALSE
                WHERE course_name=%s AND result_date=%s
            """, (course, old_date))

            # 2. activate/insert new date
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

            # history entry
            events.append((course, new_date, 'updated', old_date))

            # remove these from further add/remove checks
            active_map[course].discard(new_date)
            timeline_active[course].discard(old_date)

        # ----- Detect NEW additions -----
        for course, dates in active_map.items():
            for date in dates:
                if date not in timeline_active.get(course, set()):
                    outputs.append(f"[ADDED] {course} → {date}")

                    cursor.execute("""
                        INSERT INTO course_result_timeline
                            (course_name, result_date, first_seen, last_seen, times_appeared, is_currently_active)
                        VALUES (%s, %s, NOW(), NOW(), 1, TRUE)
                        ON CONFLICT (course_name, result_date)
                        DO UPDATE SET
                            last_seen = NOW(),
                            times_appeared = course_result_timeline.times_appeared + 1,
                            is_currently_active = TRUE
                    """, (course, date))

                    events.append((course, date, 'added', None))

        # ----- Detect REMOVALS -----
        for course, dates in timeline_active.items():
            for date in dates:
                if date not in active_map.get(course, set()):
                    outputs.append(f"[REMOVED] {course} → {date}")

                    cursor.execute("""
                        UPDATE course_result_timeline
                        SET is_currently_active = FALSE
                        WHERE course_name=%s AND result_date=%s
                    """, (course, date))

                    events.append((course, None, 'removed', date))

        # Insert history events
        if events:
            execute_batch(cursor, """
                INSERT INTO results_history (course_name, result_date, change_type, previous_date)
                VALUES (%s, %s, %s, %s)
            """, events, page_size=200)

        conn.commit()

        # Print debug outputs
        if outputs:
            print("\n".join(outputs))
        else:
            print("No changes detected.")

        print("\n================ DEBUG END ================\n")

    except Exception as e:
        conn.rollback()
        print("[DBG ERROR]:", e)

    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    debug_sync()
