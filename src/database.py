import logging
import time
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Dict, List, Optional, Set, Tuple

import psycopg2
from psycopg2.extras import RealDictCursor, execute_batch, execute_values


LOGGER = logging.getLogger(__name__)
ResultPair = Tuple[str, date]


@dataclass(frozen=True)
class ChangeCandidate:
    change_type: str
    course_key: str
    course_name: str
    old_date: Optional[date]
    new_date: Optional[date]


@dataclass(frozen=True)
class ChangeSet:
    additions: Set[ResultPair]
    destructive: Tuple[ChangeCandidate, ...]


@dataclass(frozen=True)
class SyncOutcome:
    status: str
    baseline_created: bool = False
    added: int = 0
    updated: int = 0
    removed: int = 0


@dataclass(frozen=True)
class NotificationEvent:
    history_id: int
    result_id: Optional[int]
    event_type: str
    course_name: str
    result_date: Optional[date]
    previous_date: Optional[date]


def connect(database_url: str, attempts: int = 3):
    last_error = None
    for attempt in range(1, attempts + 1):
        try:
            return psycopg2.connect(
                database_url,
                connect_timeout=10,
                application_name="sppu-result-tracker",
            )
        except psycopg2.OperationalError as exc:
            last_error = exc
            if attempt == attempts:
                break
            delay = 2 ** (attempt - 1)
            LOGGER.warning("Database connection attempt %s failed; retrying in %ss", attempt, delay)
            time.sleep(delay)
    raise last_error


def classify_changes(
    active_pairs: Set[ResultPair],
    scraped_pairs: Set[ResultPair],
    display_names: Dict[str, str],
) -> ChangeSet:
    active_by_course: Dict[str, Set[date]] = {}
    scraped_by_course: Dict[str, Set[date]] = {}
    for key, result_date in active_pairs:
        active_by_course.setdefault(key, set()).add(result_date)
    for key, result_date in scraped_pairs:
        scraped_by_course.setdefault(key, set()).add(result_date)

    additions: Set[ResultPair] = set()
    destructive = []

    for key in active_by_course.keys() | scraped_by_course.keys():
        old_dates = active_by_course.get(key, set())
        new_dates = scraped_by_course.get(key, set())
        name = display_names.get(key, key)

        if len(old_dates) == 1 and len(new_dates) == 1 and old_dates != new_dates:
            destructive.append(
                ChangeCandidate(
                    change_type="updated",
                    course_key=key,
                    course_name=name,
                    old_date=next(iter(old_dates)),
                    new_date=next(iter(new_dates)),
                )
            )
            continue

        additions.update((key, value) for value in new_dates - old_dates)
        destructive.extend(
            ChangeCandidate(
                change_type="removed",
                course_key=key,
                course_name=name,
                old_date=value,
                new_date=None,
            )
            for value in old_dates - new_dates
        )

    return ChangeSet(additions=additions, destructive=tuple(destructive))


def _insert_baseline(cursor, scraped_by_pair: Dict[ResultPair, str], seen_at: datetime) -> None:
    execute_values(
        cursor,
        """
        INSERT INTO results
            (course_key, course_name, result_date, notification_sent, first_seen, last_seen)
        VALUES %s
        ON CONFLICT (course_key, result_date) DO UPDATE SET
            course_name = EXCLUDED.course_name,
            notification_sent = TRUE,
            last_seen = EXCLUDED.last_seen,
            updated_at = NOW()
        """,
        [
            (key, name, result_date, True, seen_at, seen_at)
            for (key, result_date), name in scraped_by_pair.items()
        ],
        page_size=250,
    )


def _upsert_added_result(cursor, key: str, name: str, result_date: date, seen_at: datetime) -> int:
    cursor.execute(
        """
        INSERT INTO results
            (course_key, course_name, result_date, notification_sent, first_seen, last_seen)
        VALUES (%s, %s, %s, FALSE, %s, %s)
        ON CONFLICT (course_key, result_date) DO UPDATE SET
            course_name = EXCLUDED.course_name,
            notification_sent = FALSE,
            last_seen = EXCLUDED.last_seen,
            updated_at = NOW()
        RETURNING id
        """,
        (key, name, result_date, seen_at, seen_at),
    )
    return int(cursor.fetchone()["id"])


def _record_history(
    cursor,
    event_type: str,
    result_id: Optional[int],
    course_key: str,
    course_name: str,
    old_date: Optional[date],
    new_date: Optional[date],
) -> int:
    cursor.execute(
        """
        INSERT INTO results_history
            (result_id, course_key, course_name, change_type, old_result_date, new_result_date)
        VALUES (%s, %s, %s, %s, %s, %s)
        RETURNING id
        """,
        (result_id, course_key, course_name, event_type, old_date, new_date),
    )
    return int(cursor.fetchone()["id"])


def sync_results(
    database_url: str,
    scraped: List[Dict[str, object]],
    suspicious_count_ratio: float = 0.70,
) -> SyncOutcome:
    if not scraped:
        raise ValueError("Cannot synchronize an empty result list")

    seen_at = datetime.now(timezone.utc)
    scraped_by_pair = {
        (str(item["course_key"]), item["result_date"]): str(item["course_name"])
        for item in scraped
    }
    scraped_pairs = set(scraped_by_pair)
    conn = connect(database_url)

    try:
        with conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("SELECT pg_advisory_xact_lock(hashtext('sppu-result-tracker'))")
                cursor.execute(
                    """
                    SELECT id, course_key, course_name, result_date
                    FROM results
                    """
                )
                active_rows = cursor.fetchall()

                if not active_rows:
                    _insert_baseline(cursor, scraped_by_pair, seen_at)
                    return SyncOutcome(status="success", baseline_created=True)

                if len(scraped) < len(active_rows) * suspicious_count_ratio:
                    raise RuntimeError(
                        f"Suspicious result count: {len(scraped)} instead of approximately {len(active_rows)}"
                    )

                active_pairs = {(row["course_key"], row["result_date"]) for row in active_rows}
                result_ids = {
                    (row["course_key"], row["result_date"]): int(row["id"])
                    for row in active_rows
                }
                display_names = {row["course_key"]: row["course_name"] for row in active_rows}
                display_names.update({key: name for (key, _), name in scraped_by_pair.items()})
                changes = classify_changes(active_pairs, scraped_pairs, display_names)

                added = updated = removed = 0
                for key, result_date in sorted(changes.additions):
                    name = scraped_by_pair[(key, result_date)]
                    result_id = _upsert_added_result(cursor, key, name, result_date, seen_at)
                    _record_history(cursor, "added", result_id, key, name, None, result_date)
                    added += 1

                for candidate in changes.destructive:
                    if candidate.change_type == "updated":
                        cursor.execute(
                            """
                            UPDATE results
                            SET course_name = %s,
                                result_date = %s,
                                notification_sent = FALSE,
                                first_seen = %s,
                                last_seen = %s,
                                updated_at = NOW()
                            WHERE course_key = %s AND result_date = %s
                            RETURNING id
                            """,
                            (
                                candidate.course_name,
                                candidate.new_date,
                                seen_at,
                                seen_at,
                                candidate.course_key,
                                candidate.old_date,
                            ),
                        )
                        row = cursor.fetchone()
                        if row:
                            _record_history(
                                cursor,
                                "updated",
                                int(row["id"]),
                                candidate.course_key,
                                candidate.course_name,
                                candidate.old_date,
                                candidate.new_date,
                            )
                            updated += 1
                        continue

                    result_id = result_ids.get((candidate.course_key, candidate.old_date))
                    cursor.execute(
                        """
                        DELETE FROM results
                        WHERE course_key = %s AND result_date = %s
                        """,
                        (candidate.course_key, candidate.old_date),
                    )
                    if cursor.rowcount:
                        _record_history(
                            cursor,
                            "removed",
                            result_id,
                            candidate.course_key,
                            candidate.course_name,
                            candidate.old_date,
                            None,
                        )
                        removed += 1

                exact_seen = [
                    (seen_at, scraped_by_pair[(key, result_date)], key, result_date)
                    for key, result_date in scraped_pairs & active_pairs
                ]
                if exact_seen:
                    execute_batch(
                        cursor,
                        """
                        UPDATE results
                        SET last_seen = %s, course_name = %s
                        WHERE course_key = %s AND result_date = %s
                        """,
                        exact_seen,
                        page_size=250,
                    )

                return SyncOutcome(
                    status="success",
                    added=added,
                    updated=updated,
                    removed=removed,
                )
    finally:
        conn.close()


def pending_notifications(database_url: str, limit: int = 100) -> List[NotificationEvent]:
    conn = connect(database_url)
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                """
                SELECT id, result_id, change_type, course_name, old_result_date, new_result_date
                FROM results_history
                WHERE notification_sent = FALSE
                ORDER BY created_at, id
                LIMIT %s
                """,
                (limit,),
            )
            rows = cursor.fetchall()
        return [
            NotificationEvent(
                history_id=int(row["id"]),
                result_id=int(row["result_id"]) if row["result_id"] is not None else None,
                event_type=row["change_type"],
                course_name=row["course_name"],
                result_date=row["new_result_date"],
                previous_date=row["old_result_date"],
            )
            for row in rows
        ]
    finally:
        conn.close()


def mark_notification_sent(database_url: str, history_id: int, result_id: Optional[int]) -> None:
    conn = connect(database_url)
    try:
        with conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE results_history
                    SET notification_sent = TRUE,
                        notification_error = NULL
                    WHERE id = %s
                    """,
                    (history_id,),
                )
                if result_id is not None:
                    cursor.execute(
                        """
                        UPDATE results
                        SET notification_sent = TRUE
                        WHERE id = %s
                        """,
                        (result_id,),
                    )
    finally:
        conn.close()


def mark_notification_failed(database_url: str, history_id: int, error: str) -> None:
    conn = connect(database_url)
    try:
        with conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE results_history
                    SET notification_error = %s
                    WHERE id = %s
                    """,
                    (error[:1000], history_id),
                )
    finally:
        conn.close()
