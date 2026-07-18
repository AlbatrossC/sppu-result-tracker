import hashlib
import logging
import time
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Dict, Iterable, List, Optional, Set, Tuple

import psycopg2
from psycopg2.extras import Json, RealDictCursor, execute_batch, execute_values, register_uuid


LOGGER = logging.getLogger(__name__)
ResultPair = Tuple[str, date]

register_uuid()


@dataclass(frozen=True)
class ChangeCandidate:
    change_type: str
    course_key: str
    course_name: str
    old_date: Optional[date]
    new_date: Optional[date]

    @property
    def candidate_key(self) -> str:
        value = "|".join(
            (
                self.change_type,
                self.course_key,
                self.old_date.isoformat() if self.old_date else "",
                self.new_date.isoformat() if self.new_date else "",
            )
        )
        return hashlib.sha256(value.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class ChangeSet:
    additions: Set[ResultPair]
    destructive: Tuple[ChangeCandidate, ...]


@dataclass(frozen=True)
class SyncOutcome:
    run_id: uuid.UUID
    status: str
    baseline_created: bool = False
    added: int = 0
    updated: int = 0
    removed: int = 0


def classify_changes(
    active_pairs: Set[ResultPair],
    scraped_pairs: Set[ResultPair],
    display_names: Dict[str, str],
) -> ChangeSet:
    """Classify exact additions and potentially destructive changes."""
    active_by_course: Dict[str, Set[date]] = {}
    scraped_by_course: Dict[str, Set[date]] = {}
    for key, result_date in active_pairs:
        active_by_course.setdefault(key, set()).add(result_date)
    for key, result_date in scraped_pairs:
        scraped_by_course.setdefault(key, set()).add(result_date)

    additions: Set[ResultPair] = set()
    destructive: List[ChangeCandidate] = []

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


def _ensure_state(cursor) -> dict:
    cursor.execute(
        """
        INSERT INTO tracker_state (id) VALUES (TRUE)
        ON CONFLICT (id) DO NOTHING
        """
    )
    cursor.execute("SELECT * FROM tracker_state WHERE id = TRUE FOR UPDATE")
    return dict(cursor.fetchone())


def _queue_event(
    cursor,
    run_id: uuid.UUID,
    event_type: str,
    course_name: Optional[str] = None,
    result_date: Optional[date] = None,
    previous_date: Optional[date] = None,
    message: Optional[str] = None,
) -> None:
    event_id = uuid.uuid4()
    payload = {
        "event_id": str(event_id),
        "course_name": course_name,
        "result_date": result_date.isoformat() if result_date else None,
        "previous_date": previous_date.isoformat() if previous_date else None,
        "message": message,
    }
    cursor.execute(
        """
        INSERT INTO notification_outbox (id, run_id, event_type, payload)
        VALUES (%s, %s, %s, %s)
        """,
        (event_id, run_id, event_type, Json(payload)),
    )


def _record_result_event(
    cursor,
    run_id: uuid.UUID,
    event_type: str,
    course_key: str,
    course_name: str,
    result_date: Optional[date],
    previous_date: Optional[date],
) -> None:
    cursor.execute(
        """
        INSERT INTO results_history
            (run_id, course_key, course_name, result_date, change_type, previous_date)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (run_id, course_key, course_name, result_date, event_type, previous_date),
    )
    _queue_event(
        cursor,
        run_id,
        event_type,
        course_name=course_name,
        result_date=result_date,
        previous_date=previous_date,
    )


def _apply_failure_state(cursor, state: dict, run_id: uuid.UUID, error: str) -> None:
    failures = int(state.get("consecutive_failures") or 0) + 1
    outage_alerted = bool(state.get("outage_alerted"))
    if failures >= 3 and not outage_alerted:
        _queue_event(
            cursor,
            run_id,
            "failure",
            message=f"The SPPU tracker has failed {failures} consecutive runs. Latest error: {error[:500]}",
        )
        outage_alerted = True

    cursor.execute(
        """
        UPDATE tracker_state
        SET consecutive_failures = %s,
            outage_alerted = %s,
            updated_at = NOW()
        WHERE id = TRUE
        """,
        (failures, outage_alerted),
    )


def record_failed_run(
    database_url: str,
    run_id: uuid.UUID,
    started_at: datetime,
    error: str,
) -> None:
    """Record a failed fetch/parse/sync and queue an alert after three failures."""
    conn = connect(database_url)
    try:
        with conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                state = _ensure_state(cursor)
                cursor.execute(
                    """
                    INSERT INTO tracker_runs
                        (run_id, started_at, finished_at, status, error_message)
                    VALUES (%s, %s, NOW(), 'failed', %s)
                    ON CONFLICT (run_id) DO UPDATE SET
                        finished_at = EXCLUDED.finished_at,
                        status = EXCLUDED.status,
                        error_message = EXCLUDED.error_message
                    """,
                    (run_id, started_at, error[:2000]),
                )
                _apply_failure_state(cursor, state, run_id, error)
    finally:
        conn.close()


def _upsert_results(cursor, rows: Iterable[Tuple[str, str, date, datetime]]) -> None:
    values = list(rows)
    if not values:
        return
    execute_values(
        cursor,
        """
        INSERT INTO results (course_key, course_name, result_date, is_active, last_seen)
        VALUES %s
        ON CONFLICT (course_key, result_date) DO UPDATE SET
            course_name = EXCLUDED.course_name,
            is_active = TRUE,
            last_seen = EXCLUDED.last_seen,
            updated_at = NOW()
        """,
        values,
        template="(%s, %s, %s, TRUE, %s)",
        page_size=250,
    )


def _sync_database_once(
    database_url: str,
    scraped: List[Dict[str, object]],
    current_snapshot_hash: str,
    run_id: uuid.UUID,
    started_at: datetime,
    suspicious_count_ratio: float = 0.70,
) -> SyncOutcome:
    if not scraped:
        raise ValueError("Cannot synchronize an empty result list")

    now = datetime.now(timezone.utc)
    conn = connect(database_url)
    try:
        with conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("SELECT pg_try_advisory_xact_lock(hashtext('sppu-result-tracker')) AS locked")
                if not cursor.fetchone()["locked"]:
                    cursor.execute(
                        """
                        INSERT INTO tracker_runs (run_id, started_at, finished_at, status, parsed_count)
                        VALUES (%s, %s, NOW(), 'skipped_locked', %s)
                        """,
                        (run_id, started_at, len(scraped)),
                    )
                    return SyncOutcome(run_id=run_id, status="skipped_locked")

                state = _ensure_state(cursor)
                cursor.execute(
                    """
                    INSERT INTO tracker_runs
                        (run_id, started_at, status, parsed_count, snapshot_hash)
                    VALUES (%s, %s, 'running', %s, %s)
                    """,
                    (run_id, started_at, len(scraped), current_snapshot_hash),
                )

                scraped_by_pair = {
                    (str(item["course_key"]), item["result_date"]): str(item["course_name"])
                    for item in scraped
                }
                scraped_pairs = set(scraped_by_pair)

                if not state["initialized"]:
                    _upsert_results(
                        cursor,
                        ((key, name, result_date, now) for (key, result_date), name in scraped_by_pair.items()),
                    )
                    cursor.execute(
                        """
                        UPDATE tracker_state
                        SET initialized = TRUE,
                            last_successful_run_id = %s,
                            last_snapshot_count = %s,
                            last_snapshot_hash = %s,
                            consecutive_failures = 0,
                            pending_snapshot_hash = NULL,
                            pending_snapshot_count = NULL,
                            updated_at = NOW()
                        WHERE id = TRUE
                        """,
                        (run_id, len(scraped), current_snapshot_hash),
                    )
                    cursor.execute(
                        """
                        UPDATE tracker_runs
                        SET finished_at = NOW(), status = 'success', baseline_created = TRUE
                        WHERE run_id = %s
                        """,
                        (run_id,),
                    )
                    return SyncOutcome(run_id=run_id, status="success", baseline_created=True)

                previous_count = int(state.get("last_snapshot_count") or 0)
                suspicious = previous_count > 0 and len(scraped) < previous_count * suspicious_count_ratio
                if suspicious and state.get("pending_snapshot_hash") != current_snapshot_hash:
                    error = f"Suspicious result count: {len(scraped)} instead of approximately {previous_count}"
                    cursor.execute(
                        """
                        UPDATE tracker_state
                        SET pending_snapshot_hash = %s,
                            pending_snapshot_count = %s,
                            updated_at = NOW()
                        WHERE id = TRUE
                        """,
                        (current_snapshot_hash, len(scraped)),
                    )
                    _apply_failure_state(cursor, state, run_id, error)
                    cursor.execute(
                        """
                        UPDATE tracker_runs
                        SET finished_at = NOW(), status = 'suspect', error_message = %s
                        WHERE run_id = %s
                        """,
                        (error, run_id),
                    )
                    return SyncOutcome(run_id=run_id, status="suspect")

                cursor.execute(
                    """
                    SELECT course_key, course_name, result_date
                    FROM results
                    WHERE is_active = TRUE
                    """
                )
                active_rows = cursor.fetchall()
                active_pairs = {(row["course_key"], row["result_date"]) for row in active_rows}
                display_names = {row["course_key"]: row["course_name"] for row in active_rows}
                display_names.update({key: name for (key, _), name in scraped_by_pair.items()})
                changes = classify_changes(active_pairs, scraped_pairs, display_names)

                added = updated = removed = 0
                addition_rows = []
                for key, result_date in sorted(changes.additions):
                    name = scraped_by_pair[(key, result_date)]
                    addition_rows.append((key, name, result_date, now))
                    _record_result_event(cursor, run_id, "added", key, name, result_date, None)
                    added += 1
                _upsert_results(cursor, addition_rows)

                previous_success = state.get("last_successful_run_id")
                current_candidates = {candidate.candidate_key: candidate for candidate in changes.destructive}
                if current_candidates:
                    cursor.execute(
                        "DELETE FROM pending_changes WHERE NOT (candidate_key = ANY(%s))",
                        (list(current_candidates),),
                    )
                else:
                    cursor.execute("DELETE FROM pending_changes")

                for candidate_key, candidate in current_candidates.items():
                    cursor.execute(
                        "SELECT observations, last_seen_run_id FROM pending_changes WHERE candidate_key = %s",
                        (candidate_key,),
                    )
                    existing = cursor.fetchone()
                    consecutive = bool(
                        existing
                        and previous_success
                        and existing["last_seen_run_id"] == previous_success
                    )
                    observations = int(existing["observations"]) + 1 if consecutive else 1
                    cursor.execute(
                        """
                        INSERT INTO pending_changes
                            (candidate_key, change_type, course_key, course_name, old_date, new_date,
                             first_seen_run_id, last_seen_run_id, observations)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (candidate_key) DO UPDATE SET
                            course_name = EXCLUDED.course_name,
                            last_seen_run_id = EXCLUDED.last_seen_run_id,
                            observations = EXCLUDED.observations,
                            updated_at = NOW()
                        """,
                        (
                            candidate_key,
                            candidate.change_type,
                            candidate.course_key,
                            candidate.course_name,
                            candidate.old_date,
                            candidate.new_date,
                            run_id,
                            run_id,
                            observations,
                        ),
                    )
                    if observations < 2:
                        continue

                    if candidate.change_type == "updated":
                        cursor.execute(
                            """
                            UPDATE results SET is_active = FALSE, updated_at = NOW()
                            WHERE course_key = %s AND result_date = %s AND is_active = TRUE
                            """,
                            (candidate.course_key, candidate.old_date),
                        )
                        _upsert_results(
                            cursor,
                            [
                                (
                                    candidate.course_key,
                                    candidate.course_name,
                                    candidate.new_date,
                                    now,
                                )
                            ],
                        )
                        _record_result_event(
                            cursor,
                            run_id,
                            "updated",
                            candidate.course_key,
                            candidate.course_name,
                            candidate.new_date,
                            candidate.old_date,
                        )
                        updated += 1
                    else:
                        cursor.execute(
                            """
                            UPDATE results SET is_active = FALSE, updated_at = NOW()
                            WHERE course_key = %s AND result_date = %s AND is_active = TRUE
                            """,
                            (candidate.course_key, candidate.old_date),
                        )
                        _record_result_event(
                            cursor,
                            run_id,
                            "removed",
                            candidate.course_key,
                            candidate.course_name,
                            None,
                            candidate.old_date,
                        )
                        removed += 1
                    cursor.execute("DELETE FROM pending_changes WHERE candidate_key = %s", (candidate_key,))

                exact_seen = [
                    (now, scraped_by_pair[(key, result_date)], key, result_date)
                    for key, result_date in scraped_pairs & active_pairs
                ]
                if exact_seen:
                    execute_batch(
                        cursor,
                        """
                        UPDATE results
                        SET last_seen = %s, course_name = %s
                        WHERE course_key = %s AND result_date = %s AND is_active = TRUE
                        """,
                        exact_seen,
                        page_size=250,
                    )

                if state.get("outage_alerted"):
                    _queue_event(
                        cursor,
                        run_id,
                        "recovery",
                        message="The SPPU result tracker is working again.",
                    )

                cursor.execute(
                    """
                    UPDATE tracker_state
                    SET last_successful_run_id = %s,
                        last_snapshot_count = %s,
                        last_snapshot_hash = %s,
                        pending_snapshot_hash = NULL,
                        pending_snapshot_count = NULL,
                        consecutive_failures = 0,
                        outage_alerted = FALSE,
                        updated_at = NOW()
                    WHERE id = TRUE
                    """,
                    (run_id, len(scraped), current_snapshot_hash),
                )
                cursor.execute(
                    """
                    UPDATE tracker_runs
                    SET finished_at = NOW(), status = 'success',
                        added_count = %s, updated_count = %s, removed_count = %s
                    WHERE run_id = %s
                    """,
                    (added, updated, removed, run_id),
                )

                return SyncOutcome(
                    run_id=run_id,
                    status="success",
                    added=added,
                    updated=updated,
                    removed=removed,
                )
    finally:
        conn.close()


def sync_database(
    database_url: str,
    scraped: List[Dict[str, object]],
    current_snapshot_hash: str,
    run_id: uuid.UUID,
    started_at: datetime,
    suspicious_count_ratio: float = 0.70,
    attempts: int = 3,
) -> SyncOutcome:
    """Run the sync transaction, retrying only safe PostgreSQL rollbacks."""
    for attempt in range(1, attempts + 1):
        try:
            return _sync_database_once(
                database_url,
                scraped,
                current_snapshot_hash,
                run_id,
                started_at,
                suspicious_count_ratio,
            )
        except psycopg2.extensions.TransactionRollbackError:
            if attempt == attempts:
                raise
            delay = 2 ** (attempt - 1)
            LOGGER.warning("Database transaction was rolled back; retrying in %ss", delay)
            time.sleep(delay)

    raise RuntimeError("Database synchronization attempts were exhausted")
