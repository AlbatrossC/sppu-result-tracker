import logging
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, Optional
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import requests
from psycopg2.extras import RealDictCursor

from src.database import connect


LOGGER = logging.getLogger(__name__)
COLORS = {
    "added": 0x238636,
    "updated": 0xD29922,
    "removed": 0xDA3633,
    "failure": 0xB42318,
    "recovery": 0x1A7F37,
}


@dataclass(frozen=True)
class DeliverySummary:
    delivered: int
    failed_attempts: int
    dead_lettered: int
    remaining: int


def _webhook_with_wait(webhook_url: str) -> str:
    parts = urlsplit(webhook_url)
    query = dict(parse_qsl(parts.query, keep_blank_values=True))
    query["wait"] = "true"
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), parts.fragment))


def _format_payload(event_type: str, payload: Dict[str, object]) -> dict:
    event_id = str(payload.get("event_id") or "unknown")[:12]
    course_name = str(payload.get("course_name") or "")[:256]
    result_date = payload.get("result_date")
    previous_date = payload.get("previous_date")

    if event_type == "added":
        title = "Result added"
        description = f"**{course_name}**\nResult date: `{result_date}`"
    elif event_type == "updated":
        title = "Result updated"
        description = (
            f"**{course_name}**\n"
            f"Previous date: `{previous_date}`\n"
            f"New date: `{result_date}`"
        )
    elif event_type == "removed":
        title = "Result removed"
        description = f"**{course_name}**\nPrevious date: `{previous_date}`"
    elif event_type == "recovery":
        title = "Tracker recovered"
        description = str(payload.get("message") or "The tracker is working again.")[:4000]
    else:
        title = "Tracker failure"
        description = str(payload.get("message") or "The tracker is currently failing.")[:4000]

    return {
        "username": "SPPU Result Tracker",
        "allowed_mentions": {"parse": []},
        "embeds": [
            {
                "title": title,
                "description": description,
                "color": COLORS.get(event_type, 0x57606A),
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "footer": {"text": f"Event {event_id}"},
            }
        ],
    }


def _post_message(webhook_url: str, body: dict, session: requests.Session) -> requests.Response:
    response = session.post(webhook_url, json=body, timeout=(10, 15))
    if response.status_code == 429:
        try:
            retry_after = float(response.json().get("retry_after", 1.0))
        except (ValueError, TypeError, requests.JSONDecodeError):
            retry_after = 1.0
        if retry_after <= 10:
            time.sleep(max(0.1, retry_after))
            response = session.post(webhook_url, json=body, timeout=(10, 15))
    return response


def drain_outbox(
    database_url: str,
    webhook_url: str,
    limit: int = 100,
    session: Optional[requests.Session] = None,
) -> DeliverySummary:
    """Deliver due outbox rows oldest-first and persist each attempt."""
    client = session or requests.Session()
    endpoint = _webhook_with_wait(webhook_url)
    delivered = failed = dead = 0
    conn = connect(database_url)

    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                """
                SELECT id, event_type, payload, attempts
                FROM notification_outbox
                WHERE status = 'pending' AND next_attempt_at <= NOW()
                ORDER BY created_at, id
                LIMIT %s
                """,
                (limit,),
            )
            messages = cursor.fetchall()

        for message in messages:
            attempts = int(message["attempts"] or 0) + 1
            response = None
            error = None
            try:
                response = _post_message(
                    endpoint,
                    _format_payload(message["event_type"], message["payload"]),
                    client,
                )
                if response.status_code in (200, 204):
                    message_id = None
                    if response.status_code == 200:
                        try:
                            message_id = response.json().get("id")
                        except requests.JSONDecodeError:
                            pass
                    with conn:
                        with conn.cursor() as cursor:
                            cursor.execute(
                                """
                                UPDATE notification_outbox
                                SET status = 'sent', attempts = %s, sent_at = NOW(),
                                    discord_message_id = %s, last_error = NULL
                                WHERE id = %s
                                """,
                                (attempts, message_id, message["id"]),
                            )
                    delivered += 1
                    continue
                error = f"Discord HTTP {response.status_code}: {response.text[:500]}"
            except requests.RequestException as exc:
                error = f"{type(exc).__name__}: {exc}"

            permanent = bool(response is not None and 400 <= response.status_code < 500 and response.status_code != 429)
            is_dead = permanent or attempts >= 12
            delay_seconds = min(60 * (2 ** min(attempts - 1, 6)), 3600)
            next_attempt = datetime.now(timezone.utc) + timedelta(seconds=delay_seconds)
            with conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        UPDATE notification_outbox
                        SET status = %s, attempts = %s, next_attempt_at = %s, last_error = %s
                        WHERE id = %s
                        """,
                        ("dead" if is_dead else "pending", attempts, next_attempt, error, message["id"]),
                    )
            failed += 1
            if is_dead:
                dead += 1
            LOGGER.error("Discord delivery failed for event %s: %s", message["id"], error)

        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM notification_outbox WHERE status = 'pending'")
            remaining = cursor.fetchone()[0]
        return DeliverySummary(delivered, failed, dead, remaining)
    finally:
        conn.close()
