import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import requests


COLORS = {
    "added": 0x238636,
    "updated": 0xD29922,
    "removed": 0xDA3633,
}


@dataclass(frozen=True)
class SendResult:
    sent: bool
    error: Optional[str] = None


@dataclass(frozen=True)
class DeliverySummary:
    delivered: int
    failed: int
    remaining: int


def _webhook_with_wait(webhook_url: str) -> str:
    parts = urlsplit(webhook_url)
    query = dict(parse_qsl(parts.query, keep_blank_values=True))
    query["wait"] = "true"
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), parts.fragment))


def _format_payload(event) -> dict:
    course_name = str(event.course_name or "")[:256]
    result_date = event.result_date.isoformat() if event.result_date else None
    previous_date = event.previous_date.isoformat() if event.previous_date else None

    if event.event_type == "added":
        title = "Result added"
        description = f"**{course_name}**\nResult date: `{result_date}`"
    elif event.event_type == "updated":
        title = "Result updated"
        description = (
            f"**{course_name}**\n"
            f"Previous date: `{previous_date}`\n"
            f"New date: `{result_date}`"
        )
    else:
        title = "Result removed"
        description = f"**{course_name}**\nPrevious date: `{previous_date}`"

    return {
        "username": "SPPU Result Tracker",
        "allowed_mentions": {"parse": []},
        "embeds": [
            {
                "title": title,
                "description": description,
                "color": COLORS.get(event.event_type, 0x57606A),
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "footer": {"text": f"History {event.history_id}"},
            }
        ],
    }


def send_event(webhook_url: str, event, session: Optional[requests.Session] = None) -> SendResult:
    client = session or requests.Session()
    endpoint = _webhook_with_wait(webhook_url)

    try:
        response = client.post(endpoint, json=_format_payload(event), timeout=(10, 15))
        if response.status_code == 429:
            try:
                retry_after = float(response.json().get("retry_after", 1.0))
            except (ValueError, TypeError, requests.JSONDecodeError):
                retry_after = 1.0
            if retry_after <= 10:
                time.sleep(max(0.1, retry_after))
                response = client.post(endpoint, json=_format_payload(event), timeout=(10, 15))

        if response.status_code in (200, 204):
            return SendResult(sent=True)
        return SendResult(sent=False, error=f"Discord HTTP {response.status_code}: {response.text[:500]}")
    except requests.RequestException as exc:
        return SendResult(sent=False, error=f"{type(exc).__name__}: {exc}")
