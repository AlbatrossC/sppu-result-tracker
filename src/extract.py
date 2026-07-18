import logging
import random
import time
from email.utils import parsedate_to_datetime
from typing import Optional

import requests


LOGGER = logging.getLogger(__name__)
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}


class FetchError(RuntimeError):
    """Raised when the SPPU result page cannot be fetched safely."""


def _retry_after_seconds(response: requests.Response) -> Optional[float]:
    value = response.headers.get("Retry-After")
    if not value:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        try:
            retry_at = parsedate_to_datetime(value)
            return max(0.0, retry_at.timestamp() - time.time())
        except (TypeError, ValueError, OverflowError):
            return None


def fetch_html(
    url: str,
    attempts: int = 4,
    connect_timeout: int = 10,
    read_timeout: int = 30,
    session: Optional[requests.Session] = None,
) -> str:
    """Fetch the SPPU page with bounded retries and TLS verification."""
    client = session or requests.Session()
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 Chrome/126.0 Safari/537.36 "
            "SPPUResultTracker/2.0"
        ),
        "Accept": "text/html,application/xhtml+xml",
    }
    last_error = "unknown error"

    for attempt in range(1, attempts + 1):
        try:
            response = client.get(
                url,
                headers=headers,
                timeout=(connect_timeout, read_timeout),
                verify=True,
            )
            if response.status_code == 200:
                if not response.text.strip():
                    raise FetchError("SPPU returned an empty response body")
                LOGGER.info("Fetched SPPU page on attempt %s (%s bytes)", attempt, len(response.content))
                return response.text

            last_error = f"HTTP {response.status_code}"
            if response.status_code not in RETRYABLE_STATUS_CODES:
                raise FetchError(f"SPPU request failed with {last_error}")

            delay = _retry_after_seconds(response)
        except (requests.Timeout, requests.ConnectionError) as exc:
            last_error = f"{type(exc).__name__}: {exc}"
            delay = None
        except requests.RequestException as exc:
            raise FetchError(f"SPPU request failed: {exc}") from exc

        if attempt == attempts:
            break

        if delay is None:
            delay = min(2 ** (attempt - 1), 8) + random.uniform(0.0, 0.5)
        LOGGER.warning("SPPU fetch attempt %s failed (%s); retrying in %.1fs", attempt, last_error, delay)
        time.sleep(delay)

    raise FetchError(f"SPPU page could not be fetched after {attempts} attempts: {last_error}")
