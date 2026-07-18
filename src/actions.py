import logging
import sys
import traceback
from pathlib import Path


if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src import database, discord, extract, parse
from src.settings import Settings


LOGGER = logging.getLogger("sppu_tracker")
NOTIFICATION_LIMIT = 100


def _configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


def _send_pending_notifications(settings: Settings) -> discord.DeliverySummary:
    delivered = failed = 0
    events = database.pending_notifications(settings.database_url, NOTIFICATION_LIMIT)

    for event in events:
        result = discord.send_event(settings.discord_webhook_url, event)
        if result.sent:
            database.mark_notification_sent(settings.database_url, event.history_id, event.result_id)
            delivered += 1
            continue

        failed += 1
        database.mark_notification_failed(
            settings.database_url,
            event.history_id,
            result.error or "Discord notification failed",
        )

    remaining = len(database.pending_notifications(settings.database_url, NOTIFICATION_LIMIT))
    return discord.DeliverySummary(delivered=delivered, failed=failed, remaining=remaining)


def run_workflow(settings: Settings = None) -> bool:
    _configure_logging()

    try:
        settings = settings or Settings.from_env()
    except Exception as exc:
        LOGGER.error("Configuration error: %s", exc)
        return False

    LOGGER.info("Starting tracker run")
    try:
        html = extract.fetch_html(settings.result_url)
        scraped = parse.parse_html_content(html, settings.minimum_result_count)
        LOGGER.info("Validated %s unique SPPU results", len(scraped))

        outcome = database.sync_results(
            settings.database_url,
            scraped,
            settings.suspicious_count_ratio,
        )
        LOGGER.info(
            "Database sync status=%s baseline=%s added=%s updated=%s removed=%s",
            outcome.status,
            outcome.baseline_created,
            outcome.added,
            outcome.updated,
            outcome.removed,
        )

        delivery = _send_pending_notifications(settings)
        LOGGER.info(
            "Discord delivery: delivered=%s failed=%s remaining=%s",
            delivery.delivered,
            delivery.failed,
            delivery.remaining,
        )
        return delivery.failed == 0
    except Exception as exc:
        LOGGER.error("Tracker run failed: %s", exc)
        LOGGER.debug(traceback.format_exc())
        return False


if __name__ == "__main__":
    raise SystemExit(0 if run_workflow() else 1)
