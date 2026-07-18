import logging
import sys
import traceback
import uuid
from datetime import datetime, timezone
from pathlib import Path


if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src import database, discord, extract, parse
from src.settings import Settings


LOGGER = logging.getLogger("sppu_tracker")


def _configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


def _record_failure(settings: Settings, run_id: uuid.UUID, started_at: datetime, error: str) -> None:
    try:
        database.record_failed_run(settings.database_url, run_id, started_at, error)
        delivery = discord.drain_outbox(
            settings.database_url,
            settings.discord_webhook_url,
            settings.max_notifications_per_run,
        )
        LOGGER.info(
            "Failure outbox delivery: delivered=%s remaining=%s",
            delivery.delivered,
            delivery.remaining,
        )
    except Exception:
        LOGGER.exception("Could not record or report the failed tracker run")


def run_workflow(settings: Settings = None) -> bool:
    _configure_logging()
    started_at = datetime.now(timezone.utc)
    run_id = uuid.uuid4()

    try:
        settings = settings or Settings.from_env()
    except Exception as exc:
        LOGGER.error("Configuration error: %s", exc)
        return False

    LOGGER.info("Starting tracker run %s", run_id)
    try:
        html = extract.fetch_html(settings.result_url)
        scraped = parse.parse_html_content(html, settings.minimum_result_count)
        digest = parse.snapshot_hash(scraped)
        LOGGER.info("Validated %s unique SPPU results", len(scraped))

        outcome = database.sync_database(
            settings.database_url,
            scraped,
            digest,
            run_id,
            started_at,
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
        if outcome.status == "skipped_locked":
            return True
        if outcome.status != "success":
            delivery = discord.drain_outbox(
                settings.database_url,
                settings.discord_webhook_url,
                settings.max_notifications_per_run,
            )
            LOGGER.warning("Run ended as %s; delivered %s health messages", outcome.status, delivery.delivered)
            return False

        delivery = discord.drain_outbox(
            settings.database_url,
            settings.discord_webhook_url,
            settings.max_notifications_per_run,
        )
        LOGGER.info(
            "Discord delivery: delivered=%s failed=%s dead=%s remaining=%s",
            delivery.delivered,
            delivery.failed_attempts,
            delivery.dead_lettered,
            delivery.remaining,
        )
        return delivery.failed_attempts == 0
    except Exception as exc:
        LOGGER.error("Tracker run %s failed: %s", run_id, exc)
        LOGGER.debug(traceback.format_exc())
        _record_failure(settings, run_id, started_at, str(exc))
        return False


if __name__ == "__main__":
    raise SystemExit(0 if run_workflow() else 1)
