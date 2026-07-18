"""Read-only database health report for local diagnostics."""

import sys
from pathlib import Path


if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.database import connect
from src.settings import Settings


def print_health() -> None:
    settings = Settings.from_env(require_discord=False)
    conn = connect(settings.database_url)
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM results WHERE is_active = TRUE")
            active = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM pending_changes")
            pending_changes = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM notification_outbox WHERE status = 'pending'")
            pending_notifications = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM notification_outbox WHERE status = 'dead'")
            dead_notifications = cursor.fetchone()[0]
            cursor.execute(
                """
                SELECT status, started_at, finished_at, parsed_count,
                       added_count, updated_count, removed_count, error_message
                FROM tracker_runs ORDER BY started_at DESC LIMIT 1
                """
            )
            latest = cursor.fetchone()

        print(f"Active results: {active}")
        print(f"Pending change confirmations: {pending_changes}")
        print(f"Pending Discord messages: {pending_notifications}")
        print(f"Dead Discord messages: {dead_notifications}")
        print(f"Latest run: {latest or 'none'}")
    finally:
        conn.close()


if __name__ == "__main__":
    print_health()
