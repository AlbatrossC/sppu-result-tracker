from datetime import date
from types import SimpleNamespace

from src import actions
from src.discord import DeliverySummary
from src.settings import Settings


SETTINGS = Settings(
    database_url="postgresql://test",
    discord_webhook_url="https://discord.test/webhook",
    minimum_result_count=1,
)


def test_successful_workflow(monkeypatch):
    records = [{"course_key": "course", "course_name": "Course", "result_date": date(2026, 7, 18)}]
    monkeypatch.setattr(actions.extract, "fetch_html", lambda _url: "html")
    monkeypatch.setattr(actions.parse, "parse_html_content", lambda _html, _minimum: records)
    monkeypatch.setattr(
        actions.database,
        "sync_results",
        lambda *_args: SimpleNamespace(status="success", baseline_created=False, added=1, updated=0, removed=0),
    )
    monkeypatch.setattr(
        actions.discord,
        "send_event",
        lambda *_args: SimpleNamespace(sent=True),
    )
    monkeypatch.setattr(
        actions.database,
        "pending_notifications",
        lambda *_args: [SimpleNamespace(history_id=1, result_id=2)],
    )
    monkeypatch.setattr(actions.database, "mark_notification_sent", lambda *_args: None)

    assert actions.run_workflow(SETTINGS) is True


def test_fetch_failure_returns_false(monkeypatch):
    monkeypatch.setattr(actions.extract, "fetch_html", lambda _url: (_ for _ in ()).throw(RuntimeError("down")))

    assert actions.run_workflow(SETTINGS) is False


def test_discord_failure_fails_workflow(monkeypatch):
    records = [{"course_key": "course", "course_name": "Course", "result_date": date(2026, 7, 18)}]
    monkeypatch.setattr(actions.extract, "fetch_html", lambda _url: "html")
    monkeypatch.setattr(actions.parse, "parse_html_content", lambda _html, _minimum: records)
    monkeypatch.setattr(
        actions.database,
        "sync_results",
        lambda *_args: SimpleNamespace(status="success", baseline_created=False, added=0, updated=0, removed=0),
    )
    monkeypatch.setattr(
        actions,
        "_send_pending_notifications",
        lambda _settings: DeliverySummary(delivered=0, failed=1, remaining=1),
    )

    assert actions.run_workflow(SETTINGS) is False
