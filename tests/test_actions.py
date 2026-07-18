from datetime import date
from types import SimpleNamespace

from src import actions
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
    monkeypatch.setattr(actions.parse, "snapshot_hash", lambda _records: "hash")
    monkeypatch.setattr(
        actions.database,
        "sync_database",
        lambda *_args: SimpleNamespace(
            status="success", baseline_created=False, added=1, updated=0, removed=0
        ),
    )
    monkeypatch.setattr(
        actions.discord,
        "drain_outbox",
        lambda *_args: SimpleNamespace(
            delivered=1, failed_attempts=0, dead_lettered=0, remaining=0
        ),
    )

    assert actions.run_workflow(SETTINGS) is True


def test_fetch_failure_returns_false_and_records_failure(monkeypatch):
    monkeypatch.setattr(actions.extract, "fetch_html", lambda _url: (_ for _ in ()).throw(RuntimeError("down")))
    recorded = []
    monkeypatch.setattr(actions, "_record_failure", lambda *_args: recorded.append(True))

    assert actions.run_workflow(SETTINGS) is False
    assert recorded == [True]


def test_discord_failure_fails_workflow(monkeypatch):
    records = [{"course_key": "course", "course_name": "Course", "result_date": date(2026, 7, 18)}]
    monkeypatch.setattr(actions.extract, "fetch_html", lambda _url: "html")
    monkeypatch.setattr(actions.parse, "parse_html_content", lambda _html, _minimum: records)
    monkeypatch.setattr(actions.parse, "snapshot_hash", lambda _records: "hash")
    monkeypatch.setattr(
        actions.database,
        "sync_database",
        lambda *_args: SimpleNamespace(
            status="success", baseline_created=False, added=0, updated=0, removed=0
        ),
    )
    monkeypatch.setattr(
        actions.discord,
        "drain_outbox",
        lambda *_args: SimpleNamespace(
            delivered=0, failed_attempts=1, dead_lettered=0, remaining=1
        ),
    )

    assert actions.run_workflow(SETTINGS) is False
