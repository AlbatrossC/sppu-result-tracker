import pytest

from src.settings import Settings


def test_database_url_is_required(monkeypatch):
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setenv("DISCORD_WEBHOOK_URL", "https://discord.test/webhook")

    with pytest.raises(RuntimeError, match="DATABASE_URL"):
        Settings.from_env()


def test_database_url_requires_password(monkeypatch):
    monkeypatch.setenv(
        "DATABASE_URL",
        "postgresql://neondb_owner@ep-example-pooler.us-east-1.aws.neon.tech/neondb?sslmode=require",
    )
    monkeypatch.setenv("DISCORD_WEBHOOK_URL", "https://discord.test/webhook")

    with pytest.raises(RuntimeError, match="missing the database password"):
        Settings.from_env()


def test_database_url_must_be_postgres(monkeypatch):
    monkeypatch.setenv(
        "DATABASE_URL",
        "https://example.test",
    )
    monkeypatch.setenv("DISCORD_WEBHOOK_URL", "https://discord.test/webhook")

    with pytest.raises(RuntimeError, match="postgresql://"):
        Settings.from_env()
