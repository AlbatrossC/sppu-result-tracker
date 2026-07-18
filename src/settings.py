import os
from dataclasses import dataclass
from urllib.parse import urlparse

from dotenv import load_dotenv


load_dotenv()


@dataclass(frozen=True)
class Settings:
    database_url: str
    discord_webhook_url: str
    result_url: str = "https://onlineresults.unipune.ac.in/Result/Dashboard/Default"
    minimum_result_count: int = 25
    suspicious_count_ratio: float = 0.70
    max_notifications_per_run: int = 100

    @classmethod
    def from_env(cls, require_discord: bool = True) -> "Settings":
        database_url = os.getenv("DATABASE_URL", "").strip()
        discord_webhook_url = os.getenv("DISCORD_WEBHOOK_URL", "").strip()

        missing = []
        if not database_url:
            missing.append("DATABASE_URL")
        if require_discord and not discord_webhook_url:
            missing.append("DISCORD_WEBHOOK_URL")
        if missing:
            raise RuntimeError(f"Missing required environment variables: {', '.join(missing)}")

        _validate_database_url(database_url)

        return cls(
            database_url=database_url,
            discord_webhook_url=discord_webhook_url,
            result_url=os.getenv("SPPU_RESULT_URL", cls.result_url).strip(),
        )


def _validate_database_url(database_url: str) -> None:
    parsed = urlparse(database_url)
    if parsed.scheme not in {"postgresql", "postgres"}:
        raise RuntimeError("DATABASE_URL must start with postgresql:// or postgres://")
    if not parsed.hostname:
        raise RuntimeError("DATABASE_URL is missing a database host")
    if not parsed.username:
        raise RuntimeError("DATABASE_URL is missing a database username")
    if parsed.password is None:
        raise RuntimeError(
            "DATABASE_URL is missing the database password. Use the full PostgreSQL connection "
            "string from your database provider, including ?sslmode=require."
        )
