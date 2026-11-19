"""Environment validation. Never expose credentials via repr or diagnostics."""
from dataclasses import dataclass, field
from pathlib import Path
from typing import Mapping


@dataclass(frozen=True)
class Settings:
    bot_token: str = field(repr=False)
    owner_ids: tuple[int, ...]
    database_path: Path
    policy_path: Path
    ai_enabled: bool = False
    ai_api_key: str = field(default="", repr=False)

    @classmethod
    def from_env(cls, env: Mapping[str, str]) -> "Settings":
        token = env.get("TELEGRAM_BOT_TOKEN", "").strip()
        if not token:
            raise ValueError("TELEGRAM_BOT_TOKEN is required")
        try:
            owners = tuple(int(x.strip()) for x in env.get("BOT_OWNER_IDS", "").split(",") if x.strip())
        except ValueError:
            raise ValueError("BOT_OWNER_IDS must contain numeric Telegram IDs") from None
        if any(x <= 0 for x in owners):
            raise ValueError("BOT_OWNER_IDS must contain positive IDs")
        raw_ai = env.get("AI_ENABLED", "false").strip().lower()
        if raw_ai not in ("true", "false"):
            raise ValueError("AI_ENABLED must be true or false")
        ai_key = env.get("AI_API_KEY", "").strip()
        if raw_ai == "true" and not ai_key:
            raise ValueError("AI_API_KEY is required when AI is enabled")
        return cls(token, owners, Path(env.get("DATABASE_PATH", "data/antispam.sqlite3")),
                   Path(env.get("POLICY_PATH", "config/local/policy.json")), raw_ai == "true", ai_key)
