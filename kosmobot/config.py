from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from zoneinfo import ZoneInfo

from dotenv import load_dotenv


@dataclass(slots=True)
class Settings:
    bot_token: str
    admin_ids: set[int]
    excel_path: Path
    db_path: Path
    league_name: str
    timezone: str
    lineup_deadline_hour: int
    lineup_deadline_minute: int
    current_stage: str
    current_round: int
    allow_self_scoring: bool
    default_lineup_limits: str

    @property
    def tzinfo(self) -> ZoneInfo:
        return ZoneInfo(self.timezone)


def _parse_admin_ids(raw: str) -> set[int]:
    return {int(x.strip()) for x in raw.split(",") if x.strip()}


def load_settings() -> Settings:
    load_dotenv()
    token = os.getenv("BOT_TOKEN", "").strip()
    if not token:
        raise RuntimeError("BOT_TOKEN is required")

    return Settings(
        bot_token=token,
        admin_ids=_parse_admin_ids(os.getenv("ADMIN_IDS", "")),
        excel_path=Path(os.getenv("EXCEL_PATH", "./КосмоФентези_v4_BeteraPool_complete.xlsx")),
        db_path=Path(os.getenv("DB_PATH", "./bot.sqlite3")),
        league_name=os.getenv("LEAGUE_NAME", "КосмоФентези").strip() or "КосмоФентези",
        timezone=os.getenv("TIMEZONE", "Europe/Amsterdam"),
        lineup_deadline_hour=int(os.getenv("LINEUP_DEADLINE_HOUR", "18")),
        lineup_deadline_minute=int(os.getenv("LINEUP_DEADLINE_MINUTE", "0")),
        current_stage=os.getenv("CURRENT_STAGE", "Регулярка"),
        current_round=int(os.getenv("CURRENT_ROUND", "1")),
        allow_self_scoring=os.getenv("ALLOW_SELF_SCORING", "false").strip().lower() == "true",
        default_lineup_limits=os.getenv("DEFAULT_LINEUP_LIMITS", "Вратарь=1,Защитник=2,Нападающий=2").strip(),
    )
