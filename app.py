from __future__ import annotations

from kosmobot.bot import BotServices, build_application
from kosmobot.config import load_settings
from kosmobot.db import Database
from kosmobot.excel_sync import ExcelRepository


def main() -> None:
    settings = load_settings()
    db = Database(settings.db_path)
    excel = ExcelRepository(settings.excel_path)
    services = BotServices(settings=settings, db=db, excel=excel)
    app = build_application(services)
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
