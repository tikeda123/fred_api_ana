"""
refresh_release_calendar.py
FRED の release calendar を取得して DuckDB に保存するジョブ。

Usage:
  python jobs/refresh_release_calendar.py
  python jobs/refresh_release_calendar.py --start 2026-01-01 --end 2026-12-31
"""

import asyncio
import argparse
import sys
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from app.core.logging import get_logger, setup_logging
from app.services.fred_client import FredClient
from app.storage.duckdb import get_connection

setup_logging()
logger = get_logger("release_calendar")


async def fetch_and_store(start: date, end: date) -> int:
    async with FredClient() as client:
        resp = await client.get_release_dates(start_date=start, end_date=end)

    release_dates = resp.get("release_dates", [])
    conn = get_connection()
    from datetime import datetime
    now = datetime.utcnow()
    inserted = 0

    for rd in release_dates:
        try:
            conn.execute(
                """
                INSERT OR REPLACE INTO fact_release_calendar
                (release_id, release_name, release_date, press_release, link, retrieved_at_utc)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (rd.get("release_id"), rd.get("release_name", ""),
                 rd.get("date"), rd.get("press_release"), rd.get("link"), now),
            )
            inserted += 1
        except Exception:
            pass

    logger.info("release_calendar_stored", inserted=inserted, start=start, end=end)
    return inserted


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", default=str(date.today()))
    parser.add_argument("--end",   default=str(date.today() + timedelta(days=90)))
    args = parser.parse_args()
    asyncio.run(fetch_and_store(date.fromisoformat(args.start), date.fromisoformat(args.end)))


if __name__ == "__main__":
    main()
