"""
backfill_series.py
指定 series を指定期間で一括取り込み + 正規化するジョブ。

Usage:
  python jobs/backfill_series.py --start 2020-01-01 --end 2026-03-14
  python jobs/backfill_series.py --series DEXJPUS VIXCLS --start 2020-01-01
"""

import asyncio
import argparse
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from app.core.logging import get_logger, setup_logging
from app.services.ingest_service import fetch_and_store
from app.services.normalize_service import normalize_series

setup_logging()
logger = get_logger("backfill")

# デフォルトで取り込む全 series
DEFAULT_SERIES = [
    # FX spot
    "DEXJPUS", "DEXUSEU", "DEXUSAL", "DTWEXBGS",
    # Policy rates (monthly)
    "EFFR", "ECBDFR", "IRSTCI01JPM156N", "IRSTCI01AUM156N",
    # 3M rates (monthly)
    "IR3TIB01USM156N", "IR3TIB01JPM156N", "IR3TIB01EZM156N", "IR3TIB01AUM156N",
    # 10Y yields (monthly)
    "IRLTLT01USM156N", "IRLTLT01JPM156N", "IRLTLT01EZM156N", "IRLTLT01AUM156N",
    # Risk / commodity
    "VIXCLS", "STLFSI4", "DCOILWTICO",
    # BoJ balance sheet
    "JPNASSETS",
    # BIS REER (monthly)
    "RBUSBIS", "RBJPBIS", "RBAUBIS", "RBXMBIS",
]


async def backfill(series_ids: list[str], start: date, end: date) -> None:
    ok, failed = 0, 0
    for sid in series_ids:
        try:
            r = await fetch_and_store(sid, observation_start=start, observation_end=end)
            cnt = normalize_series(sid)
            logger.info("backfill_ok", series_id=sid, raw=r.count, normalized=cnt)
            ok += 1
        except Exception as e:
            logger.error("backfill_failed", series_id=sid, error=str(e))
            failed += 1

    logger.info("backfill_done", ok=ok, failed=failed, total=ok + failed)


def main() -> None:
    parser = argparse.ArgumentParser(description="FRED series backfill")
    parser.add_argument("--series", nargs="*", help="series IDs (default: all)")
    parser.add_argument("--start", default="2020-01-01", help="observation_start (YYYY-MM-DD)")
    parser.add_argument("--end", default=str(date.today()), help="observation_end (YYYY-MM-DD)")
    args = parser.parse_args()

    series_ids = args.series or DEFAULT_SERIES
    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end)

    logger.info("backfill_start", series=series_ids, start=start, end=end)
    asyncio.run(backfill(series_ids, start, end))


if __name__ == "__main__":
    main()
