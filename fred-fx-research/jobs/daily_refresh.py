"""
daily_refresh.py
毎営業日実行するデイリー更新ジョブ。

処理順:
  1. 日次 FX + VIX + USD Broad を直近 30 日分 refresh
  2. normalize
  3. 全 pair の factor / regime を再計算
  4. freshness audit
  5. 監査サマリーをログ出力

Usage:
  python jobs/daily_refresh.py
"""

import asyncio
import sys
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from app.core.logging import get_logger, setup_logging
from app.services.ingest_service import fetch_and_store
from app.services.normalize_service import normalize_series
from app.services.panel_service import get_panel
from app.services.factor_service import compute_factors, save_factors
from app.services.regime_service import tag_regimes, save_regimes
from app.services.freshness_service import audit_all

setup_logging()
logger = get_logger("daily_refresh")

DAILY_SERIES = ["DEXJPUS", "DEXUSEU", "DEXUSAL", "DTWEXBGS", "VIXCLS"]
PAIRS = ["USDJPY", "EURUSD", "AUDUSD"]


async def run() -> None:
    today = date.today()
    start = today - timedelta(days=30)
    logger.info("daily_refresh_start", date=today)

    # 1. refresh raw observations
    for sid in DAILY_SERIES:
        try:
            r = await fetch_and_store(sid, observation_start=start, observation_end=today)
            logger.info("fetched", series_id=sid, count=r.count)
        except Exception as e:
            logger.error("fetch_failed", series_id=sid, error=str(e))

    # 2. normalize
    for sid in DAILY_SERIES:
        try:
            cnt = normalize_series(sid, start=start, end=today)
            logger.info("normalized", series_id=sid, rows=cnt)
        except Exception as e:
            logger.error("normalize_failed", series_id=sid, error=str(e))

    # 3. factor + regime (3年分パネルで再計算)
    panel_start = today - timedelta(days=365 * 3)
    for pair in PAIRS:
        try:
            result = get_panel(pair, panel_start, today)
            panel = result["panel"]
            factors = compute_factors(panel, pair)
            save_factors(factors, pair)
            regimes = tag_regimes(panel, factors, pair)
            save_regimes(regimes, pair)
            logger.info("factors_regimes_updated", pair=pair, rows=len(panel))
        except Exception as e:
            logger.error("factor_regime_failed", pair=pair, error=str(e))

    # 4. freshness audit
    audit_results = audit_all()
    stale = [r for r in audit_results if r["status"] != "ok"]
    logger.info("freshness_audit", total=len(audit_results), stale=len(stale))
    for r in stale:
        logger.warning("stale_series", **r)

    logger.info("daily_refresh_done", date=today)


if __name__ == "__main__":
    asyncio.run(run())
