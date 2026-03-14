"""
load_csv_ohlc.py
CSV OHLC データを fact_ohlc_intraday にロードし、
FRED 欠測日を CSV close で補完する。

Usage:
  # 全ペア・全足をロード + FRED 補完
  python jobs/load_csv_ohlc.py --data-dir /path/to/data

  # 日足のみロード
  python jobs/load_csv_ohlc.py --data-dir /path/to/data --timeframes 1440

  # 特定ペアのみ
  python jobs/load_csv_ohlc.py --data-dir /path/to/data --pairs USDJPY EURUSD

  # FRED 補完をスキップ
  python jobs/load_csv_ohlc.py --data-dir /path/to/data --no-supplement
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from app.core.logging import get_logger, setup_logging
from app.services.csv_loader_service import (
    load_all_from_directory,
    supplement_fred_gaps,
    VALID_TIMEFRAMES,
)

setup_logging()
logger = get_logger("load_csv_ohlc")


def main() -> None:
    parser = argparse.ArgumentParser(description="CSV OHLC ローダー")
    parser.add_argument(
        "--data-dir",
        default="/home/tikeda/workspace/trade/market_api/fred_api/data",
        help="CSV ルートディレクトリ",
    )
    parser.add_argument("--pairs", nargs="*", help="対象ペア (default: all)")
    parser.add_argument("--timeframes", nargs="*", help="対象足 (default: all)")
    parser.add_argument("--no-supplement", action="store_true", help="FRED 補完をスキップ")
    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    if not data_dir.exists():
        logger.error("data_dir_not_found", path=str(data_dir))
        sys.exit(1)

    pairs = args.pairs
    timeframes = args.timeframes

    logger.info("load_start", data_dir=str(data_dir), pairs=pairs, timeframes=timeframes)

    # 1. CSV → fact_ohlc_intraday
    results = load_all_from_directory(data_dir, pairs=pairs, timeframes=timeframes)
    total = sum(results.values())
    logger.info("load_complete", total_rows=total, details=results)

    for key, count in sorted(results.items()):
        print(f"  {key}: {count:>8,} rows")
    print(f"  Total: {total:>8,} rows")

    # 2. FRED 欠測補完
    if not args.no_supplement:
        target_pairs = pairs or ["USDJPY", "EURUSD", "AUDUSD"]
        print("\nFRED 欠測補完:")
        for pair in target_pairs:
            try:
                count = supplement_fred_gaps(pair)
                print(f"  {pair}: {count} rows supplemented")
                logger.info("supplement_done", pair=pair, count=count)
            except Exception as e:
                logger.error("supplement_failed", pair=pair, error=str(e))
                print(f"  {pair}: FAILED - {e}")

    print("\nDone.")


if __name__ == "__main__":
    main()
