"""
FX クロスアセットパネルのみ再構築（特徴量は再計算しない）

Usage:
    python jobs/rebuild_fx_cross_asset_panel.py \\
        --pairs USDJPY,EURUSD,AUDUSD \\
        [--start 2020-01-01] [--end 2026-03-13]
"""

import argparse
import sys
import time
from datetime import date, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from app.core.logging import setup_logging
from app.services import join_panel_service


def main():
    parser = argparse.ArgumentParser(description="FX クロスアセットパネル再構築")
    parser.add_argument("--pairs", default="USDJPY,EURUSD,AUDUSD")
    parser.add_argument("--start", default=None, help="YYYY-MM-DD")
    parser.add_argument("--end",   default=None, help="YYYY-MM-DD")
    args = parser.parse_args()

    setup_logging()

    start = date.fromisoformat(args.start) if args.start else None
    end   = date.fromisoformat(args.end)   if args.end   else None
    pairs = [p.strip() for p in args.pairs.split(",")]

    print(f"\n=== FX パネル再構築 ({datetime.now().strftime('%Y-%m-%d %H:%M:%S')}) ===")
    for pair in pairs:
        print(f"  [{pair}] ...", end=" ", flush=True)
        t0 = time.time()
        n = join_panel_service.build_panel(pair, start, end)
        print(f"OK  {n} rows ({time.time() - t0:.1f}s)")
    print("\n=== 完了 ===")


if __name__ == "__main__":
    main()
