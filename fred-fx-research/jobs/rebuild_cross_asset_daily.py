"""
クロスアセット特徴量・パネル 再構築ジョブ

Usage:
    python jobs/rebuild_cross_asset_daily.py \\
        --instrument-id usatechidxusd_h4 \\
        --pairs USDJPY,EURUSD,AUDUSD \\
        [--start 2020-01-01] [--end 2026-03-13]

注意:
    DuckDB single-writer のため、API サーバーを停止してから実行すること。
"""

import argparse
import sys
import time
from datetime import date, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from app.core.logging import setup_logging, get_logger
from app.services import cross_asset_feature_service, join_panel_service

logger = get_logger(__name__)


def main():
    parser = argparse.ArgumentParser(description="クロスアセット特徴量・パネル再構築")
    parser.add_argument("--instrument-id", default="usatechidxusd_h4")
    parser.add_argument("--pairs", default="USDJPY,EURUSD,AUDUSD")
    parser.add_argument("--start", default=None, help="YYYY-MM-DD")
    parser.add_argument("--end",   default=None, help="YYYY-MM-DD")
    parser.add_argument(
        "--skip-panel", action="store_true",
        help="特徴量計算のみ実行し、パネル再構築はスキップ",
    )
    args = parser.parse_args()

    setup_logging()

    start = date.fromisoformat(args.start) if args.start else None
    end   = date.fromisoformat(args.end)   if args.end   else None
    pairs = [p.strip() for p in args.pairs.split(",")]

    print(f"\n=== クロスアセット再構築 ({datetime.now().strftime('%Y-%m-%d %H:%M:%S')}) ===")
    print(f"instrument_id : {args.instrument_id}")
    print(f"pairs         : {pairs}")
    print(f"range         : {start or 'ALL'} ~ {end or 'ALL'}\n")

    # ── 1. instrument-level 特徴量 ──────────────────────────────
    print("[1/3] instrument-level 特徴量を計算中...", end=" ", flush=True)
    t0 = time.time()
    inst_n = cross_asset_feature_service.rebuild_instrument_features(
        instrument_id=args.instrument_id,
        start=start,
        end=end,
    )
    print(f"OK  {inst_n} rows ({time.time() - t0:.1f}s)")

    # ── 2. pair-level 特徴量 ─────────────────────────────────────
    print("[2/3] pair-level 特徴量を計算中...", end=" ", flush=True)
    t0 = time.time()
    pair_n = cross_asset_feature_service.rebuild_pair_features(
        pairs=pairs,
        source_instrument_id=args.instrument_id,
        start=start,
        end=end,
    )
    print(f"OK  {pair_n} rows ({time.time() - t0:.1f}s)")

    # ── 3. mart パネル構築 ───────────────────────────────────────
    if not args.skip_panel:
        print("[3/3] FX クロスアセットパネルを構築中...")
        for pair in pairs:
            print(f"  [{pair}] ...", end=" ", flush=True)
            t0 = time.time()
            n = join_panel_service.build_panel(pair, start, end)
            print(f"OK  {n} rows ({time.time() - t0:.1f}s)")
    else:
        print("[3/3] パネル構築スキップ")

    print("\n=== 完了 ===")


if __name__ == "__main__":
    main()
