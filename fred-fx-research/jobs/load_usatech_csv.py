"""
USATECH CSV 一括ロード

Usage:
    python jobs/load_usatech_csv.py \\
        --data-dir /path/to/data/us_index \\
        --timeframes 240,1440

注意:
    DuckDB は single-writer のため、API サーバーを停止してから実行すること。
    API 起動中に実行すると lock エラーになる。
"""

import argparse
import sys
import time
import uuid
from datetime import datetime
from pathlib import Path

# プロジェクトルートを sys.path に追加
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.services import csv_ingest_service, market_bar_service
from app.storage.duckdb import get_connection
from app.core.logging import setup_logging, get_logger

logger = get_logger(__name__)

CANONICAL_SYMBOL = "USATECHIDXUSD"
TIMEFRAME_TO_INSTRUMENT = {
    "1":    "usatechidxusd_m1",
    "5":    "usatechidxusd_m5",
    "15":   "usatechidxusd_m15",
    "30":   "usatechidxusd_m30",
    "60":   "usatechidxusd_h1",
    "240":  "usatechidxusd_h4",
    "1440": "usatechidxusd_d1",
}


def _upsert_instrument(conn, instrument_id: str, timeframe: str) -> None:
    now = datetime.utcnow()
    conn.execute(
        """
        INSERT INTO md.dim_market_instrument (
            instrument_id, source_system, source_vendor,
            vendor_symbol, canonical_symbol, instrument_name,
            asset_class, base_ccy, quote_ccy,
            timeframe_native, timezone_native,
            file_format, delimiter, has_header, ts_format,
            is_active, created_at, updated_at
        ) VALUES (
            ?, 'local_csv', 'manual_upload',
            'USATECHIDXUSD', 'USATECHIDXUSD',
            ?, 'equity_index', 'USATECHIDX', 'USD',
            ?, 'UTC', 'csv_tsv', E'\\t', FALSE, '%Y-%m-%d %H:%M',
            TRUE, ?, ?
        )
        ON CONFLICT (instrument_id) DO UPDATE SET
            updated_at = excluded.updated_at,
            is_active  = TRUE
        """,
        [
            instrument_id,
            f"US Tech 100 Index ({timeframe}min)",
            timeframe,
            now, now,
        ],
    )


def load_file(data_dir: Path, timeframe: str) -> dict:
    """1 ファイルをロードしてサマリを返す"""
    csv_path = data_dir / f"{CANONICAL_SYMBOL}{timeframe}.csv"
    if not csv_path.exists():
        return {"timeframe": timeframe, "status": "not_found", "path": str(csv_path)}

    instrument_id = TIMEFRAME_TO_INSTRUMENT.get(timeframe)
    if not instrument_id:
        return {"timeframe": timeframe, "status": "unknown_timeframe"}

    # instrument マスタ登録
    conn = get_connection()
    _upsert_instrument(conn, instrument_id, timeframe)

    # upload_id 生成
    upload_id = f"upl_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
    file_hash = csv_ingest_service.compute_file_hash(csv_path)
    file_size = csv_path.stat().st_size

    from app.storage.repositories import upload_repo
    upload_repo.create_audit(
        upload_id=upload_id,
        instrument_id=instrument_id,
        source_system="job_load_usatech_csv",
        file_name=csv_path.name,
        file_hash=file_hash,
        file_size=file_size,
        parser_options={"delimiter": "\t", "has_header": False, "ts_format": "%Y-%m-%d %H:%M"},
    )

    t0 = time.time()
    try:
        result = csv_ingest_service.ingest(
            file_path=csv_path,
            instrument_id=instrument_id,
            upload_id=upload_id,
            timeframe=timeframe,
            delimiter="\t",
            has_header=False,
            ts_format="%Y-%m-%d %H:%M",
        )
        elapsed = time.time() - t0

        # H4 / D1 のみ日次集約を実行
        if timeframe in ("240", "1440"):
            daily_n = market_bar_service.rebuild_daily(instrument_id)
            result["daily_rows_rebuilt"] = daily_n

        return {
            "timeframe": timeframe,
            "instrument_id": instrument_id,
            "status": "loaded",
            "elapsed_sec": round(elapsed, 2),
            **result,
        }
    except Exception as e:
        return {"timeframe": timeframe, "instrument_id": instrument_id, "status": "failed", "error": str(e)}


def main():
    parser = argparse.ArgumentParser(description="USATECH CSV 一括ロード")
    parser.add_argument(
        "--data-dir",
        required=True,
        help="CSV ファイルが置かれたディレクトリ（USATECHIDXUSD240.csv 等を含む）",
    )
    parser.add_argument(
        "--timeframes",
        default="240,1440",
        help="ロード対象の足（カンマ区切り）: 1,5,15,30,60,240,1440",
    )
    args = parser.parse_args()

    setup_logging()
    data_dir = Path(args.data_dir)
    if not data_dir.exists():
        print(f"[ERROR] data_dir が存在しません: {data_dir}")
        sys.exit(1)

    timeframes = [tf.strip() for tf in args.timeframes.split(",")]
    print(f"\n=== USATECH CSV ロード開始 ({datetime.now().strftime('%Y-%m-%d %H:%M:%S')}) ===")
    print(f"data_dir   : {data_dir}")
    print(f"timeframes : {timeframes}\n")

    results = []
    for tf in timeframes:
        print(f"[{tf}min] ロード中...", end=" ", flush=True)
        summary = load_file(data_dir, tf)
        results.append(summary)
        if summary["status"] == "loaded":
            print(
                f"OK  detected={summary.get('row_count_detected','-')} "
                f"loaded={summary.get('row_count_loaded','-')} "
                f"rejected={summary.get('row_count_rejected','-')} "
                f"daily={summary.get('daily_rows_rebuilt','-')} "
                f"({summary.get('elapsed_sec','-')}s)"
            )
        elif summary["status"] == "not_found":
            print(f"SKIP (ファイルなし: {summary['path']})")
        else:
            print(f"FAIL  {summary.get('error','unknown error')}")

    print("\n=== 完了 ===")
    loaded = [r for r in results if r["status"] == "loaded"]
    failed = [r for r in results if r["status"] == "failed"]
    print(f"  成功: {len(loaded)} / {len(timeframes)}  失敗: {len(failed)}")


if __name__ == "__main__":
    main()
