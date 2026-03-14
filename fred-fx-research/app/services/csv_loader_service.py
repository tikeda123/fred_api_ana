"""
CSV OHLC Loader Service
- ブローカー CSV (タブ区切り, ヘッダーなし) を fact_ohlc_intraday にロード
- 日足 close で FRED 欠測日を normalized テーブルに補完

CSV 形式:
  datetime_open\tOpen\tHigh\tLow\tClose\tVolume
  2026-03-10 00:00\t157.876\t158.184\t157.272\t158.104\t187953

datetime は Open 時刻 (サーバータイム = UTC 想定)。
"""

from datetime import datetime, date
from pathlib import Path
from typing import Optional

import polars as pl

from app.core.exceptions import StorageError
from app.core.logging import get_logger
from app.storage.duckdb import get_connection

logger = get_logger(__name__)

# pair ↔ CSV サブディレクトリ / ファイル名プレフィックスの対応
PAIR_CSV_MAP = {
    "USDJPY": "usdjpy/USDJPY",
    "EURUSD": "eurusd/EURUSD",
    "AUDUSD": "audusd/AUDUSD",
}

VALID_TIMEFRAMES = {"1", "5", "15", "30", "60", "240", "1440"}


def load_csv_to_ohlc(
    csv_path: str | Path,
    pair: str,
    timeframe: str,
) -> int:
    """
    単一 CSV ファイルを fact_ohlc_intraday にロードする。

    Args:
        csv_path: CSV ファイルパス
        pair: 通貨ペア (USDJPY / EURUSD / AUDUSD)
        timeframe: 足 ('1','5','15','30','60','240','1440')

    Returns:
        ロードした行数
    """
    if timeframe not in VALID_TIMEFRAMES:
        raise ValueError(f"無効な timeframe: {timeframe} (有効: {VALID_TIMEFRAMES})")

    path = Path(csv_path)
    if not path.exists():
        raise FileNotFoundError(f"CSV が見つかりません: {path}")

    # タブ区切り、ヘッダーなし
    df = pl.read_csv(
        path,
        separator="\t",
        has_header=False,
        new_columns=["datetime_str", "open", "high", "low", "close", "volume"],
        schema_overrides={
            "datetime_str": pl.Utf8,
            "open": pl.Float64,
            "high": pl.Float64,
            "low": pl.Float64,
            "close": pl.Float64,
            "volume": pl.Int64,
        },
    )

    # datetime パース
    df = df.with_columns(
        pl.col("datetime_str").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M").alias("datetime_utc")
    )

    df = df.with_columns([
        pl.lit(pair).alias("pair"),
        pl.lit(timeframe).alias("timeframe"),
        pl.lit("csv").alias("source"),
        pl.lit(datetime.utcnow()).alias("loaded_at"),
    ])

    # 必要な列を選択
    insert_df = df.select([
        "pair", "datetime_utc", "timeframe",
        "open", "high", "low", "close", "volume",
        "source", "loaded_at",
    ])

    conn = get_connection()
    try:
        conn.register("_ohlc_df", insert_df.to_arrow())
        conn.execute("""
            INSERT OR REPLACE INTO fact_ohlc_intraday
            SELECT pair, datetime_utc, timeframe, open, high, low, close,
                   volume, source, loaded_at
            FROM _ohlc_df
        """)
        conn.unregister("_ohlc_df")
    except Exception as e:
        raise StorageError(f"OHLC ロード失敗: {e}") from e

    logger.info("ohlc_loaded", pair=pair, timeframe=timeframe, rows=len(insert_df), path=str(path))
    return len(insert_df)


def load_all_from_directory(
    data_dir: str | Path,
    pairs: Optional[list[str]] = None,
    timeframes: Optional[list[str]] = None,
) -> dict[str, int]:
    """
    data_dir 配下の全 CSV をロードする。

    Args:
        data_dir: CSV のルートディレクトリ (usdjpy/ eurusd/ audusd/ を含む)
        pairs: 対象ペア (None = 全ペア)
        timeframes: 対象足 (None = 全足)

    Returns:
        {"{pair}_{timeframe}": ロード行数} の辞書
    """
    data_dir = Path(data_dir)
    target_pairs = pairs or list(PAIR_CSV_MAP.keys())
    target_tfs = timeframes or sorted(VALID_TIMEFRAMES, key=lambda x: int(x))
    results: dict[str, int] = {}

    for pair in target_pairs:
        prefix = PAIR_CSV_MAP.get(pair)
        if not prefix:
            logger.warning("csv_pair_unknown", pair=pair)
            continue

        for tf in target_tfs:
            csv_path = data_dir / f"{prefix}{tf}.csv"
            if not csv_path.exists():
                logger.warning("csv_not_found", pair=pair, timeframe=tf, path=str(csv_path))
                continue
            try:
                count = load_csv_to_ohlc(csv_path, pair, tf)
                results[f"{pair}_{tf}"] = count
            except Exception as e:
                logger.error("csv_load_failed", pair=pair, timeframe=tf, error=str(e))
                results[f"{pair}_{tf}"] = 0

    return results


def get_daily_ohlc(
    pair: str,
    start: Optional[date] = None,
    end: Optional[date] = None,
) -> pl.DataFrame:
    """fact_ohlc_intraday から日足 OHLC を取得する。"""
    conn = get_connection()
    conditions = ["pair = ?", "timeframe = '1440'"]
    params: list = [pair]
    if start:
        conditions.append("CAST(datetime_utc AS DATE) >= ?")
        params.append(start)
    if end:
        conditions.append("CAST(datetime_utc AS DATE) <= ?")
        params.append(end)

    sql = f"""
        SELECT CAST(datetime_utc AS DATE) AS obs_date,
               open, high, low, close, volume
        FROM fact_ohlc_intraday
        WHERE {' AND '.join(conditions)}
        ORDER BY obs_date
    """
    rows = conn.execute(sql, params).fetchall()
    if not rows:
        return pl.DataFrame()
    return pl.DataFrame({
        "obs_date": [r[0] for r in rows],
        "open":     [r[1] for r in rows],
        "high":     [r[2] for r in rows],
        "low":      [r[3] for r in rows],
        "close":    [r[4] for r in rows],
        "volume":   [r[5] for r in rows],
    })


def get_intraday_ohlc(
    pair: str,
    timeframe: str,
    start_dt: Optional[datetime] = None,
    end_dt: Optional[datetime] = None,
) -> pl.DataFrame:
    """fact_ohlc_intraday から指定足の OHLC を取得する。"""
    conn = get_connection()
    conditions = ["pair = ?", "timeframe = ?"]
    params: list = [pair, timeframe]
    if start_dt:
        conditions.append("datetime_utc >= ?")
        params.append(start_dt)
    if end_dt:
        conditions.append("datetime_utc <= ?")
        params.append(end_dt)

    sql = f"""
        SELECT datetime_utc, open, high, low, close, volume
        FROM fact_ohlc_intraday
        WHERE {' AND '.join(conditions)}
        ORDER BY datetime_utc
    """
    rows = conn.execute(sql, params).fetchall()
    if not rows:
        return pl.DataFrame()
    return pl.DataFrame({
        "datetime_utc": [r[0] for r in rows],
        "open":   [r[1] for r in rows],
        "high":   [r[2] for r in rows],
        "low":    [r[3] for r in rows],
        "close":  [r[4] for r in rows],
        "volume": [r[5] for r in rows],
    })


def supplement_fred_gaps(pair: str) -> int:
    """
    FRED 欠測日を CSV の日足 close で補完する。
    fact_market_series_normalized に is_supplemental=TRUE で挿入。

    Returns:
        補完した行数
    """
    from app.models.domain_models import FX_SERIES_MAP

    # pair → FRED series_id のマッピング
    PAIR_TO_SERIES = {
        "USDJPY": "DEXJPUS",
        "EURUSD": "DEXUSEU",
        "AUDUSD": "DEXUSAL",
    }
    series_id = PAIR_TO_SERIES.get(pair)
    if not series_id:
        logger.warning("supplement_pair_unknown", pair=pair)
        return 0

    fx_info = FX_SERIES_MAP.get(series_id, {})
    domain = fx_info.get("domain", "fx_spot")
    base_ccy = fx_info.get("base_ccy")
    quote_ccy = fx_info.get("quote_ccy")

    conn = get_connection()

    # FRED normalized に存在する日付を取得
    fred_dates = conn.execute("""
        SELECT obs_date FROM fact_market_series_normalized
        WHERE series_id = ? AND is_supplemental = FALSE
    """, [series_id]).fetchall()
    fred_date_set = {r[0] for r in fred_dates}

    # CSV 日足を取得
    csv_daily = get_daily_ohlc(pair)
    if csv_daily.is_empty():
        logger.info("supplement_no_csv", pair=pair)
        return 0

    # FRED に無い日の CSV close を補完行として構築
    now = datetime.utcnow()
    today = date.today()
    rows = []
    for row in csv_daily.to_dicts():
        obs_date = row["obs_date"]
        if obs_date in fred_date_set:
            continue  # FRED に既にある日はスキップ
        if row["close"] is None:
            continue
        rows.append({
            "series_id": series_id,
            "obs_date": obs_date,
            "domain": domain,
            "pair": pair,
            "base_ccy": base_ccy,
            "quote_ccy": quote_ccy,
            "value": row["close"],
            "units_normalized": "price_quote_per_base",
            "is_derived": False,
            "is_supplemental": True,
            "transformation": "csv_close",
            "source_frequency": "D",
            "frequency_requested": "D",
            "aggregation_method": "eop",
            "as_of_realtime_start": today,
            "as_of_realtime_end": today,
            "created_at": now,
        })

    if not rows:
        logger.info("supplement_no_gaps", pair=pair)
        return 0

    df = pl.DataFrame(rows)
    try:
        conn.register("_supp_df", df.to_arrow())
        conn.execute("""
            INSERT OR REPLACE INTO fact_market_series_normalized
            SELECT series_id, obs_date, domain, pair, base_ccy, quote_ccy,
                   value, units_normalized, is_derived, is_supplemental,
                   transformation, source_frequency, frequency_requested,
                   aggregation_method, as_of_realtime_start, as_of_realtime_end,
                   created_at
            FROM _supp_df
        """)
        conn.unregister("_supp_df")
    except Exception as e:
        raise StorageError(f"補完挿入失敗: {e}") from e

    logger.info("supplement_done", pair=pair, supplemented=len(rows))
    return len(rows)
