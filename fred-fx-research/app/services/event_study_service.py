"""
Event Study Service
- イベント周辺の FX リターン分析
"""

from datetime import date, timedelta
from typing import Optional

import polars as pl

from app.core.logging import get_logger
from app.storage.duckdb import get_connection

logger = get_logger(__name__)


def analyze_event_window(
    pair: str,
    event_dates: list[date],
    window: int = 5,
    start: Optional[date] = None,
    end: Optional[date] = None,
) -> dict:
    """
    イベント日周辺の FX リターンを分析する。
    """
    # Spot data を取得
    spot_df = _load_spot_data(pair, start, end)
    if spot_df.is_empty():
        return {"events": [], "mean_path": [], "median_path": [], "stats": {}, "valid_event_count": 0}

    spot_df = spot_df.sort("obs_date")

    # daily return を計算
    spot_df = spot_df.with_columns(
        (pl.col("value") / pl.col("value").shift(1) - 1).alias("return_1d")
    )

    # obs_date のリスト（lookup用）
    dates_list = spot_df["obs_date"].to_list()
    returns_list = spot_df["return_1d"].to_list()

    events_result = []
    cum_paths = []

    for ev_date in event_dates:
        # ev_date に最も近い取引日を見つける
        ev_idx = _find_nearest_idx(dates_list, ev_date)
        if ev_idx is None:
            continue

        # pre-window と post-window の return を取得
        pre_start = max(0, ev_idx - window)
        post_end = min(len(returns_list), ev_idx + window + 1)

        pre_returns = [r for r in returns_list[pre_start:ev_idx] if r is not None]
        post_returns = [r for r in returns_list[ev_idx:post_end] if r is not None]

        if len(pre_returns) < window // 2 or len(post_returns) < 1:
            continue

        # window サイズに合わせる
        pre_returns = pre_returns[-window:]
        post_returns = post_returns[:window]

        # cumulative return path
        all_returns = pre_returns + post_returns
        cum = [0.0]
        for r in all_returns:
            cum.append(cum[-1] + r)

        # 必要な長さにパディング/トリミング
        expected_len = window * 2 + 1
        if len(cum) >= expected_len:
            cum = cum[:expected_len]
            cum_paths.append(cum)

        pre_cum = sum(pre_returns)
        post_cum = sum(post_returns)

        events_result.append({
            "event_date": str(ev_date),
            "pre_cum_return": round(pre_cum * 100, 4),
            "post_cum_return": round(post_cum * 100, 4),
            "direction": "UP" if post_cum > 0 else "DOWN",
        })

    # 平均・中央値パス
    mean_path = []
    median_path = []
    if cum_paths:
        import numpy as np
        arr = np.array(cum_paths)
        mean_path = (arr.mean(axis=0) * 100).round(4).tolist()
        median_path = (np.median(arr, axis=0) * 100).round(4).tolist()

    # 統計
    hit_count = sum(1 for e in events_result if e["direction"] == "UP")
    valid_count = len(events_result)
    pre_moves = [e["pre_cum_return"] for e in events_result]
    post_moves = [e["post_cum_return"] for e in events_result]

    stats = {
        "valid_event_count": valid_count,
        "mean_pre_move": round(sum(pre_moves) / len(pre_moves), 4) if pre_moves else None,
        "mean_post_move": round(sum(post_moves) / len(post_moves), 4) if post_moves else None,
        "hit_ratio": round(hit_count / valid_count, 4) if valid_count > 0 else None,
        "hit_ratio_label": f"{hit_count}/{valid_count}" if valid_count > 0 else "0/0",
    }

    x_axis = list(range(-window, window + 1))

    return {
        "pair": pair,
        "window": window,
        "x_axis": x_axis,
        "events": events_result,
        "mean_path": mean_path,
        "median_path": median_path,
        "stats": stats,
        "valid_event_count": valid_count,
    }


def _load_spot_data(pair: str, start: Optional[date], end: Optional[date]) -> pl.DataFrame:
    """FX spot データを fact_market_series_normalized から取得"""
    PAIR_TO_SERIES = {
        "USDJPY": "DEXJPUS",
        "EURUSD": "DEXUSEU",
        "AUDUSD": "DEXUSAL",
    }
    series_id = PAIR_TO_SERIES.get(pair)
    if not series_id:
        return pl.DataFrame()

    conn = get_connection()
    conditions = ["series_id = ?", "value IS NOT NULL"]
    params: list = [series_id]
    if start:
        conditions.append("obs_date >= ?")
        params.append(start)
    if end:
        conditions.append("obs_date <= ?")
        params.append(end)

    rows = conn.execute(
        f"""
        SELECT obs_date, value
        FROM fact_market_series_normalized
        WHERE {' AND '.join(conditions)}
        ORDER BY obs_date, as_of_realtime_start DESC
        """,
        params,
    ).fetchall()

    if not rows:
        return pl.DataFrame()

    # Deduplicate by obs_date (take latest realtime)
    seen = set()
    deduped = []
    for r in rows:
        k = str(r[0])
        if k not in seen:
            seen.add(k)
            deduped.append({"obs_date": r[0], "value": r[1]})

    return pl.DataFrame(deduped)


def _find_nearest_idx(dates_list: list, target: date) -> Optional[int]:
    """target に最も近い日付のインデックスを返す"""
    best_idx = None
    best_diff = None
    for i, d in enumerate(dates_list):
        if d is None:
            continue
        # date型に変換
        if hasattr(d, 'date'):
            d = d.date()
        diff = abs((d - target).days)
        if best_diff is None or diff < best_diff:
            best_diff = diff
            best_idx = i
        if diff == 0:
            break
    # 5日以上離れていたらスキップ
    if best_diff is not None and best_diff > 5:
        return None
    return best_idx
