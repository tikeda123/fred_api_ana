"""
Validation Service
- CSV バーデータのバリデーション
- high < low, null ts, 重複 ts, 逆順 ts, 異常 gap, 異常 volume 等を検査
"""

from dataclasses import dataclass, field

import polars as pl

from app.core.logging import get_logger

logger = get_logger(__name__)


@dataclass
class QualityFlag:
    row_number: int
    reason: str
    details: str = ""


@dataclass
class ValidationResult:
    is_valid: bool
    total_rows: int
    valid_rows: int
    rejected_rows: int
    flags: list[QualityFlag] = field(default_factory=list)

    def summary(self) -> dict:
        return {
            "is_valid": self.is_valid,
            "total_rows": self.total_rows,
            "valid_rows": self.valid_rows,
            "rejected_rows": self.rejected_rows,
            "flag_count": len(self.flags),
            "reasons": list({f.reason for f in self.flags}),
        }


# 異常 gap の閾値（前バー比の絶対変化率）
PRICE_GAP_THRESHOLD = 0.20   # 20%
VOLUME_QUANTILE    = 0.999   # 上位 0.1% を異常とみなす


def validate_bars(df: pl.DataFrame) -> ValidationResult:
    """
    OHLC バーデータのバリデーション。

    Args:
        df: ts_utc, open, high, low, close, volume 列を持つ DataFrame

    Returns:
        ValidationResult
    """
    flags: list[QualityFlag] = []
    bad_rows: set[int] = set()

    total = len(df)

    # ── 1. ts_utc null ────────────────────────────────────────────
    null_ts = df.filter(pl.col("ts_utc").is_null())
    for i in null_ts.to_dicts():
        idx = _find_row_index(df, i)
        flags.append(QualityFlag(row_number=idx, reason="ts_null", details="ts_utc is null"))
        bad_rows.add(idx)

    # null を除外して以降の検査を続行
    df_clean = df.filter(pl.col("ts_utc").is_not_null())

    # ── 2. high < low ─────────────────────────────────────────────
    hl = df_clean.with_row_index("_idx").filter(pl.col("high") < pl.col("low"))
    for row in hl.iter_rows(named=True):
        flags.append(QualityFlag(
            row_number=row["_idx"],
            reason="high_lt_low",
            details=f"high={row['high']}, low={row['low']}",
        ))
        bad_rows.add(row["_idx"])

    # ── 3. price <= 0 ─────────────────────────────────────────────
    for col in ("open", "high", "low", "close"):
        inv = df_clean.with_row_index("_idx").filter(pl.col(col) <= 0)
        for row in inv.iter_rows(named=True):
            flags.append(QualityFlag(
                row_number=row["_idx"],
                reason="zero_price",
                details=f"{col}={row[col]}",
            ))
            bad_rows.add(row["_idx"])

    # ── 4. 重複 timestamp ─────────────────────────────────────────
    dup_ts = (
        df_clean
        .with_row_index("_idx")
        .group_by("ts_utc")
        .agg(pl.col("_idx").count().alias("cnt"), pl.col("_idx").first().alias("first_idx"))
        .filter(pl.col("cnt") > 1)
    )
    for row in dup_ts.iter_rows(named=True):
        flags.append(QualityFlag(
            row_number=row["first_idx"],
            reason="duplicate_ts",
            details=f"ts_utc={row['ts_utc']} appears {row['cnt']} times",
        ))
        bad_rows.add(row["first_idx"])

    # ── 5. 逆順 timestamp ─────────────────────────────────────────
    sorted_ts = df_clean.sort("ts_utc")
    if not df_clean["ts_utc"].to_list() == sorted_ts["ts_utc"].to_list():
        # 逆順箇所を特定
        ts_list = df_clean["ts_utc"].to_list()
        for i in range(1, len(ts_list)):
            if ts_list[i] < ts_list[i - 1]:
                flags.append(QualityFlag(
                    row_number=i,
                    reason="reverse_order",
                    details=f"ts[{i}]={ts_list[i]} < ts[{i-1}]={ts_list[i-1]}",
                ))
                bad_rows.add(i)

    # ── 6. 異常 gap (close の前バー比 ±20% 超) ───────────────────
    df_sorted = df_clean.sort("ts_utc").with_row_index("_idx")
    prev_close = df_sorted["close"].shift(1)
    gap = ((df_sorted["close"] - prev_close) / prev_close).abs()
    gap_mask = gap > PRICE_GAP_THRESHOLD
    for i, (is_gap, g_val) in enumerate(zip(gap_mask.to_list(), gap.to_list())):
        if i == 0:
            continue  # 先頭行はスキップ
        if is_gap and g_val is not None:
            flags.append(QualityFlag(
                row_number=i,
                reason="abnormal_gap",
                details=f"price gap={g_val:.2%}",
            ))
            # gap は警告のみ（bad_rows には追加しない）

    # ── 7. 異常 volume (負値 / 上位 0.1% 超) ────────────────────
    if "volume" in df_clean.columns:
        neg_vol = df_clean.with_row_index("_idx").filter(
            pl.col("volume").is_not_null() & (pl.col("volume") < 0)
        )
        for row in neg_vol.iter_rows(named=True):
            flags.append(QualityFlag(
                row_number=row["_idx"],
                reason="negative_volume",
                details=f"volume={row['volume']}",
            ))
            bad_rows.add(row["_idx"])

        vols = df_clean["volume"].drop_nulls()
        if len(vols) > 0:
            vol_threshold = vols.quantile(VOLUME_QUANTILE)
            extreme_vol = df_clean.with_row_index("_idx").filter(
                pl.col("volume").is_not_null() & (pl.col("volume") > vol_threshold)
            )
            for row in extreme_vol.iter_rows(named=True):
                flags.append(QualityFlag(
                    row_number=row["_idx"],
                    reason="extreme_volume",
                    details=f"volume={row['volume']} > p99.9={vol_threshold:.0f}",
                ))
                # 警告のみ

    rejected = len(bad_rows)
    valid = total - rejected
    # 検査フラグがあっても warning のみ（bad_rows 未追加）なら is_valid=True
    is_valid = rejected == 0

    result = ValidationResult(
        is_valid=is_valid,
        total_rows=total,
        valid_rows=valid,
        rejected_rows=rejected,
        flags=flags,
    )

    logger.info(
        "validation_done",
        total=total,
        valid=valid,
        rejected=rejected,
        flag_count=len(flags),
    )
    return result


def _find_row_index(df: pl.DataFrame, row_dict: dict) -> int:
    """null を含む行の index を返す（null ts 用）"""
    null_mask = df["ts_utc"].is_null()
    idxs = null_mask.arg_true().to_list()
    return idxs[0] if idxs else 0
