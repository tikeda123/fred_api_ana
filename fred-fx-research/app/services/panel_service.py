"""
PanelService
- pair 用 macro panel の構築・保存
- 欠測率チェック付き
"""

from datetime import date
from typing import Optional

import polars as pl

from app.core.logging import get_logger
from app.storage.parquet_io import write_derived
from app.storage.repositories.panel_repo import build_panel, PAIR_SERIES_MAP

logger = get_logger(__name__)


def get_panel(
    pair: str,
    date_start: date,
    date_end: date,
    frequency: str = "D",
    features: Optional[list[str]] = None,
    vintage_mode: bool = False,
) -> dict:
    """
    panel を構築して dict を返す。
    {
        "pair": ...,
        "panel": pl.DataFrame,
        "missing_rates": {...},
        "row_count": ...,
        "available_features": [...],
    }
    """
    panel, missing_rates = build_panel(
        pair=pair,
        date_start=date_start,
        date_end=date_end,
        features=features,
        vintage_mode=vintage_mode,
    )

    # Parquet に保存
    write_derived(panel, "panel", pair, f"panel_{date_start}_{date_end}")

    return {
        "pair": pair,
        "panel": panel,
        "missing_rates": missing_rates,
        "row_count": len(panel),
        "available_features": [c for c in panel.columns if c != "obs_date"],
        "date_start": date_start,
        "date_end": date_end,
        "frequency": frequency,
        "vintage_mode": vintage_mode,
    }


def get_supported_pairs() -> list[str]:
    return list(PAIR_SERIES_MAP.keys())


def panel_to_records(panel: pl.DataFrame) -> list[dict]:
    """Streamlit / API 向けに list[dict] に変換"""
    return panel.to_dicts()
