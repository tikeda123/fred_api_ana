"""
Parquet 読み書き (Polars)
- raw    : data/raw/series_id=<ID>/year=<YYYY>/<batch>.parquet
- normalized: data/normalized/domain=<D>/pair=<P>/<series_id>.parquet
- derived   : data/derived/factor_group=<G>/pair=<P>/<factor>.parquet
"""

from pathlib import Path
import polars as pl

from app.core.config import settings
from app.core.exceptions import StorageError
from app.core.logging import get_logger

logger = get_logger(__name__)


# ── raw layer ────────────────────────────────────────────────

def write_raw(df: pl.DataFrame, series_id: str, year: int, batch_id: str) -> Path:
    path = Path(settings.raw_data_root) / f"series_id={series_id}" / f"year={year}"
    path.mkdir(parents=True, exist_ok=True)
    file = path / f"{batch_id}.parquet"
    try:
        df.write_parquet(file)
        logger.info("parquet_raw_written", path=str(file), rows=len(df))
        return file
    except Exception as e:
        raise StorageError(f"raw parquet 書き込み失敗: {e}") from e


def read_raw(series_id: str, year: int | None = None) -> pl.DataFrame:
    base = Path(settings.raw_data_root) / f"series_id={series_id}"
    if year:
        pattern = str(base / f"year={year}" / "*.parquet")
    else:
        pattern = str(base / "**" / "*.parquet")
    try:
        return pl.read_parquet(pattern)
    except Exception as e:
        raise StorageError(f"raw parquet 読み込み失敗 [{series_id}]: {e}") from e


# ── normalized layer ─────────────────────────────────────────

def write_normalized(df: pl.DataFrame, domain: str, pair: str, series_id: str) -> Path:
    path = Path(settings.normalized_data_root) / f"domain={domain}" / f"pair={pair}"
    path.mkdir(parents=True, exist_ok=True)
    file = path / f"{series_id}.parquet"
    try:
        df.write_parquet(file)
        logger.info("parquet_normalized_written", path=str(file), rows=len(df))
        return file
    except Exception as e:
        raise StorageError(f"normalized parquet 書き込み失敗: {e}") from e


def read_normalized(domain: str | None = None, pair: str | None = None) -> pl.DataFrame:
    base = Path(settings.normalized_data_root)
    if domain and pair:
        pattern = str(base / f"domain={domain}" / f"pair={pair}" / "*.parquet")
    elif pair:
        pattern = str(base / "**" / f"pair={pair}" / "*.parquet")
    else:
        pattern = str(base / "**" / "*.parquet")
    try:
        return pl.read_parquet(pattern)
    except Exception:
        return pl.DataFrame()


# ── derived layer ─────────────────────────────────────────────

def write_derived(df: pl.DataFrame, factor_group: str, pair: str, factor_name: str) -> Path:
    path = Path(settings.derived_data_root) / f"factor_group={factor_group}" / f"pair={pair}"
    path.mkdir(parents=True, exist_ok=True)
    file = path / f"{factor_name}.parquet"
    try:
        df.write_parquet(file)
        logger.info("parquet_derived_written", path=str(file), rows=len(df))
        return file
    except Exception as e:
        raise StorageError(f"derived parquet 書き込み失敗: {e}") from e


def read_derived(factor_group: str, pair: str) -> pl.DataFrame:
    pattern = str(
        Path(settings.derived_data_root)
        / f"factor_group={factor_group}"
        / f"pair={pair}"
        / "*.parquet"
    )
    try:
        return pl.read_parquet(pattern)
    except Exception:
        return pl.DataFrame()
