from datetime import date, datetime
from typing import Any, Literal, Optional
from pydantic import BaseModel, Field


# --- 共通 envelope ---

class ApiResponse(BaseModel):
    status: str = "ok"
    as_of: datetime = Field(default_factory=datetime.utcnow)
    timezone: str = "UTC"
    data: Any = None
    warnings: list[str] = []
    errors: list[str] = []


# --- Series Search ---

class SeriesSearchResult(BaseModel):
    series_id: str
    title: str
    frequency: str
    units: str
    observation_start: Optional[date]
    observation_end: Optional[date]
    last_updated: Optional[datetime]
    domain: Optional[str]
    freshness_status: Optional[str]


# --- Observations fetch ---

class ObservationsFetchRequest(BaseModel):
    series_id: str
    observation_start: Optional[date] = None
    observation_end: Optional[date] = None
    units: str = "lin"
    frequency: Optional[str] = None
    aggregation_method: str = "eop"
    realtime_start: Optional[date] = None
    realtime_end: Optional[date] = None
    vintage_dates: Optional[list[date]] = None
    store_raw: bool = True
    normalize: bool = False


class ObservationRecord(BaseModel):
    date: date
    value_raw: str
    value_num: Optional[float]
    realtime_start: date
    realtime_end: date


class ObservationsFetchResponse(BaseModel):
    series_id: str
    count: int
    observations: list[ObservationRecord]
    retrieved_at_utc: datetime
    batch_id: str


# --- Health ---

class ServiceStatus(BaseModel):
    duckdb: str
    parquet: str
    fred: str


class HealthResponse(BaseModel):
    status: str
    services: ServiceStatus


# --- Market Uploads ---

class MarketUploadAccepted(BaseModel):
    upload_id: str
    instrument_id: str
    status: Literal["accepted", "processing", "loaded", "failed"]
    next: str


class UploadStatusResponse(BaseModel):
    upload_id: str
    instrument_id: str
    status: str
    row_count_detected: Optional[int] = None
    row_count_loaded: Optional[int] = None
    row_count_rejected: Optional[int] = None
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    error_message: Optional[str] = None


# --- Market Bars ---

class DailyRebuildRequest(BaseModel):
    instrument_id: str
    start: Optional[date] = None
    end: Optional[date] = None


# --- Cross Asset Features ---

class CrossAssetFeatureBuildRequest(BaseModel):
    source_instrument_id: str
    pairs: list[str]
    start: Optional[date] = None
    end: Optional[date] = None
    feature_set: list[str] = [
        "ret_1d", "mom_5d", "mom_20d",
        "rv_5d", "rv_20d", "drawdown_20d",
        "corr_20d", "beta_60d",
        "lagcorr_l1_20d", "lagcorr_l2_20d",
    ]


class FxCrossAssetPanelRow(BaseModel):
    pair: str
    obs_date: date
    pair_close: Optional[float] = None
    pair_ret_1d: Optional[float] = None
    usatech_close: Optional[float] = None
    usatech_ret_1d: Optional[float] = None
    usatech_mom_5d: Optional[float] = None
    usatech_mom_20d: Optional[float] = None
    usatech_rv_5d: Optional[float] = None
    usatech_rv_20d: Optional[float] = None
    usatech_drawdown_20d: Optional[float] = None
    usatech_range_pct_1d: Optional[float] = None
    vix_close: Optional[float] = None
    usd_broad_close: Optional[float] = None
    rate_spread_3m: Optional[float] = None
    yield_spread_10y: Optional[float] = None
    event_risk_flag: Optional[str] = None
    regime_label: Optional[str] = None


# --- Rolling / Event / Filter requests ---

class RollingStatsRequest(BaseModel):
    pair: str
    start: date
    end: date
    corr_window: int = 20
    beta_window: int = 60


class EventAnalyzeRequest(BaseModel):
    pair: str
    event_dates: list[date]
    window: int = 5
    start: Optional[date] = None
    end: Optional[date] = None


class FilterThresholds(BaseModel):
    drawdown_pct: float = -8.0
    rv_percentile: int = 80
    vix_percentile: int = 80
    usd_z: float = 1.0
    min_conditions: int = 2


class FilterEvaluateRequest(BaseModel):
    pair: str
    start: date
    end: date
    thresholds: FilterThresholds = FilterThresholds()


class FilterSimulatePnlRequest(BaseModel):
    pair: str
    start: date
    end: date
    thresholds: FilterThresholds = FilterThresholds()
