from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Optional


@dataclass
class SeriesMetadata:
    series_id: str
    title: str
    frequency_native: str
    units_native: str
    seasonal_adjustment: str
    observation_start: Optional[date]
    observation_end: Optional[date]
    last_updated: Optional[datetime]
    source_name: Optional[str] = None
    release_id: Optional[int] = None
    release_name: Optional[str] = None
    notes: Optional[str] = None
    domain: Optional[str] = None
    base_ccy: Optional[str] = None
    quote_ccy: Optional[str] = None
    pair: Optional[str] = None
    quote_direction: Optional[str] = None
    freshness_status: str = "unknown"


@dataclass
class RawObservation:
    series_id: str
    date: date
    value_raw: str
    value_num: Optional[float]
    realtime_start: date
    realtime_end: date
    retrieved_at_utc: datetime
    file_batch_id: str
    source_last_updated: Optional[datetime] = None


@dataclass
class VintageDate:
    series_id: str
    vintage_date: date
    retrieved_at_utc: datetime


@dataclass
class ReleaseCalendarEntry:
    release_id: int
    release_name: str
    release_date: date
    press_release: Optional[bool]
    link: Optional[str]
    retrieved_at_utc: datetime


# FX series の canonical マッピング
FX_SERIES_MAP: dict[str, dict] = {
    "DEXJPUS": {
        "pair": "USDJPY",
        "base_ccy": "USD",
        "quote_ccy": "JPY",
        "quote_direction": "quote_per_base",
        "domain": "fx_spot",
    },
    "DEXUSEU": {
        "pair": "EURUSD",
        "base_ccy": "EUR",
        "quote_ccy": "USD",
        "quote_direction": "quote_per_base",
        "domain": "fx_spot",
    },
    "DEXUSAL": {
        "pair": "AUDUSD",
        "base_ccy": "AUD",
        "quote_ccy": "USD",
        "quote_direction": "quote_per_base",
        "domain": "fx_spot",
    },
    "DTWEXBGS": {
        "pair": "USD_BROAD",
        "base_ccy": "USD",
        "quote_ccy": None,
        "quote_direction": "index",
        "domain": "fx_broad",
    },
    # 米国金利
    "EFFR":           {"domain": "policy_rate", "base_ccy": "USD"},
    "FEDFUNDS":       {"domain": "policy_rate", "base_ccy": "USD"},
    # ECB
    "ECBDFR":         {"domain": "policy_rate", "base_ccy": "EUR"},
    "ECBMRRFR":       {"domain": "policy_rate", "base_ccy": "EUR"},
    # 日本
    "IRSTCI01JPM156N": {"domain": "policy_rate", "base_ccy": "JPY"},
    # 豪州
    "IRSTCI01AUM156N": {"domain": "policy_rate", "base_ccy": "AUD"},
    # 3M
    "IR3TIB01USM156N": {"domain": "rate_3m", "base_ccy": "USD"},
    "IR3TIB01JPM156N": {"domain": "rate_3m", "base_ccy": "JPY"},
    "IR3TIB01EZM156N": {"domain": "rate_3m", "base_ccy": "EUR"},
    "IR3TIB01AUM156N": {"domain": "rate_3m", "base_ccy": "AUD"},
    # 10Y
    "IRLTLT01USM156N": {"domain": "yield_10y", "base_ccy": "USD"},
    "IRLTLT01JPM156N": {"domain": "yield_10y", "base_ccy": "JPY"},
    "IRLTLT01EZM156N": {"domain": "yield_10y", "base_ccy": "EUR"},
    "IRLTLT01AUM156N": {"domain": "yield_10y", "base_ccy": "AUD"},
    # リスク・商品
    "VIXCLS":         {"domain": "risk"},
    "STLFSI4":        {"domain": "risk"},
    "DCOILWTICO":     {"domain": "commodity"},
    # BoJ バランスシート
    "JPNASSETS":      {"domain": "balance_sheet", "base_ccy": "JPY"},
    # BIS 実質実効為替レート (月次)
    "RBUSBIS":        {"domain": "reer", "base_ccy": "USD"},
    "RBJPBIS":        {"domain": "reer", "base_ccy": "JPY"},
    "RBAUBIS":        {"domain": "reer", "base_ccy": "AUD"},
    "RBXMBIS":        {"domain": "reer", "base_ccy": "EUR"},
}
