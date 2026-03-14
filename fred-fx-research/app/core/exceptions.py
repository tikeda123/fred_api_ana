class FredApiError(Exception):
    """FRED API リクエスト失敗"""
    def __init__(self, message: str, status_code: int | None = None):
        super().__init__(message)
        self.status_code = status_code


class FredRateLimitError(FredApiError):
    """429 Too Many Requests"""


class FredSeriesNotFoundError(FredApiError):
    """指定 series_id が存在しない"""


class StorageError(Exception):
    """DuckDB / Parquet 操作失敗"""


class NormalizationError(Exception):
    """正規化処理失敗"""


class FreshnessRejectError(Exception):
    """鮮度監査で reject 判定"""
