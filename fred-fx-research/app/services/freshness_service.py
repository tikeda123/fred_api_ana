"""
FreshnessService
- series メタデータから鮮度を判定
- ok / warning / reject を返す
"""

from datetime import date, timedelta
from typing import Optional

from app.core.logging import get_logger
from app.models.domain_models import SeriesMetadata
from app.storage.duckdb import get_connection
from app.storage.repositories import registry_repo

logger = get_logger(__name__)

FREQ_WARN_DAYS = {
    "D":  10,   # 日次: 10営業日
    "W":  21,   # 週次: 3週
    "M":  120,  # 月次: 4ヶ月
    "Q":  200,  # 四半期: 6ヶ月超
    "A":  400,  # 年次
}

DISCONTINUED_KEYWORDS = ["discontinued", "no longer", "superseded", "replaced"]


def audit(meta: SeriesMetadata) -> dict:
    """
    1件の series を監査して結果を返す。
    returns: {"series_id", "status", "reasons"}
    """
    reasons: list[str] = []
    status = "ok"

    # ── discontinued チェック ─────────────────────────────
    notes_lower = (meta.notes or "").lower()
    if any(kw in notes_lower for kw in DISCONTINUED_KEYWORDS):
        reasons.append("discontinued or superseded series")
        status = "reject"

    # ── observation_end の鮮度チェック ─────────────────────
    if meta.observation_end:
        today = date.today()
        age_days = (today - meta.observation_end).days
        freq = _normalize_freq(meta.frequency_native)
        threshold = FREQ_WARN_DAYS.get(freq, 120)
        if age_days > threshold:
            reasons.append(
                f"stale: last obs {meta.observation_end} ({age_days}d ago, threshold={threshold}d)"
            )
            if status == "ok":
                status = "warning"

    # ── 四半期/年次を日次モデルへ直接入力チェック ──────────
    freq = _normalize_freq(meta.frequency_native)
    if freq in ("Q", "A") and meta.domain not in (None, ""):
        reasons.append(f"low frequency ({freq}) — not suitable as primary daily factor")
        if status == "ok":
            status = "warning"

    return {
        "series_id": meta.series_id,
        "status": status,
        "reasons": reasons,
        "observation_end": str(meta.observation_end) if meta.observation_end else None,
        "frequency": meta.frequency_native,
        "domain": meta.domain,
    }


def audit_all() -> list[dict]:
    """registry 全 series を監査して結果リストを返す"""
    conn = get_connection()
    rows = conn.execute("SELECT series_id FROM dim_series_registry").fetchall()
    results = []
    for (sid,) in rows:
        meta = registry_repo.get(sid)
        if meta:
            result = audit(meta)
            # freshness_status を registry に書き戻す
            conn.execute(
                "UPDATE dim_series_registry SET freshness_status = ? WHERE series_id = ?",
                [result["status"], sid],
            )
            results.append(result)
    logger.info("freshness_audit_complete", total=len(results))
    return results


def get_stale_series() -> list[dict]:
    results = audit_all()
    return [r for r in results if r["status"] != "ok"]


def _normalize_freq(freq_str: Optional[str]) -> str:
    if not freq_str:
        return "M"
    f = freq_str.upper().strip()
    if f.startswith("D"):
        return "D"
    if f.startswith("W"):
        return "W"
    if f.startswith("M"):
        return "M"
    if f.startswith("Q"):
        return "Q"
    if f.startswith("A") or f.startswith("Y"):
        return "A"
    return "M"
