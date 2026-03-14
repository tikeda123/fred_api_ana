"""
Home ページ
- システム状態
- 主要ペアの最新スポット値 + 日次変化
- 現在のレジームサマリー
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import streamlit as st
import requests
from datetime import date, timedelta
from tz_utils import now_jst_str

API_BASE = "http://localhost:8000/api/v1"

st.set_page_config(
    page_title="FRED FX Research",
    page_icon="📈",
    layout="wide",
)

st.title("FRED FX Research Workbench")
st.caption(f"as of {now_jst_str()}")

# ── ヘルスチェック ─────────────────────────────────
st.subheader("System Status")
try:
    r = requests.get("http://localhost:8000/health", timeout=3)
    h = r.json()
    cols = st.columns(3)
    for col, (svc, status) in zip(cols, h["services"].items()):
        icon = "🟢" if status == "ok" else "🔴"
        col.metric(svc.upper(), f"{icon} {status}")
except Exception:
    st.error("API サーバーに接続できません。`uvicorn app.api.main:app` を起動してください。")
    st.stop()

st.divider()

# ── 主要ペアカード（実際のスポット値 + 変化率） ───────
st.subheader("主要ペア 最新スポット")

SPOT_SERIES = {
    "USDJPY": "DEXJPUS",
    "EURUSD": "DEXUSEU",
    "AUDUSD": "DEXUSAL",
}

@st.cache_data(ttl=300, show_spinner=False)
def fetch_latest_spot(pair: str, series_id: str):
    try:
        payload = {
            "pair": pair,
            "date_start": str(date.today() - timedelta(days=10)),
            "date_end": str(date.today()),
            "features": ["spot"],
        }
        r = requests.post(f"{API_BASE}/panel/build", json=payload, timeout=10)
        records = r.json().get("data", {}).get("records", [])
        valid = [rec for rec in records if rec.get("spot") is not None]
        if len(valid) >= 2:
            latest = valid[-1]
            prev   = valid[-2]
            val  = latest["spot"]
            prev_val = prev["spot"]
            delta = (val - prev_val) / prev_val * 100
            return val, delta, latest["obs_date"]
        elif len(valid) == 1:
            return valid[-1]["spot"], None, valid[-1]["obs_date"]
    except Exception:
        pass
    return None, None, None

cols = st.columns(3)
for col, (pair, sid) in zip(cols, SPOT_SERIES.items()):
    val, delta, obs_date = fetch_latest_spot(pair, sid)
    if val is not None:
        delta_str = f"{delta:+.2f}%" if delta is not None else None
        col.metric(
            label=pair,
            value=f"{val:.4f}" if val < 10 else f"{val:.2f}",
            delta=delta_str,
            help=f"series: {sid} | 観測日: {obs_date}",
        )
    else:
        col.metric(pair, "N/A", help=f"データなし (series: {sid})")

st.divider()

# ── 現在のレジームサマリー ─────────────────────────
st.subheader("現在のレジーム状態")

REGIME_ICONS = {
    "risk_on":        ("🟢", "Risk ON"),
    "risk_off":       ("🔴", "Risk OFF"),
    "carry_positive": ("🟢", "Carry +"),
    "carry_negative": ("🔴", "Carry −"),
    "carry_neutral":  ("⚪", "Carry 中立"),
    "normal":         ("🟢", "カーブ 順"),
    "inverted":       ("🔴", "カーブ 逆転"),
    "flat":           ("🟡", "カーブ フラット"),
    "usd_strong":     ("🔵", "USD 強"),
    "usd_weak":       ("🟡", "USD 弱"),
    "usd_neutral":    ("⚪", "USD 中立"),
}

@st.cache_data(ttl=300, show_spinner=False)
def fetch_current_regime(pair: str):
    try:
        payload = {
            "pair": pair,
            "date_start": str(date.today() - timedelta(days=30)),
            "date_end": str(date.today()),
            "include_stats": False,
            "save": False,
        }
        r = requests.post(f"{API_BASE}/regimes/tag", json=payload, timeout=10)
        rows = r.json().get("data", {}).get("regime_rows", [])
        return rows[-1] if rows else {}
    except Exception:
        return {}

regime_cols = st.columns(3)
for col, pair in zip(regime_cols, ["USDJPY", "EURUSD", "AUDUSD"]):
    col.markdown(f"**{pair}**")
    regime = fetch_current_regime(pair)
    if regime:
        for key in ["risk_state", "carry_state", "curve_state_us", "usd_state"]:
            val = regime.get(key)
            if val:
                icon, label = REGIME_ICONS.get(val, ("⚪", val))
                col.markdown(f"{icon} {key.replace('_', ' ').title()}: **{label}**")
    else:
        col.caption("データなし")

st.divider()
st.info("左サイドバーからページを選択してください。")
