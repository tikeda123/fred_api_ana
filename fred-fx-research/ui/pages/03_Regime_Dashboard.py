"""
Regime Dashboard
- risk on/off タグ時系列
- carry favorable/unfavorable 区間
- curve inversion marker
- regime 別平均リターン表
- regime transition summary
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import streamlit as st
import requests
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import date, timedelta
from tz_utils import now_jst_str

API_BASE = "http://localhost:8000/api/v1"

st.set_page_config(page_title="Regime Dashboard", layout="wide")
st.title("Regime Dashboard")

# ── サイドバー ─────────────────────────────────────
with st.sidebar:
    st.header("設定")
    pair = st.selectbox("通貨ペア", ["USDJPY", "EURUSD", "AUDUSD"])
    date_start = st.date_input("開始日", value=date.today() - timedelta(days=365 * 3))
    date_end   = st.date_input("終了日", value=date.today())
    include_stats = st.checkbox("regime 別統計を計算", value=True)
    run = st.button("レジーム分析", type="primary", use_container_width=True)

if not run:
    st.info("サイドバーで設定し「レジーム分析」を押してください。")
    st.stop()

# ── レジーム取得 ───────────────────────────────────
@st.cache_data(ttl=300, show_spinner="レジームを計算中...")
def fetch_regimes(pair, date_start, date_end, include_stats):
    payload = {
        "pair": pair,
        "date_start": str(date_start),
        "date_end": str(date_end),
        "include_stats": include_stats,
        "save": True,
    }
    r = requests.post(f"{API_BASE}/regimes/tag", json=payload, timeout=30)
    return r.json()

result = fetch_regimes(pair, date_start, date_end, include_stats)

if result.get("errors"):
    for e in result["errors"]:
        st.error(e)
    st.stop()

for w in result.get("warnings", []):
    st.warning(w)

data = result.get("data", {})
regime_rows = data.get("regime_rows", [])

if not regime_rows:
    st.warning("レジームデータがありません。先にデータを取り込んでください。")
    st.stop()

df = pd.DataFrame(regime_rows)
df["obs_date"] = pd.to_datetime(df["obs_date"])
df = df.set_index("obs_date")

# ── 現在のレジームサマリーカード ───────────────────
st.subheader("現在のレジーム状態")
latest = df.iloc[-1]
latest_date = df.index[-1].date()
st.caption(f"最終更新: {latest_date} ({now_jst_str()})")

REGIME_DISPLAY = {
    "risk_state":     {"label": "Risk State",   "icons": {"risk_on": "🟢 Risk ON", "risk_off": "🔴 Risk OFF"}},
    "carry_state":    {"label": "Carry State",  "icons": {"carry_positive": "🟢 Carry+", "carry_negative": "🔴 Carry−", "carry_neutral": "⚪ 中立"}},
    "curve_state_us": {"label": "US Curve",     "icons": {"normal": "🟢 順イールド", "inverted": "🔴 逆転", "flat": "🟡 フラット"}},
    "usd_state":      {"label": "USD State",    "icons": {"usd_strong": "🔵 USD強", "usd_weak": "🟡 USD弱", "usd_neutral": "⚪ 中立"}},
}
card_cols = st.columns(4)
for col, (state_key, cfg) in zip(card_cols, REGIME_DISPLAY.items()):
    val = latest.get(state_key, "N/A")
    display = cfg["icons"].get(val, f"⚪ {val}")
    col.metric(cfg["label"], display)

st.divider()

# ── Spot + regime overlay チャート ─────────────────
st.subheader(f"{pair} Spot × Regime Overlay")

# spot を取得
@st.cache_data(ttl=300)
def fetch_panel_spot(pair, date_start, date_end):
    payload = {"pair": pair, "date_start": str(date_start), "date_end": str(date_end), "features": ["spot", "vix", "usd_broad"]}
    r = requests.post(f"{API_BASE}/panel/build", json=payload, timeout=30)
    return r.json()

@st.cache_data(ttl=300, show_spinner=False)
def fetch_ohlc_close(pair, date_start, date_end):
    """OHLC 日足 close を取得してスポット欠損の補完に使う"""
    try:
        r = requests.get(
            f"{API_BASE}/ohlc/daily/{pair}",
            params={"start_date": str(date_start), "end_date": str(date_end), "limit": 5000},
            timeout=10,
        )
        records = r.json().get("data", {}).get("records", [])
        if records:
            odf = pd.DataFrame(records)
            odf["obs_date"] = pd.to_datetime(odf["obs_date"])
            return odf.set_index("obs_date")[["close"]]
    except Exception:
        pass
    return pd.DataFrame()

panel_result = fetch_panel_spot(pair, date_start, date_end)
panel_records = panel_result.get("data", {}).get("records", [])
ohlc_close = fetch_ohlc_close(pair, date_start, date_end)

if panel_records:
    pdf = pd.DataFrame(panel_records)
    pdf["obs_date"] = pd.to_datetime(pdf["obs_date"])
    pdf = pdf.set_index("obs_date")
    merged = pdf.join(df, how="left")
    # OHLC close でスポット欠損を補完
    if not ohlc_close.empty:
        if "spot" in merged.columns:
            merged["spot"] = merged["spot"].fillna(ohlc_close["close"])
        else:
            merged = merged.join(ohlc_close.rename(columns={"close": "spot"}), how="left")
else:
    merged = df.copy()
    if not ohlc_close.empty:
        merged = merged.join(ohlc_close.rename(columns={"close": "spot"}), how="left")

STATE_COLORS = {
    "risk_on":  "rgba(22,163,74,0.15)",
    "risk_off": "rgba(220,38,38,0.15)",
    "usd_strong": "rgba(37,99,235,0.15)",
    "usd_weak":   "rgba(234,179,8,0.15)",
    "usd_neutral":"rgba(150,150,150,0.05)",
    "carry_positive": "rgba(22,163,74,0.15)",
    "carry_negative": "rgba(220,38,38,0.15)",
    "carry_neutral":  "rgba(150,150,150,0.05)",
    "normal":   "rgba(22,163,74,0.10)",
    "inverted": "rgba(220,38,38,0.20)",
    "flat":     "rgba(234,179,8,0.10)",
}

tab1, tab2, tab3, tab4 = st.tabs(["Risk State", "Carry State", "Curve State", "USD State"])

for tab, state_col, title in [
    (tab1, "risk_state",    "Risk On / Off"),
    (tab2, "carry_state",   "Carry State"),
    (tab3, "curve_state_us","US Curve State"),
    (tab4, "usd_state",     "USD Broad State"),
]:
    with tab:
        if state_col not in merged.columns:
            st.info(f"{state_col} データなし")
            continue

        fig = go.Figure()

        # Spot line
        if "spot" in merged.columns:
            fig.add_trace(go.Scatter(
                x=merged.index, y=merged["spot"],
                name=pair, line=dict(color="#1e40af", width=1.5)
            ))

        # Regime band（状態が変わる区間ごとに rectangle を描画）
        prev_state = None
        seg_start = None
        for dt, row in merged[[state_col]].iterrows():
            state = row[state_col]
            if state != prev_state:
                if prev_state is not None and seg_start is not None:
                    color = STATE_COLORS.get(prev_state, "rgba(150,150,150,0.1)")
                    fig.add_vrect(x0=seg_start, x1=dt, fillcolor=color, layer="below",
                                  line_width=0, annotation_text=prev_state if prev_state not in ("risk_on","usd_neutral","carry_neutral","normal") else "",
                                  annotation_position="top left")
                seg_start = dt
                prev_state = state

        fig.update_layout(
            title=f"{pair} × {title}",
            height=350,
            hovermode="x unified",
            margin=dict(l=0, r=0, t=40, b=0),
            legend=dict(orientation="h"),
        )
        st.plotly_chart(fig, use_container_width=True)

# ── Regime 別統計 ──────────────────────────────────
st.divider()
st.subheader("Regime 別 平均リターン統計")
stats = data.get("regime_stats", {})

if stats:
    cols = st.columns(len(stats))
    for col, (state_col, rows) in zip(cols, stats.items()):
        col.markdown(f"**{state_col}**")
        sdf = pd.DataFrame(rows)
        if not sdf.empty:
            sdf["mean_return"] = sdf["mean_return"].map(lambda x: f"{x*100:.3f}%" if x is not None else "N/A")
            sdf["std_return"]  = sdf["std_return"].map(lambda x: f"{x*100:.3f}%" if x is not None else "N/A")
            col.dataframe(sdf, hide_index=True, use_container_width=True)

# ── Regime Transition Summary ──────────────────────
st.divider()
st.subheader("Regime Transition Summary")

for state_col in ["risk_state", "carry_state", "curve_state_us"]:
    if state_col not in df.columns:
        continue
    counts = df[state_col].value_counts()
    total = counts.sum()
    rows_disp = [{"state": s, "days": int(c), "ratio": f"{c/total:.1%}"} for s, c in counts.items()]
    st.markdown(f"**{state_col}**")
    st.dataframe(pd.DataFrame(rows_disp), hide_index=True, use_container_width=False)

# ── Raw table ──────────────────────────────────────
with st.expander("Raw Regime Data"):
    st.dataframe(df, use_container_width=True)
    csv = df.to_csv()
    st.download_button("CSV ダウンロード", csv, f"{pair}_regimes.csv", "text/csv")
