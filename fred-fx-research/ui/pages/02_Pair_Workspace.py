"""
Pair Workspace
- USDJPY / EURUSD / AUDUSD を中心とした中核分析画面
- spot / 政策金利スプレッド / 市場金利差 / VIX+USD overlay
- rolling correlation
- missing data summary
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import numpy as np
import streamlit as st
import requests
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import date, timedelta

API_BASE = "http://localhost:8000/api/v1"

st.set_page_config(page_title="Pair Workspace", layout="wide")
st.title("Pair Workspace")

# ── サイドバー ─────────────────────────────────────
with st.sidebar:
    st.header("設定")
    pair = st.selectbox("通貨ペア", ["USDJPY", "EURUSD", "AUDUSD"])
    date_start = st.date_input("開始日", value=date.today() - timedelta(days=365 * 3))
    date_end = st.date_input("終了日", value=date.today())
    frequency = st.selectbox("頻度", ["D", "W", "M"], index=0)
    vintage_mode = st.toggle("Vintage モード", value=False)

    # pair別 全 features（v1.1: 政策金利・BIS REER 追加）
    all_features = {
        "USDJPY": ["spot", "us_policy", "jp_policy", "us_3m", "jp_3m",
                   "us_10y", "jp_10y", "vix", "usd_broad", "boj_assets",
                   "us_reer", "jp_reer"],
        "EURUSD": ["spot", "us_policy", "ez_policy", "us_3m", "ez_3m",
                   "us_10y", "ez_10y", "vix", "usd_broad",
                   "us_reer", "ez_reer"],
        "AUDUSD": ["spot", "us_policy", "au_policy", "au_3m", "us_3m",
                   "au_10y", "us_10y", "vix", "usd_broad", "oil",
                   "us_reer", "au_reer"],
    }
    default_features = {
        "USDJPY": ["spot", "us_policy", "jp_policy", "us_3m", "jp_3m", "us_10y", "jp_10y", "vix", "usd_broad"],
        "EURUSD": ["spot", "us_policy", "ez_policy", "us_3m", "ez_3m", "us_10y", "ez_10y", "vix", "usd_broad"],
        "AUDUSD": ["spot", "us_policy", "au_policy", "au_3m", "us_3m", "au_10y", "us_10y", "vix", "usd_broad", "oil"],
    }
    features = st.multiselect("Features", all_features[pair], default=default_features[pair])

    run = st.button("パネル構築", type="primary", use_container_width=True)

if not run:
    st.info("サイドバーで設定を行い、「パネル構築」を押してください。")
    st.stop()

# ── パネル取得 ─────────────────────────────────────
@st.cache_data(ttl=300, show_spinner="パネルを構築中...")
def fetch_panel(pair, date_start, date_end, frequency, features, vintage_mode):
    payload = {
        "pair": pair,
        "date_start": str(date_start),
        "date_end": str(date_end),
        "frequency": frequency,
        "features": features,
        "vintage_mode": vintage_mode,
    }
    r = requests.post(f"{API_BASE}/panel/build", json=payload, timeout=30)
    return r.json()

result = fetch_panel(pair, date_start, date_end, frequency, tuple(features), vintage_mode)

if result.get("errors"):
    for e in result["errors"]:
        st.error(e)
    st.stop()

for w in result.get("warnings", []):
    st.warning(w)

panel_data = result.get("data", {})
records = panel_data.get("records", [])
if not records:
    st.warning("データがありません。先に /observations/fetch でデータを取り込んでください。")
    st.stop()

df = pd.DataFrame(records)
df["obs_date"] = pd.to_datetime(df["obs_date"])
df = df.set_index("obs_date")

# ── OHLC 日足を取得してファクターをインライン計算 ──────
@st.cache_data(ttl=300, show_spinner="OHLC データを取得中...")
def fetch_ohlc_daily(pair, date_start, date_end):
    try:
        r = requests.get(
            f"{API_BASE}/ohlc/daily/{pair}",
            params={"start_date": str(date_start), "end_date": str(date_end), "limit": 5000},
            timeout=10,
        )
        records = r.json().get("data", {}).get("records", [])
        if records:
            return pd.DataFrame(records)
    except Exception:
        pass
    return pd.DataFrame()

ohlc_raw = fetch_ohlc_daily(pair, date_start, date_end)
has_ohlc_factors = False
ohlc_df = pd.DataFrame()

if not ohlc_raw.empty and "obs_date" in ohlc_raw.columns:
    ohlc_df = ohlc_raw.copy()
    ohlc_df["obs_date"] = pd.to_datetime(ohlc_df["obs_date"])
    ohlc_df = ohlc_df.set_index("obs_date").sort_index()

    # OHLC ファクターをインライン計算
    ohlc_df["daily_range_pct"] = (ohlc_df["high"] - ohlc_df["low"]) / ohlc_df["open"] * 100
    ohlc_df["daily_range_ma20"] = ohlc_df["daily_range_pct"].rolling(20).mean()
    ohlc_df["overnight_gap"] = ohlc_df["open"] / ohlc_df["close"].shift(1) - 1
    _hl_log_sq = np.log(ohlc_df["high"] / ohlc_df["low"]) ** 2
    ohlc_df["parkinson_vol_20d"] = np.sqrt(_hl_log_sq.rolling(20).mean() / (4 * np.log(2)))

    has_ohlc_factors = True

    # spot が欠損している日を OHLC close で補完
    if "spot" in df.columns:
        df["spot"] = df["spot"].fillna(ohlc_df["close"])

    # OHLC ファクター列をメインパネルにマージ
    for c in ["parkinson_vol_20d", "daily_range_pct", "daily_range_ma20", "overnight_gap"]:
        df[c] = ohlc_df[c]

# ── チャート ───────────────────────────────────────
PAIR_RATE_COLS = {
    "USDJPY": {"policy": ("us_policy", "jp_policy"), "short": ("us_3m", "jp_3m"), "long": ("us_10y", "jp_10y")},
    "EURUSD": {"policy": ("us_policy", "ez_policy"), "short": ("us_3m", "ez_3m"), "long": ("us_10y", "ez_10y")},
    "AUDUSD": {"policy": ("us_policy", "au_policy"), "short": ("au_3m", "us_3m"), "long": ("au_10y", "us_10y")},
}
rate_cols = PAIR_RATE_COLS[pair]
pol_a, pol_b     = rate_cols["policy"]
short_a, short_b = rate_cols["short"]
long_a, long_b   = rate_cols["long"]

has_policy = pol_a in df.columns and pol_b in df.columns
has_short  = short_a in df.columns and short_b in df.columns
has_long   = long_a  in df.columns and long_b  in df.columns
has_vix    = "vix" in df.columns
has_usd    = "usd_broad" in df.columns
has_vol_panel   = has_ohlc_factors and "parkinson_vol_20d" in df.columns
has_range_panel = has_ohlc_factors and "daily_range_pct" in df.columns

subplot_titles = [f"{pair} Spot"]
if has_policy: subplot_titles.append("Policy Rate Spread")
if has_short:  subplot_titles.append("Short Rate Spread (3M)")
if has_long:   subplot_titles.append("Long Rate Spread (10Y)")
if has_vix or has_usd: subplot_titles.append("VIX / USD Broad")
if has_vol_panel: subplot_titles.append("Volatility: Parkinson vs Realized")
if has_range_panel: subplot_titles.append("Daily Range / Overnight Gap")

# secondary_y for Range/Gap panel
specs = [[{"secondary_y": False}]] * len(subplot_titles)
if has_range_panel:
    specs[-1] = [{"secondary_y": True}]

rows_count = len(subplot_titles)
fig = make_subplots(
    rows=rows_count, cols=1,
    shared_xaxes=True,
    subplot_titles=subplot_titles,
    vertical_spacing=0.04,
    specs=specs,
)

row_n = 1
# Spot
if "spot" in df.columns:
    fig.add_trace(go.Scatter(
        x=df.index, y=df["spot"],
        name=pair, line=dict(color="#2563EB", width=1.5)
    ), row=row_n, col=1)
row_n += 1

# Policy rate spread
if has_policy:
    spread_p = df[pol_a] - df[pol_b]
    fig.add_trace(go.Scatter(
        x=df.index, y=spread_p,
        name=f"Policy: {pol_a}-{pol_b}", line=dict(color="#0EA5E9", width=1.5)
    ), row=row_n, col=1)
    fig.add_hline(y=0, line_dash="dash", line_color="gray", row=row_n, col=1)
    row_n += 1

# Short rate spread
if has_short:
    spread_s = df[short_a] - df[short_b]
    fig.add_trace(go.Scatter(
        x=df.index, y=spread_s,
        name=f"3M: {short_a}-{short_b}", line=dict(color="#16A34A")
    ), row=row_n, col=1)
    fig.add_hline(y=0, line_dash="dash", line_color="gray", row=row_n, col=1)
    row_n += 1

# Long rate spread
if has_long:
    spread_l = df[long_a] - df[long_b]
    fig.add_trace(go.Scatter(
        x=df.index, y=spread_l,
        name=f"10Y: {long_a}-{long_b}", line=dict(color="#D97706")
    ), row=row_n, col=1)
    fig.add_hline(y=0, line_dash="dash", line_color="gray", row=row_n, col=1)
    row_n += 1

# VIX / USD
if has_vix or has_usd:
    if has_vix:
        fig.add_trace(go.Scatter(
            x=df.index, y=df["vix"],
            name="VIX", line=dict(color="#DC2626")
        ), row=row_n, col=1)
    if has_usd:
        fig.add_trace(go.Scatter(
            x=df.index, y=df["usd_broad"],
            name="USD Broad", line=dict(color="#7C3AED")
        ), row=row_n, col=1)
    row_n += 1

# Volatility: Parkinson vs Realized
if has_vol_panel:
    fig.add_trace(go.Scatter(
        x=df.index, y=df["parkinson_vol_20d"],
        name="Parkinson Vol 20d", line=dict(color="#E11D48", width=1.5)
    ), row=row_n, col=1)
    # realized_vol_20d はパネルの spot から計算
    if "spot" in df.columns:
        rv = df["spot"].pct_change().rolling(20).std()
        fig.add_trace(go.Scatter(
            x=df.index, y=rv,
            name="Realized Vol 20d", line=dict(color="#6366F1", width=1.5, dash="dot")
        ), row=row_n, col=1)
    row_n += 1

# Daily Range / Overnight Gap
if has_range_panel:
    fig.add_trace(go.Scatter(
        x=df.index, y=df["daily_range_pct"],
        name="Daily Range %", line=dict(color="#A3E635", width=1), opacity=0.5
    ), row=row_n, col=1, secondary_y=False)
    if "daily_range_ma20" in df.columns:
        fig.add_trace(go.Scatter(
            x=df.index, y=df["daily_range_ma20"],
            name="Range MA20", line=dict(color="#65A30D", width=2)
        ), row=row_n, col=1, secondary_y=False)
    if "overnight_gap" in df.columns:
        fig.add_trace(go.Scatter(
            x=df.index, y=df["overnight_gap"],
            name="Overnight Gap", line=dict(color="#F97316", width=1)
        ), row=row_n, col=1, secondary_y=True)
        fig.update_yaxes(title_text="Gap", row=row_n, col=1, secondary_y=True)
    fig.update_yaxes(title_text="Range %", row=row_n, col=1, secondary_y=False)

fig.update_layout(
    height=200 * rows_count,
    hovermode="x unified",
    showlegend=True,
    margin=dict(l=0, r=0, t=30, b=0),
)
st.plotly_chart(fig, use_container_width=True)

# ── Rolling Correlation ────────────────────────────
st.subheader("Rolling Correlation (spot vs spread)")
corr_options = {}
if has_policy:
    corr_options["Policy Rate Spread"] = df[pol_a] - df[pol_b]
if has_short:
    corr_options["3M Rate Spread"] = df[short_a] - df[short_b]
if has_long:
    corr_options["10Y Rate Spread"] = df[long_a] - df[long_b]

if "spot" in df.columns and corr_options:
    selected_spread_name = st.selectbox("相関を比較するスプレッド", list(corr_options.keys()))
    selected_spread = corr_options[selected_spread_name]
    roll_corr = df["spot"].diff().rolling(60).corr(selected_spread.diff())
    fig2 = go.Figure()
    fig2.add_trace(go.Scatter(x=df.index, y=roll_corr, name=f"60d corr: spot vs {selected_spread_name}",
                              line=dict(color="#2563EB")))
    fig2.add_hline(y=0, line_dash="dash", line_color="gray")
    fig2.update_layout(height=220, margin=dict(l=0, r=0, t=0, b=0),
                       yaxis=dict(range=[-1.1, 1.1]))
    st.plotly_chart(fig2, use_container_width=True)

# ── USDJPY 専用: BoJ 資産 YoY ─────────────────────
if pair == "USDJPY" and "boj_assets" in df.columns:
    st.subheader("BoJ 資産残高 YoY")
    boj_yoy = df["boj_assets"].pct_change(252) * 100
    fig3 = go.Figure()
    fig3.add_trace(go.Scatter(x=df.index, y=boj_yoy, name="BoJ Assets YoY %",
                               line=dict(color="#059669")))
    fig3.add_hline(y=0, line_dash="dash", line_color="gray")
    fig3.update_layout(height=200, yaxis_title="%", margin=dict(l=0, r=0, t=0, b=0))
    st.plotly_chart(fig3, use_container_width=True)

# ── Missing Data Summary ───────────────────────────
st.subheader("Missing Data Summary")
missing = panel_data.get("missing_rates", {})
if missing:
    miss_df = pd.DataFrame(
        [{"feature": k, "missing_rate": f"{v:.1%}"}
         for k, v in missing.items()]
    )
    def highlight(row):
        rate = float(row["missing_rate"].strip("%")) / 100
        if rate > 0.5:
            return ["background-color: #FEE2E2"] * len(row)
        elif rate > 0.1:
            return ["background-color: #FEF9C3"] * len(row)
        return [""] * len(row)
    st.caption("※ BIS REER・政策金利は月次データのため日次パネルでは欠測率が高く見えます（forward fill で補完済み）。")
    st.dataframe(miss_df.style.apply(highlight, axis=1), hide_index=True, use_container_width=True)

# ── Raw Data Table ─────────────────────────────────
with st.expander("Raw Panel Data"):
    st.dataframe(df, use_container_width=True)
    csv = df.to_csv()
    st.download_button("CSV ダウンロード", csv, f"{pair}_panel.csv", "text/csv")
