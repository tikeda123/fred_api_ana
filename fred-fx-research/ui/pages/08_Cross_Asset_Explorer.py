"""
Cross Asset Explorer
- FX ペア × USATECH (US Tech 100 Index) オーバーレイ分析
- ボラティリティ / ドローダウン / risk_filter_flag 表示
- 3段サブプロット: Spot + USATECH / Volatility / Drawdown
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

API_BASE = "http://localhost:8000/api/v1"

st.set_page_config(page_title="Cross Asset Explorer", layout="wide")
st.title("Cross Asset Explorer")
st.caption("FX ペアと USATECH (US Tech 100 Index) のクロスアセット分析")

# ── サイドバー ─────────────────────────────────────
with st.sidebar:
    st.header("設定")
    pair = st.selectbox("通貨ペア", ["USDJPY", "EURUSD", "AUDUSD"])
    date_start = st.date_input("開始日", value=date.today() - timedelta(days=365 * 2))
    date_end = st.date_input("終了日", value=date.today())

    st.subheader("オーバーレイ")
    show_usatech = st.toggle("USATECH オーバーレイ", value=True)
    show_vix = st.toggle("VIX オーバーレイ", value=True)
    show_usd_broad = st.toggle("USD Broad Index", value=False)
    show_risk_filter = st.toggle("Risk Filter フラグ", value=True)

    st.subheader("パネル再構築")
    rebuild_btn = st.button("パネルを再構築", use_container_width=True,
                             help="最新データでパネルを再計算します")

    run = st.button("表示", type="primary", use_container_width=True)

# ── パネル再構築 ────────────────────────────────────
if rebuild_btn:
    with st.spinner("パネルを再構築中..."):
        try:
            r = requests.post(
                f"{API_BASE}/panels/fx-crossasset/rebuild",
                params={"pair": pair, "start": str(date_start), "end": str(date_end)},
                timeout=60,
            )
            if r.status_code == 200:
                result = r.json()
                data = result.get("data", result) if isinstance(result, dict) else result
                st.success(f"再構築完了: {data.get('rows_built', '?')} 行")
                st.cache_data.clear()
            else:
                st.error(f"再構築エラー: {r.status_code} — {r.text}")
        except requests.exceptions.ConnectionError:
            st.error("API サーバーに接続できません。")

if not run:
    st.info("サイドバーで設定を行い、「表示」を押してください。")
    st.stop()

# ── パネルデータ取得 ────────────────────────────────
@st.cache_data(ttl=300, show_spinner="クロスアセットパネルを取得中...")
def fetch_panel(pair: str, start: str, end: str, include_usatech: bool, include_fred: bool):
    try:
        r = requests.get(
            f"{API_BASE}/panels/fx-crossasset",
            params={
                "pair": pair,
                "start": start,
                "end": end,
                "include_usatech": include_usatech,
                "include_fred": include_fred,
            },
            timeout=30,
        )
        if r.status_code == 200:
            body = r.json()
            return body.get("data", body) if isinstance(body, dict) else body
        st.error(f"API エラー: {r.status_code} — {r.text}")
    except requests.exceptions.ConnectionError:
        st.error("API サーバーに接続できません。")
    return []

rows = fetch_panel(pair, str(date_start), str(date_end), show_usatech, show_vix or show_usd_broad)

if not rows:
    st.warning("データが見つかりません。パネルを再構築するか、データをアップロードしてください。")
    st.stop()

df = pd.DataFrame(rows)
df["obs_date"] = pd.to_datetime(df["obs_date"])
df = df.sort_values("obs_date").reset_index(drop=True)

# ── サマリーメトリクス ─────────────────────────────
col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("データ行数", f"{len(df):,}")
with col2:
    if "pair_close" in df.columns and df["pair_close"].notna().any():
        latest = df["pair_close"].dropna().iloc[-1]
        prev = df["pair_close"].dropna().iloc[-2] if len(df["pair_close"].dropna()) > 1 else latest
        delta = (latest - prev) / prev * 100
        st.metric(f"{pair} 最新", f"{latest:.4f}", f"{delta:+.2f}%")
with col3:
    if "usatech_close" in df.columns and df["usatech_close"].notna().any():
        latest_ut = df["usatech_close"].dropna().iloc[-1]
        prev_ut = df["usatech_close"].dropna().iloc[-2] if len(df["usatech_close"].dropna()) > 1 else latest_ut
        delta_ut = (latest_ut - prev_ut) / prev_ut * 100
        st.metric("USATECH 最新", f"{latest_ut:.1f}", f"{delta_ut:+.2f}%")
with col4:
    if "risk_filter_flag" in df.columns:
        avoid_pct = (df["risk_filter_flag"] == "avoid").sum() / len(df) * 100
        st.metric("Avoid 日率", f"{avoid_pct:.1f}%")

# ── 3段サブプロット ────────────────────────────────
row_specs = [
    [{"secondary_y": True}],
    [{"secondary_y": False}],
    [{"secondary_y": False}],
]
subplot_titles = [
    f"{pair} Spot vs USATECH",
    "Volatility (rv_20d)",
    "Drawdown 20d",
]
fig = make_subplots(
    rows=3, cols=1,
    shared_xaxes=True,
    row_heights=[0.45, 0.30, 0.25],
    subplot_titles=subplot_titles,
    specs=row_specs,
    vertical_spacing=0.06,
)

# Row 1: FX spot + USATECH
if "pair_close" in df.columns:
    fig.add_trace(
        go.Scatter(
            x=df["obs_date"], y=df["pair_close"],
            name=f"{pair} Spot",
            line=dict(color="#1f77b4", width=1.5),
        ),
        row=1, col=1, secondary_y=False,
    )

if show_usatech and "usatech_close" in df.columns:
    fig.add_trace(
        go.Scatter(
            x=df["obs_date"], y=df["usatech_close"],
            name="USATECH",
            line=dict(color="#ff7f0e", width=1.2, dash="dot"),
            opacity=0.8,
        ),
        row=1, col=1, secondary_y=True,
    )

# VIX overlay (row 1, secondary_y)
if show_vix and "vix" in df.columns and df["vix"].notna().any():
    fig.add_trace(
        go.Scatter(
            x=df["obs_date"], y=df["vix"],
            name="VIX",
            line=dict(color="#d62728", width=1.0, dash="dash"),
            opacity=0.6,
        ),
        row=1, col=1, secondary_y=True,
    )

# Risk filter shading (row 1)
if show_risk_filter and "risk_filter_flag" in df.columns:
    avoid_dates = df[df["risk_filter_flag"] == "avoid"]["obs_date"]
    for dt in avoid_dates:
        fig.add_vrect(
            x0=dt - pd.Timedelta(days=0.5),
            x1=dt + pd.Timedelta(days=0.5),
            fillcolor="rgba(214, 39, 40, 0.15)",
            line_width=0,
            row=1, col=1,
        )

# Row 2: Volatility
rv_col = "usatech_rv_20d" if "usatech_rv_20d" in df.columns else None
pair_rv_col = "pair_rv_20d" if "pair_rv_20d" in df.columns else None

if rv_col and df[rv_col].notna().any():
    fig.add_trace(
        go.Scatter(
            x=df["obs_date"], y=df[rv_col] * 100,
            name="USATECH RV20d (%)",
            line=dict(color="#ff7f0e", width=1.2),
            fill="tozeroy",
            fillcolor="rgba(255, 127, 14, 0.15)",
        ),
        row=2, col=1,
    )

if pair_rv_col and df[pair_rv_col].notna().any():
    fig.add_trace(
        go.Scatter(
            x=df["obs_date"], y=df[pair_rv_col] * 100,
            name=f"{pair} RV20d (%)",
            line=dict(color="#1f77b4", width=1.2),
        ),
        row=2, col=1,
    )

# Row 3: Drawdown
dd_col = "usatech_drawdown_20d" if "usatech_drawdown_20d" in df.columns else None

if dd_col and df[dd_col].notna().any():
    dd_vals = df[dd_col] * 100
    fig.add_trace(
        go.Bar(
            x=df["obs_date"], y=dd_vals,
            name="USATECH DD20d (%)",
            marker_color=[
                "rgba(214,39,40,0.7)" if v <= -8 else "rgba(255,127,14,0.5)"
                for v in dd_vals.fillna(0)
            ],
        ),
        row=3, col=1,
    )
    # -8% ライン
    fig.add_hline(y=-8, line_dash="dash", line_color="red",
                  annotation_text="-8%", row=3, col=1)

fig.update_layout(
    height=750,
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    margin=dict(l=40, r=40, t=60, b=20),
    hovermode="x unified",
    template="plotly_white",
)
fig.update_yaxes(title_text=pair, row=1, col=1, secondary_y=False)
if show_usatech or show_vix:
    fig.update_yaxes(title_text="USATECH / VIX", row=1, col=1, secondary_y=True)
fig.update_yaxes(title_text="RV (%)", row=2, col=1)
fig.update_yaxes(title_text="DD (%)", row=3, col=1)
fig.update_xaxes(title_text="日付", row=3, col=1)

st.plotly_chart(fig, use_container_width=True)

# ── データテーブル ─────────────────────────────────
with st.expander("データテーブル"):
    display_cols = [c for c in [
        "obs_date", "pair_close", "pair_ret_1d",
        "usatech_close", "usatech_ret_1d", "usatech_rv_20d", "usatech_drawdown_20d",
        "vix", "usd_broad", "risk_filter_flag",
    ] if c in df.columns]
    st.dataframe(df[display_cols].tail(100), use_container_width=True, hide_index=True)
