"""
Lag Correlation
- FX ペアと USATECH の遅行相関分析
- ローリング相関係数 / ローリング β
- ラグ相関バーチャート (l1, l2)
- 散布図
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import streamlit as st
import requests
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import date, timedelta

API_BASE = "http://localhost:8000/api/v1"

st.set_page_config(page_title="Lag Correlation", layout="wide")
st.title("Lag Correlation Analysis")
st.caption("USATECH リターンと FX ペアリターンの遅行相関・ローリング統計")

# ── サイドバー ─────────────────────────────────────
with st.sidebar:
    st.header("設定")
    pair = st.selectbox("通貨ペア", ["USDJPY", "EURUSD", "AUDUSD"])
    date_start = st.date_input("開始日", value=date.today() - timedelta(days=365 * 3))
    date_end = st.date_input("終了日", value=date.today())

    st.subheader("分析設定")
    corr_window = st.slider("相関ウィンドウ (日)", min_value=10, max_value=60, value=20, step=5)
    beta_window = st.slider("β ウィンドウ (日)", min_value=20, max_value=120, value=60, step=10)
    max_lag = st.slider("最大ラグ (日)", min_value=1, max_value=10, value=5, step=1)

    run = st.button("分析実行", type="primary", use_container_width=True)

if not run:
    st.info("サイドバーで設定を行い、「分析実行」を押してください。")
    st.stop()

# ── データ取得 ─────────────────────────────────────
@st.cache_data(ttl=300, show_spinner="データを取得中...")
def fetch_panel(pair: str, start: str, end: str):
    try:
        r = requests.get(
            f"{API_BASE}/panels/fx-crossasset",
            params={"pair": pair, "start": start, "end": end,
                    "include_usatech": True, "include_fred": False},
            timeout=30,
        )
        if r.status_code == 200:
            body = r.json()
            return body.get("data", body) if isinstance(body, dict) else body
        st.error(f"API エラー: {r.status_code}")
    except requests.exceptions.ConnectionError:
        st.error("API サーバーに接続できません。")
    return []

@st.cache_data(ttl=300, show_spinner="特徴量を取得中...")
def fetch_features(pair: str, start: str, end: str):
    try:
        r = requests.get(
            f"{API_BASE}/cross-asset/features",
            params={
                "feature_scope": "pair",
                "scope_id": pair,
                "pivot": True,
                "start": start,
                "end": end,
            },
            timeout=30,
        )
        if r.status_code == 200:
            body = r.json()
            return body.get("data", body) if isinstance(body, dict) else body
    except requests.exceptions.ConnectionError:
        pass
    return []

panel_rows = fetch_panel(pair, str(date_start), str(date_end))
feature_rows = fetch_features(pair, str(date_start), str(date_end))

if not panel_rows:
    st.warning("パネルデータが見つかりません。Cross Asset Explorer でパネルを構築してください。")
    st.stop()

df = pd.DataFrame(panel_rows)
df["obs_date"] = pd.to_datetime(df["obs_date"])
df = df.sort_values("obs_date").reset_index(drop=True)

# 特徴量データをマージ
if feature_rows:
    df_feat = pd.DataFrame(feature_rows)
    df_feat["obs_date"] = pd.to_datetime(df_feat["obs_date"])
    df = df.merge(df_feat, on="obs_date", how="left", suffixes=("", "_feat"))

# リターン列の確認
ret_usatech = df.get("usatech_ret_1d") if "usatech_ret_1d" in df.columns else None
ret_pair = df.get("pair_ret_1d") if "pair_ret_1d" in df.columns else None

if ret_usatech is None or ret_pair is None:
    st.warning("リターンデータが不足しています。データを確認してください。")
    st.stop()

# ローカルでローリング相関・β を計算（フォールバック）
usatech_s = df["usatech_ret_1d"].astype(float)
pair_s = df["pair_ret_1d"].astype(float)

# ローリング相関
rolling_corr = usatech_s.rolling(corr_window).corr(pair_s)

# ローリング β = cov(pair, usatech) / var(usatech)
def rolling_beta(x: pd.Series, y: pd.Series, w: int) -> pd.Series:
    """y の x に対する beta (y = beta * x + alpha)"""
    cov = x.rolling(w).cov(y)
    var = x.rolling(w).var()
    return cov / var.replace(0, np.nan)

rolling_beta_s = rolling_beta(usatech_s, pair_s, beta_window)

# ラグ相関の計算
lag_corrs = {}
for lag in range(0, max_lag + 1):
    if lag == 0:
        corr_val = usatech_s.corr(pair_s)
    else:
        corr_val = usatech_s.shift(lag).corr(pair_s)
    lag_corrs[f"lag_{lag}d"] = corr_val

# ── メトリクス ─────────────────────────────────────
st.subheader("統計サマリー")
cols = st.columns(4)
with cols[0]:
    current_corr = rolling_corr.dropna().iloc[-1] if rolling_corr.notna().any() else float("nan")
    st.metric(f"直近相関 ({corr_window}d)", f"{current_corr:.3f}")
with cols[1]:
    current_beta = rolling_beta_s.dropna().iloc[-1] if rolling_beta_s.notna().any() else float("nan")
    st.metric(f"直近 β ({beta_window}d)", f"{current_beta:.3f}")
with cols[2]:
    best_lag = max(lag_corrs, key=lambda k: abs(lag_corrs[k]))
    st.metric("最強ラグ", best_lag.replace("lag_", "").replace("d", " 日"))
with cols[3]:
    best_corr_val = lag_corrs[best_lag]
    st.metric("最強ラグ相関", f"{best_corr_val:.3f}")

st.divider()

# ── プロット: 4パネル ──────────────────────────────
fig = make_subplots(
    rows=2, cols=2,
    subplot_titles=[
        f"ローリング相関 ({corr_window}d)",
        f"ローリング β ({beta_window}d)",
        "ラグ相関バーチャート",
        f"散布図: USATECH ret vs {pair} ret",
    ],
    vertical_spacing=0.12,
    horizontal_spacing=0.08,
)

# ── Row1, Col1: ローリング相関 ─────────────────────
color_corr = ["rgba(44,160,44,0.8)" if v > 0 else "rgba(214,39,40,0.8)"
              for v in rolling_corr.fillna(0)]

fig.add_trace(
    go.Scatter(
        x=df["obs_date"], y=rolling_corr,
        name=f"Rolling Corr {corr_window}d",
        line=dict(color="#2ca02c", width=1.5),
    ),
    row=1, col=1,
)
fig.add_hline(y=0, line_dash="dash", line_color="gray", row=1, col=1)
fig.add_hline(y=0.3, line_dash="dot", line_color="green",
              annotation_text="+0.3", row=1, col=1)
fig.add_hline(y=-0.3, line_dash="dot", line_color="red",
              annotation_text="-0.3", row=1, col=1)

# ── Row1, Col2: ローリング β ──────────────────────
fig.add_trace(
    go.Scatter(
        x=df["obs_date"], y=rolling_beta_s,
        name=f"Rolling β {beta_window}d",
        line=dict(color="#9467bd", width=1.5),
    ),
    row=1, col=2,
)
fig.add_hline(y=0, line_dash="dash", line_color="gray", row=1, col=2)

# ── Row2, Col1: ラグ相関バー ─────────────────────
lag_labels = list(lag_corrs.keys())
lag_values = list(lag_corrs.values())
bar_colors = ["rgba(214,39,40,0.8)" if v < 0 else "rgba(44,160,44,0.8)"
              for v in lag_values]

fig.add_trace(
    go.Bar(
        x=lag_labels, y=lag_values,
        name="Lag Correlation",
        marker_color=bar_colors,
        text=[f"{v:.3f}" for v in lag_values],
        textposition="outside",
    ),
    row=2, col=1,
)
fig.add_hline(y=0, line_dash="dash", line_color="gray", row=2, col=1)

# ── Row2, Col2: 散布図 ────────────────────────────
# 色: ローリング相関の値でグラデーション
valid_mask = usatech_s.notna() & pair_s.notna()
scatter_corr = rolling_corr.reindex(usatech_s.index).fillna(0)

fig.add_trace(
    go.Scatter(
        x=usatech_s[valid_mask],
        y=pair_s[valid_mask],
        mode="markers",
        name="日次リターン",
        marker=dict(
            size=4,
            color=scatter_corr[valid_mask],
            colorscale="RdYlGn",
            cmin=-1,
            cmax=1,
            showscale=True,
            colorbar=dict(title="Rolling Corr", x=1.02),
            opacity=0.6,
        ),
    ),
    row=2, col=2,
)

# 回帰線
if valid_mask.sum() > 10:
    x_vals = usatech_s[valid_mask].values
    y_vals = pair_s[valid_mask].values
    coeffs = np.polyfit(x_vals, y_vals, 1)
    x_line = np.linspace(x_vals.min(), x_vals.max(), 100)
    y_line = np.polyval(coeffs, x_line)
    fig.add_trace(
        go.Scatter(
            x=x_line, y=y_line,
            name=f"OLS (β={coeffs[0]:.3f})",
            line=dict(color="navy", width=1.5, dash="dash"),
        ),
        row=2, col=2,
    )

fig.update_layout(
    height=700,
    showlegend=True,
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    margin=dict(l=40, r=60, t=60, b=40),
    hovermode="closest",
    template="plotly_white",
)
fig.update_yaxes(title_text="相関係数", row=1, col=1)
fig.update_yaxes(title_text="β", row=1, col=2)
fig.update_yaxes(title_text="相関係数", row=2, col=1)
fig.update_xaxes(title_text="USATECH ret", row=2, col=2)
fig.update_yaxes(title_text=f"{pair} ret", row=2, col=2)

st.plotly_chart(fig, use_container_width=True)

# ── ラグ相関テーブル ───────────────────────────────
with st.expander("ラグ相関詳細"):
    df_lag = pd.DataFrame({
        "ラグ": [k.replace("lag_", "").replace("d", " 日") for k in lag_corrs],
        "相関係数": [f"{v:.4f}" for v in lag_corrs.values()],
        "解釈": [
            "USATECH が FX に与える即時影響" if k == "lag_0d"
            else f"USATECH が {k.replace('lag_', '').replace('d', '')} 日後の FX に与える影響"
            for k in lag_corrs
        ],
    })
    st.dataframe(df_lag, use_container_width=True, hide_index=True)

# ── 期間別ローリング相関テーブル ────────────────────
with st.expander("期間別統計"):
    df_stats = pd.DataFrame({
        "統計量": ["平均", "標準偏差", "最大", "最小", "直近"],
        f"ローリング相関 ({corr_window}d)": [
            f"{rolling_corr.mean():.4f}",
            f"{rolling_corr.std():.4f}",
            f"{rolling_corr.max():.4f}",
            f"{rolling_corr.min():.4f}",
            f"{rolling_corr.dropna().iloc[-1]:.4f}" if rolling_corr.notna().any() else "N/A",
        ],
        f"ローリング β ({beta_window}d)": [
            f"{rolling_beta_s.mean():.4f}",
            f"{rolling_beta_s.std():.4f}",
            f"{rolling_beta_s.max():.4f}",
            f"{rolling_beta_s.min():.4f}",
            f"{rolling_beta_s.dropna().iloc[-1]:.4f}" if rolling_beta_s.notna().any() else "N/A",
        ],
    })
    st.dataframe(df_stats, use_container_width=True, hide_index=True)
