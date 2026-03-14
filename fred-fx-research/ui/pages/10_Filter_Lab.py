"""
Filter Lab
- risk_filter_flag のルールを動的に変更して影響を検証
- ドローダウン / ボラティリティ / VIX / USD のしきい値スライダー
- "avoid" 日のハイライト + PnL シミュレーション
- JSON エクスポート
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import json
import streamlit as st
import requests
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import date, timedelta

API_BASE = "http://localhost:8000/api/v1"

st.set_page_config(page_title="Filter Lab", layout="wide")
st.title("Filter Lab")
st.caption("risk_filter_flag のしきい値を動的に変更し、no-trade フィルターの影響を検証します。")

# ── サイドバー ─────────────────────────────────────
with st.sidebar:
    st.header("設定")
    pair = st.selectbox("通貨ペア", ["USDJPY", "EURUSD", "AUDUSD"])
    date_start = st.date_input("開始日", value=date.today() - timedelta(days=365 * 3))
    date_end = st.date_input("終了日", value=date.today())

    st.subheader("フィルターしきい値")
    st.caption("2条件以上を同時に満たした日が 'avoid' になります。")

    dd_threshold = st.slider(
        "ドローダウン しきい値 (DD ≤ X%)",
        min_value=-30.0, max_value=-1.0, value=-8.0, step=0.5,
        format="%.1f%%",
        help="USATECH の 20日ドローダウンがこの値以下の日に条件成立",
    )
    rv_pct = st.slider(
        "実現ボラ パーセンタイル (RV > Pxx)",
        min_value=50, max_value=99, value=80, step=5,
        help="USATECH 20日 RV がこのパーセンタイルより高い日に条件成立",
    )
    vix_pct = st.slider(
        "VIX パーセンタイル (VIX > Pxx)",
        min_value=50, max_value=99, value=80, step=5,
        help="VIX がこのパーセンタイルより高い日に条件成立",
    )
    usd_z_threshold = st.slider(
        "USD Broad Z-score しきい値 (Z ≥ X)",
        min_value=0.5, max_value=3.0, value=1.0, step=0.1,
        format="%.1f",
        help="USD Broad の Z-score がこの値以上の日に条件成立",
    )
    min_conditions = st.slider(
        "avoid とみなす最小条件数",
        min_value=1, max_value=4, value=2, step=1,
    )

    run = st.button("フィルター適用", type="primary", use_container_width=True)

if not run:
    st.info("サイドバーでしきい値を設定し、「フィルター適用」を押してください。")
    st.stop()

# ── パネルデータ取得 ────────────────────────────────
@st.cache_data(ttl=300, show_spinner="パネルを取得中...")
def fetch_panel(pair: str, start: str, end: str):
    try:
        r = requests.get(
            f"{API_BASE}/panels/fx-crossasset",
            params={"pair": pair, "start": start, "end": end,
                    "include_usatech": True, "include_fred": True},
            timeout=30,
        )
        if r.status_code == 200:
            body = r.json()
            return body.get("data", body) if isinstance(body, dict) else body
        st.error(f"API エラー: {r.status_code}")
    except requests.exceptions.ConnectionError:
        st.error("API サーバーに接続できません。")
    return []

rows = fetch_panel(pair, str(date_start), str(date_end))

if not rows:
    st.warning("データが見つかりません。Cross Asset Explorer でパネルを構築してください。")
    st.stop()

df = pd.DataFrame(rows)
df["obs_date"] = pd.to_datetime(df["obs_date"])
df = df.sort_values("obs_date").reset_index(drop=True)

# ── カスタムフィルターの適用 ────────────────────────
def apply_custom_filter(
    df: pd.DataFrame,
    dd_thr: float,
    rv_pct: int,
    vix_pct: int,
    usd_z_thr: float,
    min_cond: int,
) -> pd.DataFrame:
    df = df.copy()

    # RV パーセンタイル計算
    rv_col = "usatech_rv_20d"
    rv_p = df[rv_col].quantile(rv_pct / 100) if rv_col in df.columns and df[rv_col].notna().any() else np.nan

    # VIX パーセンタイル計算
    vix_col = "vix"
    vix_p = df[vix_col].quantile(vix_pct / 100) if vix_col in df.columns and df[vix_col].notna().any() else np.nan

    # USD Z-score
    usd_col = "usd_broad"
    if usd_col in df.columns and df[usd_col].notna().any():
        usd_mean = df[usd_col].mean()
        usd_std = df[usd_col].std()
        df["_usd_z"] = (df[usd_col] - usd_mean) / usd_std.clip(lower=1e-9)
    else:
        df["_usd_z"] = np.nan

    # 各条件フラグ
    dd_col = "usatech_drawdown_20d"
    cond_dd = (df[dd_col] <= dd_thr / 100) if dd_col in df.columns else pd.Series(False, index=df.index)
    cond_rv = (df[rv_col] > rv_p) if rv_col in df.columns and not np.isnan(rv_p) else pd.Series(False, index=df.index)
    cond_vix = (df[vix_col] > vix_p) if vix_col in df.columns and not np.isnan(vix_p) else pd.Series(False, index=df.index)
    cond_usd = (df["_usd_z"] >= usd_z_thr) if "_usd_z" in df.columns else pd.Series(False, index=df.index)

    df["cond_dd"] = cond_dd.fillna(False)
    df["cond_rv"] = cond_rv.fillna(False)
    df["cond_vix"] = cond_vix.fillna(False)
    df["cond_usd"] = cond_usd.fillna(False)

    n_cond = df["cond_dd"].astype(int) + df["cond_rv"].astype(int) + df["cond_vix"].astype(int) + df["cond_usd"].astype(int)
    df["custom_flag"] = np.where(n_cond >= min_cond, "avoid", "ok")

    return df, rv_p, vix_p

df, rv_threshold, vix_threshold = apply_custom_filter(
    df, dd_threshold, rv_pct, vix_pct, usd_z_threshold, min_conditions
)

# ── PnL シミュレーション ────────────────────────────
def simulate_pnl(df: pd.DataFrame, flag_col: str, ret_col: str) -> pd.Series:
    """avoid 日はポジションなし (ret=0) として累積リターンを計算"""
    if ret_col not in df.columns:
        return pd.Series(dtype=float)
    rets = df[ret_col].fillna(0).copy()
    filtered_rets = np.where(df[flag_col] == "avoid", 0.0, rets)
    base_cumret = (1 + rets).cumprod() - 1
    filtered_cumret = (1 + pd.Series(filtered_rets)).cumprod() - 1
    return base_cumret, filtered_cumret

base_cum, filtered_cum = simulate_pnl(df, "custom_flag", "pair_ret_1d")

# ── メトリクス ─────────────────────────────────────
avoid_mask = df["custom_flag"] == "avoid"
avoid_count = avoid_mask.sum()
avoid_pct = avoid_count / len(df) * 100

orig_flag = df.get("risk_filter_flag")
if orig_flag is not None:
    orig_avoid = (orig_flag == "avoid").sum()
    orig_pct = orig_avoid / len(df) * 100
else:
    orig_avoid = None
    orig_pct = None

col1, col2, col3, col4, col5 = st.columns(5)
with col1:
    st.metric("データ行数", f"{len(df):,}")
with col2:
    st.metric("Avoid 日数 (カスタム)", f"{avoid_count:,}", f"{avoid_pct:.1f}%")
with col3:
    if orig_avoid is not None:
        st.metric("Avoid 日数 (デフォルト)", f"{orig_avoid:,}", f"{orig_pct:.1f}%")
with col4:
    st.metric(f"RV P{rv_pct}", f"{rv_threshold:.3f}" if not np.isnan(rv_threshold) else "N/A")
with col5:
    st.metric(f"VIX P{vix_pct}", f"{vix_threshold:.1f}" if not np.isnan(vix_threshold) else "N/A")

st.divider()

# ── 3段プロット ────────────────────────────────────
fig = make_subplots(
    rows=3, cols=1,
    shared_xaxes=True,
    row_heights=[0.45, 0.30, 0.25],
    subplot_titles=[
        f"{pair} Spot (avoid 日をハイライト)",
        "累積リターン比較",
        "条件フラグ",
    ],
    vertical_spacing=0.06,
)

# Row 1: FX Spot + avoid ハイライト
if "pair_close" in df.columns:
    fig.add_trace(
        go.Scatter(
            x=df["obs_date"], y=df["pair_close"],
            name=f"{pair} Spot",
            line=dict(color="#1f77b4", width=1.5),
        ),
        row=1, col=1,
    )

# avoid 日のシェーディング
avoid_dates = df[avoid_mask]["obs_date"]
for dt in avoid_dates:
    fig.add_vrect(
        x0=dt - pd.Timedelta(hours=12),
        x1=dt + pd.Timedelta(hours=12),
        fillcolor="rgba(214, 39, 40, 0.2)",
        line_width=0,
        row=1, col=1,
    )

# デフォルトフラグとの比較 (点線)
if orig_flag is not None:
    orig_avoid_dates = df[orig_flag == "avoid"]["obs_date"]
    for dt in orig_avoid_dates:
        fig.add_vrect(
            x0=dt - pd.Timedelta(hours=12),
            x1=dt + pd.Timedelta(hours=12),
            fillcolor="rgba(148, 103, 189, 0.15)",
            line_width=0,
            row=1, col=1,
        )

# Row 2: 累積リターン
if len(base_cum) > 0:
    fig.add_trace(
        go.Scatter(
            x=df["obs_date"], y=base_cum * 100,
            name=f"{pair} Buy & Hold (%)",
            line=dict(color="#1f77b4", width=1.5),
        ),
        row=2, col=1,
    )
    fig.add_trace(
        go.Scatter(
            x=df["obs_date"], y=filtered_cum * 100,
            name="Filter 適用 (%)",
            line=dict(color="#2ca02c", width=1.5),
        ),
        row=2, col=1,
    )
    fig.add_hline(y=0, line_dash="dash", line_color="gray", row=2, col=1)

# Row 3: 条件フラグ (スタックバー)
condition_cols = ["cond_dd", "cond_rv", "cond_vix", "cond_usd"]
cond_labels = ["DD", "RV", "VIX", "USD-Z"]
colors = ["#d62728", "#ff7f0e", "#9467bd", "#8c564b"]
for col, label, color in zip(condition_cols, cond_labels, colors):
    if col in df.columns:
        fig.add_trace(
            go.Bar(
                x=df["obs_date"],
                y=df[col].astype(int),
                name=label,
                marker_color=color,
                opacity=0.7,
            ),
            row=3, col=1,
        )

fig.update_layout(
    height=750,
    barmode="stack",
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    margin=dict(l=40, r=40, t=60, b=20),
    hovermode="x unified",
    template="plotly_white",
)
fig.update_yaxes(title_text=pair, row=1, col=1)
fig.update_yaxes(title_text="累積リターン (%)", row=2, col=1)
fig.update_yaxes(title_text="条件数", row=3, col=1)
fig.update_xaxes(title_text="日付", row=3, col=1)

st.plotly_chart(fig, use_container_width=True)

# ── avoid 日一覧 ───────────────────────────────────
st.subheader("Avoid 日一覧")
avoid_df = df[avoid_mask][["obs_date", "pair_close", "pair_ret_1d",
                            "cond_dd", "cond_rv", "cond_vix", "cond_usd",
                            "custom_flag"]].copy()
avoid_df["obs_date"] = avoid_df["obs_date"].dt.strftime("%Y-%m-%d")
avoid_df["pair_ret_1d"] = avoid_df["pair_ret_1d"].map(lambda x: f"{x*100:.2f}%" if pd.notna(x) else "N/A")

cond_display = {"cond_dd": "DD", "cond_rv": "RV", "cond_vix": "VIX", "cond_usd": "USD-Z"}
avoid_df = avoid_df.rename(columns={k: v for k, v in cond_display.items() if k in avoid_df.columns})

st.dataframe(avoid_df, use_container_width=True, hide_index=True)

# ── JSON エクスポート ──────────────────────────────
st.subheader("フィルター設定エクスポート")

filter_config = {
    "pair": pair,
    "date_start": str(date_start),
    "date_end": str(date_end),
    "thresholds": {
        "drawdown_20d_pct": dd_threshold,
        "rv_percentile": rv_pct,
        "rv_computed_threshold": float(rv_threshold) if not np.isnan(rv_threshold) else None,
        "vix_percentile": vix_pct,
        "vix_computed_threshold": float(vix_threshold) if not np.isnan(vix_threshold) else None,
        "usd_broad_zscore": usd_z_threshold,
        "min_conditions_for_avoid": min_conditions,
    },
    "results": {
        "total_days": int(len(df)),
        "avoid_days": int(avoid_count),
        "avoid_pct": float(avoid_pct),
        "avoid_dates": avoid_df["obs_date"].tolist() if "obs_date" in avoid_df.columns else [],
    }
}

col_json, col_download = st.columns(2)
with col_json:
    with st.expander("JSON プレビュー"):
        st.json(filter_config)

with col_download:
    st.download_button(
        label="JSON ダウンロード",
        data=json.dumps(filter_config, indent=2, ensure_ascii=False),
        file_name=f"filter_config_{pair}_{date.today().isoformat()}.json",
        mime="application/json",
        use_container_width=True,
    )
