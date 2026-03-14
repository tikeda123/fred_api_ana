"""
Event Study
- release 周辺の FX リターン観測
- event-aligned cumulative return chart
- pre/post mean move / hit ratio
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import streamlit as st
import requests
import pandas as pd
import plotly.graph_objects as go
from datetime import date, timedelta
import numpy as np

API_BASE = "http://localhost:8000/api/v1"

st.set_page_config(page_title="Event Study", layout="wide")
st.title("Event Study")

# ── サイドバー ─────────────────────────────────────
with st.sidebar:
    st.header("設定")
    pair = st.selectbox("通貨ペア", ["USDJPY", "EURUSD", "AUDUSD"])
    date_start = st.date_input("分析開始日", value=date.today() - timedelta(days=365 * 3))
    date_end   = st.date_input("分析終了日", value=date.today())
    window = st.slider("イベントウィンドウ (営業日)", 1, 10, 5)
    run = st.button("分析実行", type="primary", use_container_width=True)

if not run:
    st.info("サイドバーで設定し「分析実行」を押してください。")
    st.stop()

# ── spot パネル取得 ────────────────────────────────
@st.cache_data(ttl=300, show_spinner="パネルを取得中...")
def fetch_spot(pair, date_start, date_end):
    payload = {"pair": pair, "date_start": str(date_start),
               "date_end": str(date_end), "features": ["spot"]}
    r = requests.post(f"{API_BASE}/panel/build", json=payload, timeout=30)
    return r.json()

panel_result = fetch_spot(pair, date_start, date_end)
records = panel_result.get("data", {}).get("records", [])

if not records:
    st.warning("spot データがありません。先にデータを取り込んでください。")
    st.stop()

spot_df = pd.DataFrame(records)
spot_df["obs_date"] = pd.to_datetime(spot_df["obs_date"])
spot_df = spot_df.set_index("obs_date").dropna()
spot_df["return_1d"] = spot_df["spot"].pct_change()

# ── release calendar 取得 ──────────────────────────
@st.cache_data(ttl=600, show_spinner="Release calendar を取得中...")
def fetch_releases(date_start, date_end):
    r = requests.get(f"{API_BASE}/releases/list",
                     params={"start_date": str(date_start), "end_date": str(date_end), "limit": 500},
                     timeout=10)
    return r.json().get("data", [])

releases = fetch_releases(date_start, date_end)

if not releases:
    st.warning("Release calendar データがありません。")
    st.info("左メニュー「Releases」ページで「FRED から取得」ボタンを押してデータを取り込んでください。")

    # DB なしでもデモ：FOMC 会合の概算日で代替
    st.subheader("デモ: 手動イベント日を使った分析")
    event_dates_input = st.text_area(
        "イベント日を入力 (1行1日, YYYY-MM-DD)",
        value="2024-01-31\n2024-03-20\n2024-05-01\n2024-06-12\n2024-07-31\n2024-09-18\n2024-11-07\n2024-12-18\n2025-01-29\n2025-03-19\n2025-05-07\n2025-06-18\n2025-07-30\n2025-09-17\n2025-11-07\n2025-12-18\n2026-01-29\n2026-03-19",
    )
    event_dates = []
    for line in event_dates_input.strip().splitlines():
        try:
            event_dates.append(pd.Timestamp(line.strip()))
        except Exception:
            pass
    event_label = "Manual Events"
else:
    # release name でフィルタ
    release_df = pd.DataFrame(releases)
    release_names = sorted(release_df["release_name"].unique().tolist())
    selected_release = st.selectbox("Release を選択", release_names)
    event_dates = pd.to_datetime(
        release_df[release_df["release_name"] == selected_release]["release_date"].tolist()
    )
    event_label = selected_release

st.divider()

if len(event_dates) == 0:
    st.warning("イベント日が取得できませんでした。")
    st.stop()

# ── Event Window 計算 ──────────────────────────────
returns_by_event: list[list[float]] = []
valid_events = []

for ev_date in event_dates:
    try:
        mask_before = (spot_df.index >= ev_date - timedelta(days=window * 2)) & (spot_df.index <= ev_date)
        mask_after  = (spot_df.index >= ev_date) & (spot_df.index <= ev_date + timedelta(days=window * 2))
        pre  = spot_df[mask_before]["return_1d"].dropna().tail(window)
        post = spot_df[mask_after]["return_1d"].dropna().head(window)
        if len(pre) < window // 2 or len(post) < 1:
            continue
        window_returns = list(pre) + list(post)
        # -window → 0 → +window に正規化
        returns_by_event.append(window_returns[:window * 2])
        valid_events.append(ev_date.date())
    except Exception:
        continue

if not returns_by_event:
    st.warning("有効なイベントウィンドウが取得できませんでした。")
    st.stop()

st.success(f"有効イベント数: {len(valid_events)} 件")

# ── Event-aligned cumulative return chart ─────────
st.subheader(f"Event-aligned Cumulative Return: {pair} × {event_label}")

# 全イベントを time-0 で揃える（cumsum per event）
fig = go.Figure()
cum_matrix = []
for ret_list in returns_by_event:
    cum = np.cumsum([0.0] + ret_list)
    if len(cum) >= window * 2:
        cum_matrix.append(cum[:window * 2 + 1])

if cum_matrix:
    x_axis = list(range(-window, window + 1))[:len(cum_matrix[0])]
    arr = np.array(cum_matrix)

    # 各イベントのパス（薄く表示）
    for i, row in enumerate(arr):
        fig.add_trace(go.Scatter(
            x=x_axis[:len(row)], y=row * 100,
            mode="lines",
            line=dict(width=0.8, color="lightblue"),
            showlegend=False,
            hoverinfo="skip",
        ))

    # 平均パス
    mean_path = arr.mean(axis=0)
    fig.add_trace(go.Scatter(
        x=x_axis[:len(mean_path)], y=mean_path * 100,
        name="平均", mode="lines+markers",
        line=dict(color="#2563EB", width=2.5),
    ))

    # 中央値パス
    median_path = np.median(arr, axis=0)
    fig.add_trace(go.Scatter(
        x=x_axis[:len(median_path)], y=median_path * 100,
        name="中央値", mode="lines",
        line=dict(color="#D97706", width=1.5, dash="dash"),
    ))

    fig.add_vline(x=0, line_dash="dash", line_color="red", annotation_text="Event Day")
    fig.add_hline(y=0, line_color="gray", line_width=0.8)
    fig.update_layout(
        xaxis_title="Days from event",
        yaxis_title="Cumulative return (%)",
        height=400,
        margin=dict(l=0, r=0, t=10, b=0),
    )
    st.plotly_chart(fig, use_container_width=True)

# ── Pre / Post 統計 ────────────────────────────────
st.subheader("Pre / Post Move 統計")

pre_means, post_means = [], []
hit_count = 0
for ret_list in returns_by_event:
    mid = len(ret_list) // 2
    pre_cum  = sum(ret_list[:mid])
    post_cum = sum(ret_list[mid:])
    pre_means.append(pre_cum * 100)
    post_means.append(post_cum * 100)
    if post_cum > 0:
        hit_count += 1

col1, col2, col3, col4 = st.columns(4)
col1.metric("平均 pre-move", f"{np.mean(pre_means):.3f}%")
col2.metric("平均 post-move", f"{np.mean(post_means):.3f}%")
col3.metric("Hit Ratio (post>0)", f"{hit_count}/{len(post_means)} = {hit_count/len(post_means):.0%}")
col4.metric("有効イベント数", len(valid_events))

# ── Raw event table ────────────────────────────────
with st.expander("Raw Event Table"):
    rows = []
    for ev, pre, post in zip(valid_events, pre_means, post_means):
        rows.append({"event_date": ev, "pre_move%": round(pre, 4), "post_move%": round(post, 4), "direction": "UP" if post > 0 else "DOWN"})
    ev_df = pd.DataFrame(rows)
    st.dataframe(ev_df, hide_index=True, use_container_width=True)
    st.download_button("CSV", ev_df.to_csv(index=False), f"{pair}_events.csv")
