"""
Releases
- FRED Release Calendar の取得・閲覧
- Event Study 用のイベントデータ管理
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import streamlit as st
import requests
import pandas as pd
from datetime import date, timedelta

API_BASE = "http://localhost:8000/api/v1"

st.set_page_config(page_title="Releases", layout="wide")
st.title("Releases")
st.caption("FRED Release Calendar の取得・閲覧。Event Study で使用するイベントデータを管理します。")

# ── Fetch セクション ───────────────────────────────
st.subheader("Release Calendar 取得 (fetch)")

col1, col2 = st.columns(2)
with col1:
    fetch_start = st.date_input("開始日", value=date.today() - timedelta(days=365 * 3),
                                 key="fetch_start")
with col2:
    fetch_end = st.date_input("終了日", value=date.today(), key="fetch_end")

fetch_btn = st.button("FRED から取得", type="primary", use_container_width=True)

if fetch_btn:
    with st.spinner("FRED Release Calendar を取得中..."):
        try:
            r = requests.post(
                f"{API_BASE}/releases/fetch",
                json={
                    "start_date": str(fetch_start),
                    "end_date": str(fetch_end),
                },
                timeout=60,
            )
            if r.status_code == 200:
                body = r.json()
                data = body.get("data", body) if isinstance(body, dict) else body
                inserted = data.get("inserted", 0) if isinstance(data, dict) else 0
                st.success(f"取得完了: {inserted} 件のリリース日を保存しました。")
                st.cache_data.clear()
            else:
                st.error(f"エラー: {r.status_code} — {r.text}")
        except requests.exceptions.ConnectionError:
            st.error("API サーバーに接続できません。サーバーが起動しているか確認してください。")

# ── 一覧表示セクション ─────────────────────────────
st.divider()
st.subheader("Release Calendar 一覧")

col_a, col_b, col_c = st.columns([2, 2, 1])
with col_a:
    list_start = st.date_input("開始日", value=date.today() - timedelta(days=365),
                                key="list_start")
with col_b:
    list_end = st.date_input("終了日", value=date.today(), key="list_end")
with col_c:
    limit = st.number_input("表示件数", min_value=10, max_value=2000, value=500, step=50)

@st.cache_data(ttl=60, show_spinner="リリースデータを取得中...")
def fetch_release_list(start: str, end: str, limit: int):
    try:
        r = requests.get(
            f"{API_BASE}/releases/list",
            params={"start_date": start, "end_date": end, "limit": limit},
            timeout=10,
        )
        if r.status_code == 200:
            body = r.json()
            data = body.get("data", body) if isinstance(body, dict) else body
            return data if isinstance(data, list) else []
    except requests.exceptions.ConnectionError:
        pass
    return []

if st.button("一覧を表示", use_container_width=True):
    st.cache_data.clear()

releases = fetch_release_list(str(list_start), str(list_end), limit)

if not releases:
    st.info("リリースデータがありません。上の「FRED から取得」ボタンでデータを取り込んでください。")
else:
    df = pd.DataFrame(releases)

    # サマリー
    col_m1, col_m2, col_m3 = st.columns(3)
    with col_m1:
        st.metric("リリース件数", f"{len(df):,}")
    with col_m2:
        unique_names = df["release_name"].nunique() if "release_name" in df.columns else 0
        st.metric("ユニーク Release 数", unique_names)
    with col_m3:
        if "release_date" in df.columns and len(df) > 0:
            st.metric("日付範囲", f"{df['release_date'].min()} ~ {df['release_date'].max()}")

    # Release Name フィルタ
    if "release_name" in df.columns:
        release_names = sorted(df["release_name"].unique().tolist())
        selected = st.multiselect(
            "Release Name でフィルタ（空欄で全表示）",
            options=release_names,
            default=[],
            placeholder="例: Federal Funds Rate, Employment Situation ...",
        )
        if selected:
            df = df[df["release_name"].isin(selected)]

    st.dataframe(df, use_container_width=True, hide_index=True)

    # CSV ダウンロード
    st.download_button(
        "CSV ダウンロード",
        df.to_csv(index=False),
        "release_calendar.csv",
        mime="text/csv",
    )
