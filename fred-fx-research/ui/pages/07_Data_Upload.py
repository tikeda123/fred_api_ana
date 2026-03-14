"""
Data Upload
- OHLC CSV ファイルを API 経由でアップロード
- アップロードステータスのポーリング
- 最近のアップロード履歴表示
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import time
import streamlit as st
import requests
import pandas as pd

API_BASE = "http://localhost:8000/api/v1"

st.set_page_config(page_title="Data Upload", layout="wide")
st.title("Data Upload")
st.caption("OHLC CSV ファイルをアップロードし、クロスアセット分析用データを登録します。")

# ── サイドバー ─────────────────────────────────────
with st.sidebar:
    st.header("アップロード設定")
    vendor_symbol = st.text_input("Vendor Symbol", value="USATECHIDXUSD",
                                   help="ブローカー側のシンボル名")
    canonical_symbol = st.text_input("Canonical Symbol", value="USATECHIDXUSD",
                                      help="正規化後のシンボル名")
    instrument_name = st.text_input("Instrument Name", value="US Tech 100 Index",
                                     help="表示名（省略可）")
    asset_class = st.selectbox("アセットクラス", ["equity_index", "fx", "commodity"], index=0)
    timeframe = st.selectbox("タイムフレーム", ["240", "60", "1440"], index=0,
                              format_func=lambda x: {"240": "H4 (240)", "60": "H1 (60)", "1440": "D1 (1440)"}[x])
    delimiter = st.selectbox("区切り文字", ["tab", "comma", "semicolon"], index=0)
    has_header = st.toggle("ヘッダー行あり", value=False)
    ts_format = st.text_input("タイムスタンプ形式", value="%Y-%m-%d %H:%M",
                               help="例: %Y-%m-%d %H:%M, %Y.%m.%d %H:%M")

# ── ファイルアップロードUI ───────────────────────────
st.subheader("CSV ファイルのアップロード")

uploaded_file = st.file_uploader(
    "OHLC CSV ファイルを選択",
    type=["csv", "txt"],
    help="タブ区切り / ヘッダーなし: timestamp, open, high, low, close, volume",
)

if uploaded_file is not None:
    st.info(f"ファイル: **{uploaded_file.name}** ({uploaded_file.size:,} bytes)")

    col1, col2 = st.columns([1, 3])
    with col1:
        upload_btn = st.button("アップロード開始", type="primary", use_container_width=True)

    if upload_btn:
        with st.spinner("アップロード中..."):
            delim_map = {"tab": "\t", "comma": ",", "semicolon": ";"}
            files = {"file": (uploaded_file.name, uploaded_file.getvalue(), "text/plain")}
            form_data = {
                "vendor_symbol": vendor_symbol,
                "canonical_symbol": canonical_symbol,
                "instrument_name": instrument_name or "",
                "asset_class": asset_class,
                "timeframe": timeframe,
                "delimiter": delim_map[delimiter],
                "has_header": str(has_header).lower(),
                "ts_format": ts_format,
            }
            try:
                r = requests.post(
                    f"{API_BASE}/market/uploads",
                    files=files,
                    data=form_data,
                    timeout=30,
                )
                if r.status_code == 202:
                    result = r.json()
                    data = result.get("data", result) if isinstance(result, dict) else result
                    upload_id = data["upload_id"]
                    st.success(f"受付完了: upload_id = `{upload_id}`")
                    st.session_state["last_upload_id"] = upload_id
                else:
                    st.error(f"エラー: {r.status_code} — {r.text}")
            except requests.exceptions.ConnectionError:
                st.error("API サーバーに接続できません。サーバーが起動しているか確認してください。")

# ── ステータス確認 ─────────────────────────────────
st.divider()
st.subheader("アップロードステータス確認")

last_id = st.session_state.get("last_upload_id", "")
check_id = st.text_input("Upload ID", value=last_id, placeholder="例: abc123")

if check_id:
    col_check, col_refresh = st.columns([1, 1])
    with col_check:
        check_btn = st.button("ステータス確認", use_container_width=True)
    with col_refresh:
        auto_poll = st.toggle("自動更新 (5秒)", value=False)

    def fetch_status(uid: str) -> dict | None:
        try:
            r = requests.get(f"{API_BASE}/market/uploads/{uid}", timeout=10)
            if r.status_code == 200:
                body = r.json()
                data = body.get("data", body) if isinstance(body, dict) else body
                return data
        except requests.exceptions.ConnectionError:
            pass
        return None

    if check_btn or auto_poll:
        status_data = fetch_status(check_id)

        if status_data is None:
            st.warning("ステータスを取得できませんでした。")
        else:
            status = status_data.get("status", "unknown")

            status_color = {
                "accepted": "🟡",
                "processing": "🔵",
                "loaded": "🟢",
                "failed": "🔴",
            }.get(status, "⚪")

            st.metric("ステータス", f"{status_color} {status}")

            col_a, col_b, col_c = st.columns(3)
            with col_a:
                st.metric("検出行数", status_data.get("row_count_detected") or "-")
            with col_b:
                st.metric("ロード済み", status_data.get("row_count_loaded") or "-")
            with col_c:
                st.metric("棄却行数", status_data.get("row_count_rejected") or "-")

            if status_data.get("error_message"):
                st.error(f"エラー: {status_data['error_message']}")

            if status_data.get("started_at"):
                started = status_data["started_at"]
                finished = status_data.get("finished_at", "-")
                st.caption(f"開始: {started} / 完了: {finished}")

            if auto_poll and status in ("accepted", "processing"):
                time.sleep(5)
                st.rerun()

# ── アップロード履歴 ───────────────────────────────
st.divider()
st.subheader("最近のアップロード履歴")

@st.cache_data(ttl=30, show_spinner="履歴を取得中...")
def fetch_recent_uploads():
    try:
        r = requests.get(f"{API_BASE}/market/uploads", timeout=10)
        if r.status_code == 200:
            body = r.json()
            data = body.get("data", body) if isinstance(body, dict) else body
            return data if isinstance(data, list) else []
    except requests.exceptions.ConnectionError:
        pass
    return []

if st.button("履歴を更新", use_container_width=False):
    st.cache_data.clear()

history = fetch_recent_uploads()

if not history:
    st.info("アップロード履歴がありません。")
else:
    df_hist = pd.DataFrame(history)
    display_cols = [c for c in [
        "upload_id", "instrument_id", "status",
        "row_count_detected", "row_count_loaded", "row_count_rejected",
        "started_at", "finished_at",
    ] if c in df_hist.columns]

    status_emoji = {"loaded": "🟢", "failed": "🔴", "processing": "🔵", "accepted": "🟡"}
    if "status" in df_hist.columns:
        df_hist["status"] = df_hist["status"].map(lambda s: f"{status_emoji.get(s, '⚪')} {s}")

    st.dataframe(df_hist[display_cols], use_container_width=True, hide_index=True)
