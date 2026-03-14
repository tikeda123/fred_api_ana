"""
Series Catalog
- FRED series 検索
- メタデータ表示
- watchlist への追加
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import streamlit as st
import requests
import pandas as pd

API_BASE = "http://localhost:8000/api/v1"

st.set_page_config(page_title="Series Catalog", layout="wide")
st.title("Series Catalog")

# ── 検索フォーム ───────────────────────────────────
with st.form("search_form"):
    col1, col2, col3 = st.columns([3, 1, 1])
    keyword = col1.text_input("キーワード", value="Japanese Yen")
    limit = col2.number_input("最大件数", 10, 200, 50)
    refresh = col3.checkbox("FREDから再取得", value=False)
    submitted = st.form_submit_button("検索")

if submitted and keyword:
    with st.spinner("検索中..."):
        try:
            params = {"q": keyword, "limit": limit, "refresh_from_fred": refresh}
            r = requests.get(f"{API_BASE}/series/search", params=params, timeout=15)
            result = r.json()
            data = result.get("data", [])
            warnings = result.get("warnings", [])

            if warnings:
                for w in warnings:
                    st.warning(w)

            if not data:
                st.info("該当する series が見つかりませんでした。")
            else:
                df = pd.DataFrame(data)
                st.success(f"{len(df)} 件見つかりました")

                # テーブル表示
                show_cols = ["series_id", "title", "frequency", "units",
                             "observation_start", "observation_end", "domain", "freshness_status"]
                show_cols = [c for c in show_cols if c in df.columns]
                st.dataframe(
                    df[show_cols],
                    use_container_width=True,
                    hide_index=True,
                )

                # 行クリックで詳細
                st.subheader("Series 詳細")
                selected_id = st.selectbox("series_id を選択", df["series_id"].tolist())
                if selected_id:
                    row = df[df["series_id"] == selected_id].iloc[0]
                    st.json(row.to_dict())

        except Exception as e:
            st.error(f"検索失敗: {e}")
