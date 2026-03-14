"""
Data Quality
- stale / warning / reject series 一覧
- missing heatmap
- ingestion log
全データは API 経由で取得（DuckDB 直接接続は行わない）
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import streamlit as st
import requests
import pandas as pd
import plotly.graph_objects as go
from datetime import date, timedelta
from tz_utils import to_jst

API_BASE = "http://localhost:8000/api/v1"

st.set_page_config(page_title="Data Quality", layout="wide")
st.title("Data Quality")

# ── Freshness Audit ────────────────────────────────
st.subheader("Freshness Audit")

@st.cache_data(ttl=60, show_spinner="鮮度監査中...")
def run_audit():
    try:
        r = requests.post(f"{API_BASE}/data-quality/audit", timeout=30)
        return r.json().get("data", [])
    except Exception as e:
        st.error(f"鮮度監査失敗: {e}")
        return []

if st.button("鮮度監査を実行", type="primary"):
    st.cache_data.clear()

audit_results = run_audit()
if audit_results:
    df = pd.DataFrame(audit_results)

    # ステータス別カラー
    def color_status(val):
        if val == "ok":
            return "background-color: #DCFCE7"
        elif val == "warning":
            return "background-color: #FEF9C3"
        elif val == "reject":
            return "background-color: #FEE2E2"
        return ""

    col1, col2, col3 = st.columns(3)
    col1.metric("OK",      len(df[df["status"] == "ok"]),      delta=None)
    col2.metric("Warning", len(df[df["status"] == "warning"]), delta=None)
    col3.metric("Reject",  len(df[df["status"] == "reject"]),  delta=None)

    show_cols = [c for c in ["series_id","status","frequency","observation_end","domain","reasons"] if c in df.columns]
    st.dataframe(
        df[show_cols].style.applymap(color_status, subset=["status"]),
        use_container_width=True,
        hide_index=True,
    )

st.divider()

# ── Missing Heatmap ────────────────────────────────
heatmap_pair = st.selectbox("Missing Heatmap 対象ペア", ["USDJPY", "EURUSD", "AUDUSD"], key="heatmap_pair")
st.subheader(f"Missing Data Heatmap ({heatmap_pair} Panel)")

@st.cache_data(ttl=300, show_spinner="パネル取得中...")
def fetch_panel_quality(pair: str):
    payload = {
        "pair": pair,
        "date_start": str(date.today() - timedelta(days=365 * 3)),
        "date_end": str(date.today()),
    }
    r = requests.post(f"{API_BASE}/panel/build", json=payload, timeout=30)
    return r.json()

panel_result = fetch_panel_quality(heatmap_pair)
panel_data = panel_result.get("data", {})
records = panel_data.get("records", [])
missing_rates = panel_data.get("missing_rates", {})

if records:
    pdf = pd.DataFrame(records)
    pdf["obs_date"] = pd.to_datetime(pdf["obs_date"])
    pdf = pdf.set_index("obs_date")

    # monthly missing heatmap
    feature_cols = [c for c in pdf.columns]
    null_mask = pdf.isnull().astype(int)
    monthly = null_mask.resample("ME").mean()

    if not monthly.empty and feature_cols:
        fig = go.Figure(data=go.Heatmap(
            z=monthly.values.T,
            x=[str(d.date()) for d in monthly.index],
            y=monthly.columns.tolist(),
            colorscale="RdYlGn_r",
            zmin=0, zmax=1,
            colorbar=dict(title="Missing rate"),
        ))
        fig.update_layout(
            title=f"月次欠測率 Heatmap ({heatmap_pair})",
            height=max(300, len(feature_cols) * 22 + 60),
            margin=dict(l=0, r=0, t=40, b=0),
        )
        st.plotly_chart(fig, use_container_width=True)
        st.caption("※ 政策金利・BIS REER は月次データのため日次パネルでは欠測率が高くなります（forward fill 済み）。")

if missing_rates:
    mr_df = pd.DataFrame([
        {"feature": k, "missing_rate": f"{v:.1%}"}
        for k, v in missing_rates.items()
    ])
    st.dataframe(mr_df, hide_index=True, use_container_width=False)

st.divider()

# ── Ingestion Log ──────────────────────────────────
st.subheader("Ingestion Log")

@st.cache_data(ttl=30)
def fetch_ingestion_log():
    try:
        r = requests.get(f"{API_BASE}/data-quality/ingestion-log", timeout=10)
        records = r.json().get("data", [])
        if records:
            return pd.DataFrame(records)
    except Exception:
        pass
    return pd.DataFrame()

log_df = fetch_ingestion_log()
if not log_df.empty:
    for tc in ["started_at", "finished_at"]:
        if tc in log_df.columns:
            log_df[tc] = log_df[tc].apply(lambda x: to_jst(pd.Timestamp(x)) if pd.notna(x) and x else "")
    def color_log(val):
        if val == "ok":
            return "background-color: #DCFCE7"
        elif val == "failed":
            return "background-color: #FEE2E2"
        elif val == "running":
            return "background-color: #DBEAFE"
        return ""
    st.dataframe(
        log_df.style.applymap(color_log, subset=["status"]),
        use_container_width=True,
        hide_index=True,
    )
else:
    st.info("ingestion ログなし")

# ── Market Bars 品質（USATECH）──────────────────────
st.divider()
st.subheader("Market Bars 品質レポート (USATECH)")

with st.expander("USATECH データ品質を確認する", expanded=False):
    instrument_id_input = st.text_input(
        "Instrument ID", value="usatechidxusd_h4", key="mq_instrument"
    )
    col_mq_s, col_mq_e = st.columns(2)
    with col_mq_s:
        mq_start = st.date_input("開始日", value=date.today() - timedelta(days=365 * 3), key="mq_start")
    with col_mq_e:
        mq_end = st.date_input("終了日", value=date.today(), key="mq_end")

    @st.cache_data(ttl=120, show_spinner="品質レポートを取得中...")
    def fetch_market_quality(instrument_id: str, start: str, end: str):
        try:
            r = requests.get(
                f"{API_BASE}/quality/market-report",
                params={"instrument_id": instrument_id, "start": start, "end": end},
                timeout=20,
            )
            if r.status_code == 200:
                return r.json().get("data", {})
        except Exception as e:
            st.error(f"品質レポート取得失敗: {e}")
        return {}

    if st.button("品質レポート取得", key="mq_run"):
        st.cache_data.clear()

    mq_data = fetch_market_quality(instrument_id_input, str(mq_start), str(mq_end))

    if mq_data:
        # 行数メトリクス
        col_r, col_n, col_d, col_ar = st.columns(4)
        with col_r:
            st.metric("Raw 行数", f"{mq_data.get('raw_rows', 0):,}")
        with col_n:
            st.metric("Norm 行数", f"{mq_data.get('norm_rows', 0):,}")
        with col_d:
            st.metric("Daily 行数", f"{mq_data.get('daily_rows', 0):,}")
        with col_ar:
            st.metric("異常 Range 日数", f"{mq_data.get('abnormal_range_count', 0):,}")

        # 日付ギャップ一覧
        gaps = mq_data.get("date_gaps", [])
        if gaps:
            st.warning(f"日付ギャップが {len(gaps)} 件検出されました（5日超）")
            df_gaps = pd.DataFrame(gaps)
            st.dataframe(df_gaps, use_container_width=True, hide_index=True)
        else:
            st.success("日付ギャップなし（5日超の欠損なし）")

        if mq_data.get("range_p99") is not None:
            st.caption(f"Range P99: {mq_data['range_p99']:.4f}（この値超が異常 Range として計上）")
    else:
        st.info("レポートを取得するには「品質レポート取得」ボタンをクリックしてください。")

# ── Registry 一覧 ──────────────────────────────────
st.divider()
st.subheader("Series Registry")

@st.cache_data(ttl=60)
def fetch_registry():
    try:
        r = requests.get(f"{API_BASE}/data-quality/registry", timeout=10)
        records = r.json().get("data", [])
        if records:
            return pd.DataFrame(records)
    except Exception:
        pass
    return pd.DataFrame()

reg_df = fetch_registry()
if not reg_df.empty:
    if "last_updated" in reg_df.columns:
        reg_df["last_updated"] = reg_df["last_updated"].apply(
            lambda x: to_jst(pd.Timestamp(x)) if pd.notna(x) and x else ""
        )
    st.dataframe(reg_df, hide_index=True, use_container_width=True)
