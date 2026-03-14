"""
Memo Builder
- 研究メモ作成 / 保存 / Markdown エクスポート
- as_of / sources / vintage_mode / missing_evidence を自動付与
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import streamlit as st
import requests
import pandas as pd
from datetime import date, timedelta
from tz_utils import to_jst

API_BASE = "http://localhost:8000/api/v1"

st.set_page_config(page_title="Memo Builder", layout="wide")
st.title("Memo Builder")

# ── サイドバー：分析設定 ───────────────────────────
with st.sidebar:
    st.header("分析設定")
    pair = st.selectbox("通貨ペア", ["USDJPY", "EURUSD", "AUDUSD"])
    date_start = st.date_input("開始日", value=date.today() - timedelta(days=365))
    date_end   = st.date_input("終了日", value=date.today())
    vintage_mode = st.toggle("Vintage モード", value=False)

    ALL_FEATURES_MAP = {
        "USDJPY": ["spot","us_policy","jp_policy","us_3m","jp_3m","us_10y","jp_10y",
                   "vix","usd_broad","boj_assets","us_reer","jp_reer"],
        "EURUSD": ["spot","us_policy","ez_policy","us_3m","ez_3m","us_10y","ez_10y",
                   "vix","usd_broad","us_reer","ez_reer"],
        "AUDUSD": ["spot","us_policy","au_policy","au_3m","us_3m","au_10y","us_10y",
                   "vix","usd_broad","oil","us_reer","au_reer"],
    }
    DEFAULT_FEATURES_MAP = {
        "USDJPY": ["spot","us_policy","jp_policy","us_3m","jp_3m","us_10y","jp_10y","vix","usd_broad"],
        "EURUSD": ["spot","us_policy","ez_policy","us_3m","ez_3m","us_10y","ez_10y","vix","usd_broad"],
        "AUDUSD": ["spot","us_policy","au_policy","au_3m","us_3m","au_10y","us_10y","vix","usd_broad","oil"],
    }
    selected_factors = st.multiselect(
        "使用する因子", ALL_FEATURES_MAP[pair], default=DEFAULT_FEATURES_MAP[pair]
    )
    load_regime = st.checkbox("レジームサマリーを取得", value=True)

# ── 過去メモ一覧 ───────────────────────────────────
with st.expander("保存済みメモ一覧"):
    try:
        r = requests.get(f"{API_BASE}/memo/list", timeout=5)
        memos = r.json().get("data", [])
        if memos:
            memo_df = pd.DataFrame(memos)
            if "created_at" in memo_df.columns:
                memo_df["created_at"] = pd.to_datetime(memo_df["created_at"]).apply(lambda x: to_jst(x) if pd.notna(x) else "")
            st.dataframe(memo_df[["memo_id","pair","as_of","research_question","created_at"]],
                         hide_index=True, use_container_width=True)
            sel_id = st.selectbox("メモを読み込む", ["-- 選択 --"] + [m["memo_id"] for m in memos])
            if sel_id != "-- 選択 --" and st.button("読み込み"):
                mr = requests.get(f"{API_BASE}/memo/{sel_id}", timeout=5)
                loaded = mr.json().get("data", {})
                st.session_state.update({
                    "research_question": loaded.get("research_question", ""),
                    "hypothesis":        loaded.get("hypothesis", ""),
                    "observations":      loaded.get("observations", ""),
                    "uncertainty":       loaded.get("uncertainty", ""),
                    "next_experiment":   loaded.get("next_experiment", ""),
                    "risk_controls":     loaded.get("risk_controls", ""),
                })
                st.success(f"メモ {sel_id} を読み込みました")
        else:
            st.info("保存済みメモなし")
    except Exception as e:
        st.warning(f"メモ取得失敗: {e}")

st.divider()

# ── メモ入力フォーム ───────────────────────────────
st.subheader("研究メモ作成")

research_question = st.text_area(
    "Research Question（何を調べているか）",
    value=st.session_state.get("research_question", f"{pair} の動きを説明する主要因子は何か？"),
    height=80,
)
hypothesis = st.text_area(
    "Hypothesis（仮説）",
    value=st.session_state.get("hypothesis", f"{pair} は短期金利差と VIX に有意に反応する。"),
    height=80,
)
observations = st.text_area(
    "Observations（観測事実）",
    value=st.session_state.get("observations", "分析期間中、carry_positive の局面が多く続いた。"),
    height=100,
)
uncertainty = st.text_area(
    "Uncertainty（不確実性・証拠不足）",
    value=st.session_state.get("uncertainty", "月次金利データのため日次の動きを説明しきれない。"),
    height=80,
)
next_experiment = st.text_area(
    "Next Experiment（次のステップ）",
    value=st.session_state.get("next_experiment", "rolling OLS で金利差の感応度が時変かどうか確認する。"),
    height=80,
)
risk_controls = st.text_area(
    "Risk Controls（リスク管理・留意点）",
    value=st.session_state.get("risk_controls", "改定値が多い系列は vintage mode で確認すること。"),
    height=80,
)

# ── レジームサマリー取得 ───────────────────────────
regime_summary = {}
missing_rates  = {}

if load_regime:
    if st.button("レジームを取得して反映"):
        with st.spinner("レジーム分析中..."):
            try:
                panel_r = requests.post(
                    f"{API_BASE}/panel/build",
                    json={"pair": pair, "date_start": str(date_start),
                          "date_end": str(date_end), "features": selected_factors},
                    timeout=30,
                )
                missing_rates = panel_r.json().get("data", {}).get("missing_rates", {})

                regime_r = requests.post(
                    f"{API_BASE}/regimes/tag",
                    json={"pair": pair, "date_start": str(date_start),
                          "date_end": str(date_end), "include_stats": True, "save": False},
                    timeout=30,
                )
                regime_data = regime_r.json().get("data", {})
                latest_regime = regime_data.get("regime_rows", [{}])[-1]
                regime_summary = {
                    "latest": latest_regime,
                    "stats": regime_data.get("regime_stats", {}),
                }
                st.session_state["regime_summary"] = regime_summary
                st.session_state["missing_rates"]  = missing_rates
                st.success("レジームサマリーを取得しました")
            except Exception as e:
                st.error(f"レジーム取得失敗: {e}")

regime_summary = st.session_state.get("regime_summary", {})
missing_rates  = st.session_state.get("missing_rates", {})

if regime_summary:
    with st.expander("レジームサマリー"):
        st.json(regime_summary)
if missing_rates:
    with st.expander("欠測率"):
        st.json(missing_rates)

# ── 保存 & エクスポート ────────────────────────────
st.divider()
col1, col2 = st.columns(2)

with col1:
    if st.button("メモを保存", type="primary", use_container_width=True):
        with st.spinner("保存中..."):
            payload = {
                "pair": pair,
                "date_start": str(date_start),
                "date_end": str(date_end),
                "research_question": research_question,
                "hypothesis": hypothesis,
                "observations": observations,
                "uncertainty": uncertainty,
                "next_experiment": next_experiment,
                "risk_controls": risk_controls,
                "selected_factors": selected_factors,
                "vintage_mode": vintage_mode,
                "regime_summary": regime_summary,
                "missing_rates": missing_rates,
            }
            try:
                r = requests.post(f"{API_BASE}/memo/generate", json=payload, timeout=10)
                memo = r.json().get("data", {})
                st.success(f"保存しました (memo_id: {memo.get('memo_id')})")
                st.session_state["last_memo_id"] = memo.get("memo_id")
            except Exception as e:
                st.error(f"保存失敗: {e}")

with col2:
    memo_id = st.session_state.get("last_memo_id")
    if memo_id:
        try:
            md_r = requests.get(f"{API_BASE}/memo/{memo_id}/markdown", timeout=5)
            md_text = md_r.text
            st.download_button(
                "Markdown ダウンロード",
                md_text,
                f"memo_{memo_id}.md",
                "text/markdown",
                use_container_width=True,
            )
        except Exception:
            pass
    else:
        st.button("Markdown ダウンロード", disabled=True, use_container_width=True)

# ── プレビュー ─────────────────────────────────────
st.divider()
with st.expander("Markdown プレビュー"):
    preview_lines = [
        f"# Research Memo: {pair}",
        f"**Period**: {date_start} → {date_end} | **Vintage**: {vintage_mode} | **Generated**: {to_jst(pd.Timestamp.utcnow())}",
        f"",
        f"## Research Question\n{research_question}",
        f"## Hypothesis\n{hypothesis}",
        f"## Observations\n{observations}",
        f"## Uncertainty\n{uncertainty}",
        f"## Next Experiment\n{next_experiment}",
        f"## Risk Controls\n{risk_controls}",
    ]
    if missing_rates:
        stale = [f"{k}({v:.0%})" for k, v in missing_rates.items() if v > 0.3]
        if stale:
            preview_lines.append(f"\n### Missing Evidence\n" + "\n".join(f"- {s}" for s in stale))
    st.markdown("\n\n".join(preview_lines))
