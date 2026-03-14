"""
MemoService
- 研究メモの生成・保存・取得
- JSON + Markdown の両形式で出力
- as_of / sources / vintage_mode / missing_evidence を自動付与
"""

import json
import uuid
from datetime import date, datetime
from pathlib import Path
from typing import Optional

from app.core.config import settings
from app.core.logging import get_logger
from app.storage.duckdb import get_connection

logger = get_logger(__name__)

MEMO_DIR = Path("data/memos")


def generate_memo(
    pair: str,
    date_start: date,
    date_end: date,
    research_question: str,
    hypothesis: str,
    observations: str,
    uncertainty: str,
    next_experiment: str,
    risk_controls: str,
    selected_factors: list[str],
    vintage_mode: bool,
    regime_summary: Optional[dict] = None,
    missing_rates: Optional[dict] = None,
) -> dict:
    """
    研究メモ dict を生成する。
    自動付与: as_of, sources, vintage_mode, missing_evidence
    """
    memo_id = str(uuid.uuid4())[:8]
    now = datetime.utcnow()

    # missing_evidence: 欠測率 > 30% の factor を自動収集
    missing_evidence: list[str] = []
    if missing_rates:
        missing_evidence = [
            f"{k} (missing {v:.0%})"
            for k, v in missing_rates.items()
            if v > 0.3
        ]

    memo = {
        "memo_id": memo_id,
        "created_at_utc": now.isoformat(),
        "as_of": date.today().isoformat(),
        "pair": pair,
        "date_range": {"start": str(date_start), "end": str(date_end)},
        "vintage_mode": vintage_mode,
        "sources": _build_sources(pair, selected_factors),
        "research_question": research_question,
        "hypothesis": hypothesis,
        "observations": observations,
        "uncertainty": uncertainty,
        "next_experiment": next_experiment,
        "risk_controls": risk_controls,
        "selected_factors": selected_factors,
        "regime_summary": regime_summary or {},
        "missing_evidence": missing_evidence,
    }

    _save_memo(memo)
    return memo


def memo_to_markdown(memo: dict) -> str:
    lines = [
        f"# Research Memo: {memo['pair']}",
        f"",
        f"**Memo ID**: `{memo['memo_id']}`  ",
        f"**As of**: {memo['as_of']}  ",
        f"**Period**: {memo['date_range']['start']} → {memo['date_range']['end']}  ",
        f"**Vintage mode**: {memo['vintage_mode']}  ",
        f"",
        f"---",
        f"",
        f"## Research Question",
        f"",
        memo["research_question"],
        f"",
        f"## Hypothesis",
        f"",
        memo["hypothesis"],
        f"",
        f"## Observations",
        f"",
        memo["observations"],
        f"",
        f"## Uncertainty",
        f"",
        memo["uncertainty"],
        f"",
        f"## Next Experiment",
        f"",
        memo["next_experiment"],
        f"",
        f"## Risk Controls",
        f"",
        memo["risk_controls"],
        f"",
        f"---",
        f"",
        f"## Metadata",
        f"",
        f"**Selected factors**: {', '.join(memo['selected_factors'])}  ",
        f"**Sources**: {', '.join(memo['sources'])}  ",
    ]

    if memo.get("missing_evidence"):
        lines += [
            f"",
            f"### Missing Evidence",
            f"",
            *[f"- {m}" for m in memo["missing_evidence"]],
        ]

    if memo.get("regime_summary"):
        lines += [
            f"",
            f"### Regime Summary",
            f"",
            f"```json",
            json.dumps(memo["regime_summary"], indent=2, ensure_ascii=False),
            f"```",
        ]

    return "\n".join(lines)


def list_memos() -> list[dict]:
    conn = get_connection()
    try:
        rows = conn.execute(
            "SELECT memo_id, pair, as_of, research_question, created_at FROM memo_store ORDER BY created_at DESC"
        ).fetchall()
        return [
            {"memo_id": r[0], "pair": r[1], "as_of": str(r[2]),
             "research_question": r[3], "created_at": str(r[4])}
            for r in rows
        ]
    except Exception:
        return []


def get_memo(memo_id: str) -> Optional[dict]:
    MEMO_DIR.mkdir(parents=True, exist_ok=True)
    path = MEMO_DIR / f"{memo_id}.json"
    if not path.exists():
        return None
    return json.loads(path.read_text())


def _save_memo(memo: dict) -> None:
    MEMO_DIR.mkdir(parents=True, exist_ok=True)
    path = MEMO_DIR / f"{memo['memo_id']}.json"
    path.write_text(json.dumps(memo, ensure_ascii=False, indent=2))

    conn = get_connection()
    _ensure_memo_table(conn)
    try:
        conn.execute(
            """
            INSERT OR REPLACE INTO memo_store
            (memo_id, pair, as_of, research_question, content_json, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                memo["memo_id"], memo["pair"], memo["as_of"],
                memo["research_question"],
                json.dumps(memo, ensure_ascii=False),
                memo["created_at_utc"],
            ),
        )
        logger.info("memo_saved", memo_id=memo["memo_id"], pair=memo["pair"])
    except Exception as e:
        logger.warning("memo_db_save_failed", error=str(e))


def _ensure_memo_table(conn) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS memo_store (
            memo_id TEXT PRIMARY KEY,
            pair TEXT,
            as_of DATE,
            research_question TEXT,
            content_json TEXT,
            created_at TIMESTAMP
        )
    """)


def _build_sources(pair: str, factors: list[str]) -> list[str]:
    SOURCE_MAP = {
        "spot":       "FRED/" + {"USDJPY":"DEXJPUS","EURUSD":"DEXUSEU","AUDUSD":"DEXUSAL"}.get(pair, pair),
        "us_3m":      "FRED/IR3TIB01USM156N",
        "jp_3m":      "FRED/IR3TIB01JPM156N",
        "ez_3m":      "FRED/IR3TIB01EZM156N",
        "au_3m":      "FRED/IR3TIB01AUM156N",
        "us_10y":     "FRED/IRLTLT01USM156N",
        "jp_10y":     "FRED/IRLTLT01JPM156N",
        "ez_10y":     "FRED/IRLTLT01EZM156N",
        "au_10y":     "FRED/IRLTLT01AUM156N",
        "vix":        "FRED/VIXCLS",
        "usd_broad":  "FRED/DTWEXBGS",
        "oil":        "FRED/DCOILWTICO",
        "boj_assets": "FRED/JPNASSETS",
    }
    sources = set()
    for f in factors:
        src = SOURCE_MAP.get(f)
        if src:
            sources.add(src.replace("pair", pair))
    return sorted(sources)
