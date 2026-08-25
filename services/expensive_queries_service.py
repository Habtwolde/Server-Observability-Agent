# services/expensive_queries_service.py
from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st

from services import metrics_service
from services.llm_service import chat_completion


# ----------------------------
# Domain model
# ----------------------------
@dataclass(frozen=True)
class QueryTypeOption:
    label: str          # user friendly label shown in dropdown
    sheet_name: str     # exact bronze sheet_name
    kind: str           # cpu | io | elapsed | other


# ----------------------------
# Sheet discovery
# ----------------------------
_EXPENSIVE_SHEET_PATTERNS: List[Tuple[str, str]] = [
    # (regex, kind)
    (r"\bTop\s+Worker\s+Time\s+Queries\b", "cpu"),
    (r"\bTop\s+Logical\s+Reads\s+Queries\b", "io"),
    (r"\bTop\s+Avg\s+Elapsed\s+Time\b", "elapsed"),
    (r"\bTop\s+IO\s+Statements\b", "io"),
    # broader catch-alls (still expensive queries)
    (r"\bTop\s+.*\bQueries\b", "other"),
    (r"\bTop\s+.*\bStatements\b", "other"),
]


def _friendly_label(sheet_name: str, kind: str) -> str:
    s = str(sheet_name or "").strip()
    if kind == "cpu":
        return "Most expensive queries by CPU (Worker Time)"
    if kind == "io":
        # "logical reads" and "io statements" both land here
        if re.search(r"logical\s+reads", s, flags=re.IGNORECASE):
            return "Most expensive queries by IO (Logical Reads)"
        if re.search(r"\bIO\s+Statements\b", s, flags=re.IGNORECASE):
            return "Most expensive queries by IO (IO Statements)"
        return "Most expensive queries by IO"
    if kind == "elapsed":
        return "Slowest queries (Avg Elapsed Time)"
    # fall back to the raw sheet title for anything else
    return s


@st.cache_data(show_spinner=False, ttl=300)
def list_expensive_query_types(server_name: str) -> List[QueryTypeOption]:
    """
    Discover which 'Top ...' expensive-query-related sheets exist for a server.
    This is intentionally data-driven from delta, not hard-coded to 2 types.
    """
    if not server_name:
        return []

    available = metrics_service.list_available_sheets_any(server_name)
    if not available:
        return []

    picked: List[QueryTypeOption] = []
    seen = set()

    for sheet in available:
        sheet_str = str(sheet)
        kind = None
        for pat, k in _EXPENSIVE_SHEET_PATTERNS:
            if re.search(pat, sheet_str, flags=re.IGNORECASE):
                kind = k
                break
        if not kind:
            continue

        # de-dup exact sheet_name
        if sheet_str.lower() in seen:
            continue
        seen.add(sheet_str.lower())

        picked.append(
            QueryTypeOption(
                label=_friendly_label(sheet_str, kind),
                sheet_name=sheet_str,
                kind=kind,
            )
        )

    # Sorting: show the most important buckets first, then alphabetically
    priority = {"cpu": 0, "io": 1, "elapsed": 2, "other": 3}
    picked.sort(key=lambda o: (priority.get(o.kind, 9), o.label.lower(), o.sheet_name.lower()))
    return picked


# ----------------------------
# Data access
# ----------------------------
def _fetch_sheet_for_ingestion(
    server_name: str,
    sheet_name: str,
    ingestion_date: str,
) -> Tuple[pd.DataFrame, Optional[str]]:
    if not server_name or not sheet_name or not ingestion_date:
        return pd.DataFrame(), None

    q = f"""
    SELECT CAST(snapshot_date AS string) AS snapshot_date
    FROM btris_dbx.observability.sql_diagnostics_files_delta
    WHERE server_name = '{metrics_service._sql_quote(server_name)}'
      AND CAST(ingestion_date AS string) = '{metrics_service._sql_quote(ingestion_date)}'
    LIMIT 1
    """
    df_snap = metrics_service.run_query(q)

    if df_snap.empty or "snapshot_date" not in df_snap.columns:
        return pd.DataFrame(), None

    snapshot = str(df_snap["snapshot_date"].iloc[0])
    df = metrics_service._fetch_sheet(server_name, snapshot, sheet_name)
    return df, snapshot

@st.cache_data(show_spinner=False, ttl=300)
def fetch_latest_expensive_queries(
    server_name: str,
    sheet_name: str,
    ingestion_date: str | None = None,
) -> Tuple[pd.DataFrame, Optional[str]]:
    """
    Fetch expensive-query rows for the selected ingestion_date when provided.
    Falls back to the latest snapshot behavior when ingestion_date is not supplied.
    Returns (df, snapshot_used).
    """
    if not server_name or not sheet_name:
        return pd.DataFrame(), None

    if ingestion_date:
        df, snap = _fetch_sheet_for_ingestion(server_name, sheet_name, ingestion_date)
    else:
        df, snap = metrics_service._fetch_sheet_latest(server_name, sheet_name)

    if df is None or df.empty:
        return pd.DataFrame(), snap
    return df, snap

def pick_query_text_column(df: pd.DataFrame) -> Optional[str]:
    if df is None or df.empty:
        return None
    candidates = [
        "Short Query Text",
        "Query Text",
        "Statement Text",
        "SQL Text",
        "Text",
        "query_text",
        "short_query_text",
        "statement_text",
    ]
    return metrics_service._pick_column(list(df.columns), candidates)


def pick_sort_metric_column(df: pd.DataFrame, kind: str) -> Optional[str]:
    """
    Pick a metric column to sort by for 'top' ordering depending on query kind.
    """
    if df is None or df.empty:
        return None

    if kind == "cpu":
        cands = ["Total Worker Time", "Avg Worker Time", "Worker Time", "total_worker_time", "avg_worker_time"]
    elif kind == "io":
        cands = ["Total Logical Reads", "Avg Logical Reads", "Logical Reads", "total_logical_reads", "avg_logical_reads",
                 "Total Physical Reads", "Avg Physical Reads", "Physical Reads"]
    elif kind == "elapsed":
        cands = ["Avg Elapsed Time", "Total Elapsed Time", "Elapsed Time", "avg_elapsed_time", "total_elapsed_time"]
    else:
        cands = ["Total Worker Time", "Total Logical Reads", "Avg Elapsed Time", "Execution Count"]

    return metrics_service._pick_column(list(df.columns), cands)


def build_query_dropdown_items(df: pd.DataFrame, *, query_col: str, limit: int = 200) -> List[str]:
    """
    Build stable dropdown display strings for each row, keeping the row index.
    """
    if df is None or df.empty or not query_col:
        return []

    # Keep original ordering; caller may sort df prior to passing it here
    items: List[str] = []
    for idx, v in enumerate(df[query_col].astype(str).fillna("").tolist()):
        v = v.strip()
        if not v or v.lower() == "nan":
            v = "<blank query text>"
        # truncate but keep readable
        v_short = (v[:160] + "…") if len(v) > 160 else v
        items.append(f"{idx+1:03d} — {v_short}")
        if len(items) >= int(limit):
            break
    return items


# ----------------------------
# LLM query-analysis helpers
# ----------------------------
def _json_safe(value: Any) -> Any:
    if value is None:
        return None

    try:
        if pd.isna(value):
            return None
    except Exception:
        pass

    if isinstance(value, (str, bool, int, float)):
        return value

    if hasattr(value, "item"):
        try:
            scalar = value.item()
            if isinstance(scalar, (str, bool, int, float)) or scalar is None:
                return scalar
        except Exception:
            pass

    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(v) for v in value]

    return str(value)


def _compact_row(row: Dict[str, Any], max_fields: int = 80) -> Dict[str, Any]:
    compact: Dict[str, Any] = {}
    for idx, (key, value) in enumerate((row or {}).items()):
        if idx >= max_fields:
            break
        compact[str(key)] = _json_safe(value)
    return compact


def build_query_analysis_payload(
    *,
    server_name: str,
    ingestion_date: Optional[str],
    snapshot: Optional[str],
    query_type_label: str,
    sheet_name: str,
    query_kind: str,
    query_text: str,
    row: Dict[str, Any],
    metric_col: Optional[str],
    sort_value: Any,
    row_index: int,
    total_rows: int,
) -> Dict[str, Any]:
    """Build a compact, evidence-only payload for selected-query LLM analysis."""

    return {
        "server_name": server_name,
        "ingestion_date": ingestion_date,
        "snapshot": snapshot,
        "query_bucket": {
            "label": query_type_label,
            "sheet_name": sheet_name,
            "kind": query_kind,
        },
        "selection": {
            "row_index_1_based": int(row_index) + 1,
            "total_rows_loaded": int(total_rows),
            "sort_metric_column": metric_col,
            "sort_metric_value": _json_safe(sort_value),
        },
        "query_text": (query_text or "")[:12000],
        "row_metrics": _compact_row(row),
    }


def build_expensive_query_analysis_messages(payload: Dict[str, Any]) -> List[Dict[str, str]]:
    """Build the LLM prompt for selected expensive-query tuning analysis.

    The prompt deliberately asks for an optimized SQL alternative and a detailed
    recommendation narrative because the UI's selected-query workflow should go
    beyond generic triage and give a developer a concrete rewrite direction.
    """

    system = (
        "You are a senior SQL Server query tuning specialist for SQL Server 2022 and newer/Azure SQL. "
        "Analyze only the provided diagnostic row and SQL text. Do not invent execution-plan operators, "
        "indexes, row counts, wait stats, object names, parameter values, or database facts that are not present. "
        "When the SQL text is complete enough, propose the best-fit optimized T-SQL rewrite that preserves semantics. "
        "If semantics are ambiguous, provide a clearly labeled template rewrite and state the assumptions. "
        "Ground recommendations in modern SQL Server capabilities such as Query Store, Parameter Sensitive Plan optimization, "
        "memory grant feedback, cardinality-estimation/statistics validation, batch mode where applicable, and regression-safe testing."
    )

    user = f"""
Analyze this selected expensive-query diagnostic row and produce a developer-ready optimization readout.

Return Markdown with EXACTLY these sections, in this order:

## What stands out
- 3-6 bullets grounded in the provided metric names and values.

## Likely performance drivers
- Explain whether the row appears CPU-heavy, IO-heavy, latency-heavy, execution-count-heavy, or inconclusive from the evidence.
- Call out query-shape risks visible in the SQL text, such as non-SARGable predicates, SELECT *, implicit conversions, scalar UDFs/functions on columns, parameter-sensitive predicates, OR-heavy filters, DISTINCT/GROUP BY pressure, broad joins, missing TOP/order selectivity, key-lookups, spills, or memory grants only when supported by the supplied text/metrics.

## Recommended optimized query
- Provide the best-fit rewritten T-SQL in a fenced ```sql block.
- Preserve the original query semantics as much as possible. Prefer SARGable predicates, explicit column lists when inferable, safer date-range predicates, pre-aggregation or EXISTS where appropriate, and parameterization patterns that reduce plan instability.
- If the original SQL text is truncated or ambiguous, provide a template rewrite with TODO comments instead of pretending to know missing columns or joins.

## Detailed recommendation rationale
Write 2-4 well-designed paragraphs explaining why the rewrite and supporting actions should improve performance on the latest SQL Server generation. Discuss applicable modern behavior such as Parameter Sensitive Plan optimization vs classic parameter sniffing, Query Store baselines/plan regression checks, cardinality-estimation/statistics quality, memory grant feedback/spills, and index design trade-offs. Be specific to the observed query and metrics; do not give generic filler.

## Query tuning recommendations
- 5-8 prioritized DBA/developer actions. Include validation steps such as reviewing the actual execution plan, checking indexes/statistics, parameter sniffing or Parameter Sensitive Plan behavior, spills, memory grants, and regression-safe testing when applicable.

## Follow-up diagnostics to capture
- 4-7 specific follow-up checks or DMV/Query Store/plan-review actions.

## Evidence limitations
- State what cannot be concluded from this row alone.

Data payload:
{json.dumps(payload, ensure_ascii=False, indent=2)}
""".strip()

    return [{"role": "system", "content": system}, {"role": "user", "content": user}]


def analyze_expensive_query_with_llm(
    *,
    server_name: str,
    ingestion_date: Optional[str],
    snapshot: Optional[str],
    query_type_label: str,
    sheet_name: str,
    query_kind: str,
    query_text: str,
    row: Dict[str, Any],
    metric_col: Optional[str],
    sort_value: Any,
    row_index: int,
    total_rows: int,
) -> str:
    """Generate an evidence-grounded tuning readout for one selected expensive-query row."""

    payload = build_query_analysis_payload(
        server_name=server_name,
        ingestion_date=ingestion_date,
        snapshot=snapshot,
        query_type_label=query_type_label,
        sheet_name=sheet_name,
        query_kind=query_kind,
        query_text=query_text,
        row=row,
        metric_col=metric_col,
        sort_value=sort_value,
        row_index=row_index,
        total_rows=total_rows,
    )

    messages = build_expensive_query_analysis_messages(payload)

    return chat_completion(
        messages,
        temperature=0.1,
        max_tokens=2600,
    )


def answer_expensive_query_followup_with_llm(
    *,
    server_name: str,
    ingestion_date: Optional[str],
    snapshot: Optional[str],
    query_type_label: str,
    sheet_name: str,
    query_kind: str,
    query_text: str,
    row: Dict[str, Any],
    metric_col: Optional[str],
    sort_value: Any,
    row_index: int,
    total_rows: int,
    prior_analysis: str,
    question: str,
) -> str:
    """Answer a scoped follow-up question for the selected expensive-query row."""

    payload = build_query_analysis_payload(
        server_name=server_name,
        ingestion_date=ingestion_date,
        snapshot=snapshot,
        query_type_label=query_type_label,
        sheet_name=sheet_name,
        query_kind=query_kind,
        query_text=query_text,
        row=row,
        metric_col=metric_col,
        sort_value=sort_value,
        row_index=row_index,
        total_rows=total_rows,
    )

    system = (
        "You are a senior SQL Server query tuning specialist answering a follow-up question about one selected expensive-query row. "
        "Stay grounded in the supplied row, SQL text, and previous analysis. If the user asks for facts not present, say what additional evidence is needed. "
        "When the follow-up asks how to optimize the query, include a semantics-preserving T-SQL rewrite or a clearly labeled template, plus SQL Server 2022+/Azure SQL guidance for Query Store, Parameter Sensitive Plan optimization, memory grants, spills, and statistics/index validation where relevant."
    )

    user = f"""
Selected-query evidence:
{json.dumps(payload, ensure_ascii=False, indent=2)}

Previous analysis:
{(prior_analysis or '')[:8000]}

Follow-up question:
{question}

Answer in practical Markdown. Be specific, include optimized-query guidance when relevant, and do not fabricate missing evidence.
""".strip()

    return chat_completion(
        [{"role": "system", "content": system}, {"role": "user", "content": user}],
        temperature=0.1,
        max_tokens=1400,
    )
