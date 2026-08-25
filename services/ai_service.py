import json
import os
import re
from difflib import get_close_matches
from typing import Optional, List, Dict, Any
from services.metrics_service import _sql_quote, build_server_profile

try:
    from databricks.vector_search.client import VectorSearchClient
except ImportError:
    VectorSearchClient = None

from db.connection import run_query
from services.llm_service import chat_completion


# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------
VECTOR_ENDPOINT = os.getenv(
    "VECTOR_SEARCH_ENDPOINT",
    "sql-observability-vector-endpoint",
).strip()

VECTOR_INDEX = os.getenv(
    "VECTOR_SEARCH_INDEX",
    "btris_dbx.observability.sql-diag-vector-index",
).strip()


# ---------------------------------------------------------
# Registry helpers
# ---------------------------------------------------------
def _get_all_servers() -> List[str]:
    df = run_query("""
        SELECT DISTINCT server_name
        FROM btris_dbx.observability.sql_diagnostics_files_delta
        ORDER BY server_name
    """)
    if df.empty or "server_name" not in df.columns:
        return []
    return [str(x) for x in df["server_name"].dropna().astype(str).tolist()]


def _get_ingestion_dates_for_server(server_name: str) -> List[str]:
    df = run_query(f"""
        SELECT DISTINCT CAST(ingestion_date AS STRING) AS ingestion_date
        FROM btris_dbx.observability.sql_diagnostics_files_delta
        WHERE server_name = '{_sql_quote(server_name)}'
        ORDER BY ingestion_date DESC
    """)
    if df.empty or "ingestion_date" not in df.columns:
        return []
    return [str(x) for x in df["ingestion_date"].dropna().astype(str).tolist()]


def _get_global_ingestion_dates() -> List[str]:
    df = run_query("""
        SELECT DISTINCT CAST(ingestion_date AS STRING) AS ingestion_date
        FROM btris_dbx.observability.sql_diagnostics_files_delta
        ORDER BY ingestion_date DESC
    """)
    if df.empty or "ingestion_date" not in df.columns:
        return []
    return [str(x) for x in df["ingestion_date"].dropna().astype(str).tolist()]


# ---------------------------------------------------------
# Text normalization / parsing
# ---------------------------------------------------------
def _normalize_text(s: str) -> str:
    return re.sub(r"[^a-z0-9]", "", (s or "").lower())


def _extract_explicit_dates(question: str) -> List[str]:
    if not question:
        return []
    return re.findall(r"\b(20\d{2}-\d{2}-\d{2})\b", question)


# ---------------------------------------------------------
# Server resolution
# ---------------------------------------------------------
def _resolve_server_from_question(question: str, selected_server: Optional[str]) -> Optional[str]:
    servers = _get_all_servers()
    if not servers:
        return selected_server

    q = question or ""
    q_lower = q.lower()
    q_norm = _normalize_text(q)

    # 1) Exact contains match
    exact_matches = [s for s in servers if s.lower() in q_lower]
    if exact_matches:
        exact_matches.sort(key=len, reverse=True)
        return exact_matches[0]

    # 2) Normalized contains match
    normalized_matches = [s for s in servers if _normalize_text(s) in q_norm]
    if normalized_matches:
        normalized_matches.sort(key=len, reverse=True)
        return normalized_matches[0]

    # 3) Token / partial match
    tokens = re.findall(r"[a-zA-Z0-9_-]+", q_lower)
    partial_hits = []
    for s in servers:
        s_lower = s.lower()
        score = 0
        for t in tokens:
            if len(t) >= 4 and t in s_lower:
                score += len(t)
        if score > 0:
            partial_hits.append((score, s))
    if partial_hits:
        partial_hits.sort(reverse=True)
        return partial_hits[0][1]

    # 4) Fuzzy match
    server_norm_map = {_normalize_text(s): s for s in servers}
    fuzzy = get_close_matches(q_norm, list(server_norm_map.keys()), n=1, cutoff=0.55)
    if fuzzy:
        return server_norm_map[fuzzy[0]]

    return selected_server


def _resolve_servers_for_compare(question: str, selected_server: Optional[str]) -> List[str]:
    servers = _get_all_servers()
    if not servers:
        return [selected_server] if selected_server else []

    q_lower = (question or "").lower()
    found: List[str] = []

    for s in servers:
        if s.lower() in q_lower and s not in found:
            found.append(s)

    if len(found) < 2:
        norm_map = {_normalize_text(s): s for s in servers}
        tokens = re.findall(r"[a-zA-Z0-9_-]+", q_lower)
        for token in tokens:
            if len(token) < 4:
                continue
            fuzzy = get_close_matches(_normalize_text(token), list(norm_map.keys()), n=2, cutoff=0.65)
            for f in fuzzy:
                s = norm_map[f]
                if s not in found:
                    found.append(s)

    if not found and selected_server:
        found = [selected_server]

    return found[:2]


# ---------------------------------------------------------
# Date resolution
# ---------------------------------------------------------
def _resolve_single_ingestion_date(
    question: str,
    resolved_server: Optional[str],
    selected_ingestion_date: Optional[str],
) -> Optional[str]:
    q = (question or "").lower()
    explicit_dates = _extract_explicit_dates(question)

    if explicit_dates:
        return explicit_dates[0]

    dates = (
        _get_ingestion_dates_for_server(resolved_server)
        if resolved_server else _get_global_ingestion_dates()
    )
    if not dates:
        return selected_ingestion_date

    if "latest ingestion" in q or "latest snapshot" in q or re.search(r"\blatest\b", q):
        return dates[0]

    if "previous ingestion" in q or "previous snapshot" in q or "prior ingestion" in q:
        return dates[1] if len(dates) > 1 else dates[0]

    if "last week" in q:
        from datetime import datetime
        try:
            latest_dt = datetime.strptime(dates[0], "%Y-%m-%d")
            for d in dates[1:]:
                dt = datetime.strptime(d, "%Y-%m-%d")
                if (latest_dt - dt).days >= 7:
                    return d
            return dates[1] if len(dates) > 1 else dates[0]
        except Exception:
            return dates[1] if len(dates) > 1 else dates[0]

    return selected_ingestion_date or (dates[0] if dates else None)


def _resolve_compare_dates(
    question: str,
    resolved_server: Optional[str],
    selected_ingestion_date: Optional[str],
) -> Optional[List[str]]:
    q = (question or "").lower()
    explicit_dates = _extract_explicit_dates(question)

    if "compare" in q and len(explicit_dates) >= 2:
        return explicit_dates[:2]

    dates = (
        _get_ingestion_dates_for_server(resolved_server)
        if resolved_server else _get_global_ingestion_dates()
    )
    if len(dates) < 2:
        return None

    if "compare" in q and ("latest" in q or "previous" in q or "last week" in q):
        if "last week" in q:
            second = _resolve_single_ingestion_date("last week", resolved_server, selected_ingestion_date)
            if second and second != dates[0]:
                return [dates[0], second]
        return [dates[0], dates[1]]

    return None


# ---------------------------------------------------------
# Intent detection + semantic sheet weighting
# ---------------------------------------------------------
def _is_non_diagnostic_chat(question: str) -> bool:
    q = (question or "").strip().lower()
    if not q:
        return True

    simple_chat = {
        "hi", "hello", "hey", "thanks", "thank you", "ok", "okay",
        "good morning", "good afternoon", "good evening"
    }
    if q in simple_chat:
        return True

    diagnostic_shortcuts = {"cpu", "io", "i/o", "ram", "ple"}
    if len(q) <= 3 and q.isalpha() and q not in diagnostic_shortcuts:
        return True

    return False

def _detect_query_intent(question: str) -> str:
    q = (question or "").lower().strip()

    if "compare" in q:
        return "compare"

    general_knowledge_signals = [
        "what is ",
        "what's ",
        "explain ",
        "meaning of ",
        "define ",
        "how does ",
        "difference between ",
    ]
    if any(sig in q for sig in general_knowledge_signals):
        return "general"

    if any(x in q for x in ["cpu", "scheduler", "worker time"]):
        return "cpu"
    if any(x in q for x in ["io", "latency", "read", "write", "disk", "iops"]):
        return "io"
    if any(x in q for x in ["wait", "blocking", "lock", "latch", "cxpacket", "cxconsumer", "pageiolatch"]):
        return "waits"
    if any(x in q for x in ["query", "queries", "procedure", "stored procedure", "index", "logical reads", "elapsed"]):
        return "queries"
    if any(x in q for x in ["memory", "ple", "grant", "buffer", "cache"]):
        return "memory"
    if "tempdb" in q:
        return "tempdb"
    if any(x in q for x in ["config", "maxdop", "cost threshold", "server memory", "setting"]):
        return "config"

    return "general"


def _sheet_boost_keywords(intent: str) -> List[str]:
    mapping = {
        "cpu": ["worker", "cpu", "scheduler"],
        "io": ["io", "logical reads", "physical reads", "latency", "write", "read"],
        "waits": ["wait", "blocking", "latch", "lock"],
        "queries": ["query", "queries", "statement", "procedure", "stored procedure", "index"],
        "memory": ["memory", "ple", "grant", "buffer", "cache"],
        "tempdb": ["tempdb"],
        "config": ["config", "maxdop", "cost threshold", "server memory"],
        "general": [],
        "compare": [],
    }
    return mapping.get(intent, [])


def _sheet_weight(sheet_name: str, intent: str) -> int:
    s = (sheet_name or "").lower()
    score = 0

    for kw in _sheet_boost_keywords(intent):
        if kw in s:
            score += 10

    if "top" in s:
        score += 2
    if "wait" in s:
        score += 2
    if "query" in s or "statement" in s or "procedure" in s:
        score += 2
    if "memory" in s:
        score += 1
    if "config" in s:
        score += 1

    return score


# ---------------------------------------------------------
# Vector retrieval
# ---------------------------------------------------------
def _search_vector_index(
    question: str,
    filters: Dict[str, Any],
    num_results: int = 12,
) -> List[Dict[str, Any]]:
    if VectorSearchClient is None:
        return []

    host = os.getenv("DATABRICKS_HOST", "").strip()
    if host and not host.startswith(("http://", "https://")):
        host = f"https://{host}"

    token = os.getenv("DATABRICKS_TOKEN", "").strip()
    client_id = os.getenv("DATABRICKS_CLIENT_ID", "").strip()
    client_secret = os.getenv("DATABRICKS_CLIENT_SECRET", "").strip()

    if not host:
        raise RuntimeError("DATABRICKS_HOST is not set in the app environment.")

    if not VECTOR_INDEX:
        raise RuntimeError("VECTOR_SEARCH_INDEX is not configured in the app environment.")

    if not VECTOR_ENDPOINT:
        raise RuntimeError("VECTOR_SEARCH_ENDPOINT is not configured in the app environment.")

    if client_id and client_secret:
        vsc = VectorSearchClient(
            workspace_url=host,
            service_principal_client_id=client_id,
            service_principal_client_secret=client_secret,
            disable_notice=True,
        )
    elif token:
        vsc = VectorSearchClient(
            workspace_url=host,
            personal_access_token=token,
            disable_notice=True,
        )
    else:
        raise RuntimeError(
            "Vector Search auth is not configured. "
            "Expected app-provided service principal credentials or DATABRICKS_TOKEN."
        )

    index = vsc.get_index(
        endpoint_name=VECTOR_ENDPOINT,
        index_name=VECTOR_INDEX,
    )

    resp = index.similarity_search(
        query_text=question,
        columns=[
            "server_name",
            "snapshot_date",
            "ingestion_date",
            "sheet_name",
            "content",
        ],
        filters=filters,
        num_results=num_results,
    )

    result_rows: List[Dict[str, Any]] = []
    manifest_cols: List[str] = []

    if isinstance(resp, dict):
        manifest = resp.get("manifest", {})
        cols = manifest.get("columns", [])
        manifest_cols = [c.get("name") for c in cols if isinstance(c, dict)]

        data = resp.get("result", {}).get("data_array", [])
        for row in data:
            if manifest_cols and len(manifest_cols) == len(row):
                result_rows.append(dict(zip(manifest_cols, row)))

    return result_rows


def _rerank_rows_by_intent(rows: List[Dict[str, Any]], intent: str, top_k: int = 6) -> List[Dict[str, Any]]:
    scored = []
    for r in rows:
        score = _sheet_weight(str(r.get("sheet_name") or ""), intent)
        scored.append((score, r))
    scored.sort(key=lambda x: x[0], reverse=True)
    return [r for _, r in scored[:top_k]]


# ---------------------------------------------------------
# Prompt builders with diagnostic reasoning chains
# ---------------------------------------------------------
def _json_safe(obj: Any) -> Any:
    if obj is None:
        return None

    if isinstance(obj, (str, bool, int, float)):
        return obj

    try:
        import numpy as np  # type: ignore
        if isinstance(obj, (np.integer,)):
            return int(obj)
        if isinstance(obj, (np.floating,)):
            return float(obj)
        if isinstance(obj, (np.bool_,)):
            return bool(obj)
    except Exception:
        pass

    if isinstance(obj, dict):
        return {str(k): _json_safe(v) for k, v in obj.items()}

    if isinstance(obj, (list, tuple)):
        return [_json_safe(x) for x in obj]

    try:
        if hasattr(obj, "item"):
            return obj.item()
    except Exception:
        pass

    return str(obj)
def _build_prompt_single_scope(
    question: str,
    intent: str,
    resolved_server: Optional[str],
    resolved_ingestion_date: Optional[str],
    rows: List[Dict[str, Any]],
) -> str:
    context = []
    for r in rows:
        context.append(
            {
                "server_name": r.get("server_name"),
                "snapshot_date": r.get("snapshot_date"),
                "ingestion_date": r.get("ingestion_date"),
                "sheet_name": r.get("sheet_name"),
                "content": r.get("content"),
            }
        )

    deterministic_profile: Dict[str, Any] = {}
    if resolved_server:
        try:
            deterministic_profile = build_server_profile(
                resolved_server,
                resolved_ingestion_date,
            )
        except Exception as e:
            deterministic_profile = {
                "profile_error": str(e),
                "server_name": resolved_server,
                "ingestion_date": resolved_ingestion_date,
            }

    compact_profile = _json_safe({
        "server_name": deterministic_profile.get("server_name"),
        "snapshot": deterministic_profile.get("snapshot"),
        "instance": deterministic_profile.get("instance", {}),
        "utilization": deterministic_profile.get("utilization", {}),
        "pressure": deterministic_profile.get("pressure", {}),
        "configuration": deterministic_profile.get("configuration", {}),
        "io_stats": deterministic_profile.get("io_stats", {}),
        "wait_summary": deterministic_profile.get("wait_summary", {}),
        "top_waits": deterministic_profile.get("top_waits", [])[:8],
        "query_hotspots": deterministic_profile.get("query_hotspots", [])[:8],
        "database_settings": deterministic_profile.get("database_settings", {}),
        "notes": deterministic_profile.get("notes", []),
    })

    return f"""
    You are answering a SQL Server observability question for a selected server snapshot.

    Use deterministic server context as the primary source of truth.
    Use retrieved diagnostics snippets as supporting evidence.
    Do not invent metrics, waits, queries, dates, or configuration values.
    If evidence is partial, missing, or conflicting, say so clearly.

    Answer behavior rules:
    - Match the shape of the user's question.
    - Do not force the same structure every time.
    - If the question is simple, answer simply.
    - If the question asks "why", explain the most likely cause using the available evidence.
    - If the question asks for recommendation, prioritize concrete next actions.
    - If the question asks for interpretation, focus on interpretation rather than generic remediation.
    - Prefer concise, direct language over consultant-style repetition.

    Resolved scope:
    - Server: {resolved_server or "not specified"}
    - Ingestion Date: {resolved_ingestion_date or "not specified"}
    - Intent: {intent}

    User question:
    {question}

    Deterministic server context:
    {json.dumps(compact_profile, ensure_ascii=False, indent=2)}

    Retrieved diagnostics snippets:
    {json.dumps(context, ensure_ascii=False, indent=2)}
    """.strip()

def _build_prompt_compare_scope(
    question: str,
    intent: str,
    compare_servers: List[str],
    compare_dates: List[str],
    left_rows: List[Dict[str, Any]],
    right_rows: List[Dict[str, Any]],
) -> str:
    left_context = []
    right_context = []

    for r in left_rows:
        left_context.append(
            {
                "server_name": r.get("server_name"),
                "snapshot_date": r.get("snapshot_date"),
                "ingestion_date": r.get("ingestion_date"),
                "sheet_name": r.get("sheet_name"),
                "content": r.get("content"),
            }
        )

    for r in right_rows:
        right_context.append(
            {
                "server_name": r.get("server_name"),
                "snapshot_date": r.get("snapshot_date"),
                "ingestion_date": r.get("ingestion_date"),
                "sheet_name": r.get("sheet_name"),
                "content": r.get("content"),
            }
        )

    left_profile: Dict[str, Any] = {}
    right_profile: Dict[str, Any] = {}

    try:
        if len(compare_servers) == 2:
            left_profile = build_server_profile(
                compare_servers[0],
                compare_dates[0] if compare_dates else None,
            )
            right_profile = build_server_profile(
                compare_servers[1],
                compare_dates[1] if len(compare_dates) > 1 else compare_dates[0] if compare_dates else None,
            )
        elif len(compare_servers) == 1 and len(compare_dates) >= 2:
            left_profile = build_server_profile(compare_servers[0], compare_dates[0])
            right_profile = build_server_profile(compare_servers[0], compare_dates[1])
    except Exception as e:
        left_profile = {"profile_error": str(e)}
        right_profile = {"profile_error": str(e)}

    def _compact(profile: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "server_name": profile.get("server_name"),
            "snapshot": profile.get("snapshot"),
            "instance": profile.get("instance", {}),
            "utilization": profile.get("utilization", {}),
            "pressure": profile.get("pressure", {}),
            "configuration": profile.get("configuration", {}),
            "io_stats": profile.get("io_stats", {}),
            "wait_summary": profile.get("wait_summary", {}),
            "top_waits": profile.get("top_waits", [])[:8],
            "query_hotspots": profile.get("query_hotspots", [])[:8],
            "database_settings": profile.get("database_settings", {}),
            "notes": profile.get("notes", []),
        }

    left_compact = _json_safe(_compact(left_profile))
    right_compact = _json_safe(_compact(right_profile))

    return f"""
You are answering a comparison question for SQL Server observability.

Use deterministic server context as the primary comparison basis.
Use retrieved diagnostics snippets as supporting evidence.
Do not invent metrics, waits, queries, dates, or configuration values.
If evidence is partial, missing, or conflicting, say so clearly.

Comparison answer rules:
- Match the shape of the user's question.
- Do not force the same comparison template every time.
- Focus on the most material differences only.
- If the user asks a narrow comparison question, answer narrowly.
- If one side is clearly worse, say so directly and explain why.
- Prefer concise, evidence-based language over generic consultant phrasing.

Comparison scope:
- Servers: {", ".join(compare_servers) if compare_servers else "not specified"}
- Date A: {compare_dates[0] if len(compare_dates) > 0 else "not specified"}
- Date B: {compare_dates[1] if len(compare_dates) > 1 else "not specified"}
- Intent: {intent}

User question:
{question}

Deterministic context for scope A:
{json.dumps(left_compact, ensure_ascii=False, indent=2)}

Deterministic context for scope B:
{json.dumps(right_compact, ensure_ascii=False, indent=2)}

Retrieved diagnostics snippets for scope A:
{json.dumps(left_context, ensure_ascii=False, indent=2)}

Retrieved diagnostics snippets for scope B:
{json.dumps(right_context, ensure_ascii=False, indent=2)}
""".strip()


# ---------------------------------------------------------
# LLM call
# ---------------------------------------------------------

# ---------------------------------------------------------
# No-vector Agent profile context
# ---------------------------------------------------------
def _compact_profile_for_agent(profile: Dict[str, Any]) -> Dict[str, Any]:
    """Return the deterministic server evidence the Agent should know without vector search."""
    return _json_safe({
        "server_name": profile.get("server_name") or profile.get("server"),
        "snapshot": profile.get("snapshot"),
        "instance": profile.get("instance", {}),
        "utilization": profile.get("utilization", {}),
        "pressure": profile.get("pressure", {}),
        "configuration": profile.get("configuration", {}),
        "io_stats": profile.get("io_stats", {}),
        "wait_summary": profile.get("wait_summary", {}),
        "top_waits": (profile.get("top_waits") or [])[:8],
        "query_hotspots": (profile.get("query_hotspots") or [])[:8],
        "workload": profile.get("workload", {}),
        "backup_summary": profile.get("backup_summary", {}),
        "tempdb": profile.get("tempdb", {}),
        "database_settings": profile.get("database_settings", {}),
        "operational_health": profile.get("operational_health", {}),
        "database_distribution": profile.get("database_distribution", {}),
        "notes": profile.get("notes", []),
    })


def _build_agent_context(server_name: str, ingestion_date: Optional[str]) -> Dict[str, Any]:
    """Build active dashboard context from ingested diagnostics without Vector Search."""
    profile: Dict[str, Any]
    try:
        profile = build_server_profile(server_name, ingestion_date)
    except Exception as exc:
        profile = {
            "server_name": server_name,
            "snapshot": None,
            "profile_error": str(exc),
            "notes": [f"Server profile could not be loaded: {exc}"],
        }

    return {
        "scope": {
            "server_name": server_name,
            "ingestion_date": ingestion_date,
            "snapshot": profile.get("snapshot"),
        },
        "profile": _compact_profile_for_agent(profile),
    }


def _is_incomplete_agent_question(question: str) -> bool:
    q = (question or "").strip().lower()
    if not q:
        return True
    if q in {"what is", "what's", "why is", "how is", "can you", "tell me", "explain", "summarize"}:
        return True
    if len(q.split()) <= 2 and q in {"server", "database", "db", "query", "queries", "wait", "waits", "memory", "disk", "storage"}:
        return True
    return False


def _classify_agent_request(question: str, selected_server: Optional[str]) -> str:
    """
    Scope-aware Agent classifier.

    The selected dashboard scope is considered before generic English phrasing.
    For example, "what is the server name?" is a scope fact, not a generic SQL question.
    """
    q = (question or "").strip().lower()
    if not q:
        return "empty"
    if _is_non_diagnostic_chat(q):
        return "chat"
    if "compare" in q:
        return "server_compare"
    if _is_incomplete_agent_question(q):
        return "incomplete"

    scope_terms = [
        "this server", "selected server", "current server", "active server", "our server",
        "that server", "on this server", "for this server", "here", "current scope",
        "active scope", "this ingestion", "selected ingestion", "current ingestion",
    ]
    has_scope_language = any(term in q for term in scope_terms)
    mentions_selected = bool(selected_server and selected_server.lower() in q)

    if any(term in q for term in [
        "server name", "which server", "selected server", "current server", "active server",
        "ingestion date", "snapshot", "scope", "what server", "version", "edition", "sql banner",
    ]):
        return "scope_fact"

    if any(term in q for term in [
        "summarize", "summary", "overview", "tell me about", "what do you know",
        "health", "status", "posture", "state of", "about this server",
    ]):
        return "server_summary"

    if any(term in q for term in [
        "risk", "risks", "issue", "issues", "problem", "problems", "concern",
        "concerns", "wrong", "bad", "bottleneck", "bottlenecks", "investigate",
        "next", "recommend", "recommendation", "action", "fix", "remediate",
    ]):
        return "server_recommendation"

    if any(term in q for term in [
        "cpu", "scheduler", "worker time", "io", "i/o", "latency", "read", "write",
        "disk", "iops", "wait", "blocking", "lock", "latch", "cxpacket",
        "cxconsumer", "pageiolatch", "query", "queries", "procedure", "stored procedure",
        "index", "logical reads", "elapsed", "memory", "ple", "grant", "buffer",
        "cache", "tempdb", "config", "configuration", "maxdop", "cost threshold",
        "server memory", "setting", "backup", "page verify", "database",
    ]):
        return "server_metric" if (selected_server or has_scope_language or mentions_selected) else "general_sql"

    general_knowledge_signals = [
        "what is ", "what's ", "define ", "meaning of ", "how does ",
        "difference between ", "best practice", "best practices",
    ]
    explicit_general_targets = [
        "sql server", "database engine", "t-sql", "transaction log", "isolation level",
        "clustered index", "nonclustered index",
    ]
    if any(sig in q for sig in general_knowledge_signals) and any(term in q for term in explicit_general_targets) and not has_scope_language:
        return "general_sql"

    # In this app, ambiguous questions after a scope is loaded should be interpreted
    # as questions about the active server unless clearly general SQL knowledge.
    return "server_summary" if selected_server else "general_sql"


def _answer_scope_fact(context: Dict[str, Any], question: str) -> str:
    scope = context.get("scope") or {}
    profile = context.get("profile") or {}
    inst = profile.get("instance") or {}
    q = (question or "").lower()

    server = scope.get("server_name") or profile.get("server_name") or "unknown"
    ingestion = scope.get("ingestion_date") or "not specified"
    snapshot = scope.get("snapshot") or profile.get("snapshot") or "not resolved"

    facts = []
    if "server" in q or "scope" in q or "name" in q:
        facts.append(f"The selected server is `{server}`.")
    if "ingestion" in q or "date" in q or "scope" in q:
        facts.append(f"The selected ingestion date is `{ingestion}`.")
    if "snapshot" in q or "scope" in q:
        facts.append(f"The resolved diagnostics snapshot is `{snapshot}`.")
    if any(term in q for term in ["edition", "version", "sql banner", "sql server"]):
        sql_desc = inst.get("sql_and_edition") or inst.get("sql_banner") or inst.get("edition")
        if sql_desc:
            facts.append(f"SQL Server evidence shows `{sql_desc}`.")

    if not facts:
        facts = [
            f"The selected server is `{server}`.",
            f"The selected ingestion date is `{ingestion}`.",
            f"The resolved diagnostics snapshot is `{snapshot}`.",
        ]

    return " ".join(facts)


def _build_agent_profile_prompt(question: str, intent: str, context: Dict[str, Any]) -> str:
    return f"""
You are the SQL Server Observability Agent for this dashboard.

The user is working inside an active dashboard scope. Use the provided server context as the source of truth.
Do not say you need the server name, ingestion date, purpose, or configuration if it is present in the context.
Do not invent metrics, waits, queries, dates, configuration values, database names, incidents, or root causes.
If evidence is partial, say what is missing and still answer from the available evidence.

Answer behavior rules:
- First infer the sense of the user's message, even if it is informal, incomplete, or starts with "what is" / "explain".
- If the user asks a simple fact, answer directly in 1-2 sentences.
- If the user asks for a summary, provide a concise server health summary using the profile.
- If the user asks what to investigate, prioritize concrete next checks from the evidence.
- If the user asks about a metric, interpret that metric in the context of this server snapshot.
- Keep the answer practical and scoped to this server.
- Format as plain text only. Do not use Markdown, asterisks for bullets or bold, or decorative symbols.
- If you need a list, use short numbered lines like "1. Investigate waits" instead of bullet symbols.

Detected intent: {intent}

User message:
{question}

Active server context JSON:
{json.dumps(_json_safe(context), ensure_ascii=False, indent=2)}
""".strip()


def _call_llm(prompt: str, mode: str = "diagnostic") -> str:
    if mode == "chat":
        system_text = (
            "You are the SQL Server Observability Agent inside a SQL Server observability app. "
            "Reply naturally and briefly. "
            "Use plain text only: do not use Markdown, asterisks for bullets or bold, or decorative symbols. "
            "Do not use diagnostic report structure. "
            "Do not invent server-specific evidence."
        )
    elif mode == "general":
        system_text = (
            "You are a senior SQL Server expert. "
            "Answer clearly, directly, and naturally. "
            "Use plain text only: do not use Markdown, asterisks for bullets or bold, or decorative symbols. "
            "Do not force a diagnostic report structure. "
            "Do not assume access to server-specific diagnostics unless explicitly provided."
        )
    else:
        system_text = (
            "You are a senior SQL Server performance engineer. "
            "Use only the provided diagnostics evidence. "
            "Use plain text only: do not use Markdown, asterisks for bullets or bold, or decorative symbols. "
            "Do not invent metrics, waits, queries, dates, or settings."
        )

    messages = [
        {
            "role": "system",
            "content": system_text,
        },
        {
            "role": "user",
            "content": prompt,
        },
    ]

    try:
        return chat_completion(
            messages,
            temperature=0.1,
            max_tokens=1800,
        )
    except Exception as e:
        return f"Agent analysis could not be generated. Error: {e}"


# ---------------------------------------------------------
# Public API
# ---------------------------------------------------------
def ask_server_ai(
    server_name: str,
    ingestion_date: str,
    question: str,
    num_results: int = 12,
) -> Dict[str, Any]:
    q = (question or "").strip()
    selected_server = (server_name or "").strip()
    selected_ingestion = (ingestion_date or "").strip() or None

    if not q:
        return {
            "answer": "Please enter a question.",
            "found": False,
            "mode": "empty",
            "resolved_server": selected_server or None,
            "resolved_ingestion_date": selected_ingestion,
            "compare_servers": [],
            "compare_dates": [],
        }

    request_kind = _classify_agent_request(q, selected_server or None)
    q_lower = q.lower()

    # -----------------------------------------------------
    # Mode 1: simple chat / lightweight conversational reply
    # -----------------------------------------------------
    if request_kind == "chat":
        q_norm = q.strip().lower()
        scope_suffix = ""
        if selected_server:
            scope_suffix = f" Current scope: `{selected_server}`"
            if selected_ingestion:
                scope_suffix += f" / `{selected_ingestion}`"
            scope_suffix += "."

        if q_norm in {"hi", "hello", "hey", "good morning", "good afternoon", "good evening"}:
            answer = f"Hi — ask me about this server, compare ingestions, or ask a general SQL Server question.{scope_suffix}"
        elif q_norm in {"thanks", "thank you"}:
            answer = "You’re welcome."
        elif q_norm in {"ok", "okay"}:
            answer = "Understood."
        else:
            answer = f"Ask me about this server, compare ingestions, or ask a general SQL Server question.{scope_suffix}"

        return {
            "answer": answer,
            "found": bool(selected_server),
            "mode": "agent_chat",
            "resolved_server": selected_server or None,
            "resolved_ingestion_date": selected_ingestion,
            "compare_servers": [],
            "compare_dates": [],
        }

    # -----------------------------------------------------
    # Mode 2: no-vector server-context path
    # -----------------------------------------------------
    if selected_server and request_kind in {
        "scope_fact",
        "server_summary",
        "server_recommendation",
        "server_metric",
        "incomplete",
    }:
        context = _build_agent_context(selected_server, selected_ingestion)

        if request_kind == "scope_fact":
            return {
                "answer": _answer_scope_fact(context, q),
                "found": True,
                "mode": "agent",
                "resolved_server": selected_server,
                "resolved_ingestion_date": selected_ingestion,
                "compare_servers": [],
                "compare_dates": [],
            }

        if request_kind == "incomplete":
            scope = context.get("scope") or {}
            server = scope.get("server_name") or selected_server
            snapshot = scope.get("snapshot") or "not resolved"
            answer = (
                f"I’m scoped to `{server}`"
                + (f" for ingestion `{selected_ingestion}`" if selected_ingestion else "")
                + f" (snapshot `{snapshot}`). Please finish the question, or ask for a server summary, highest risks, CPU, waits, I/O, memory, configuration, query hotspots, or next investigation steps."
            )
            return {
                "answer": answer,
                "found": True,
                "mode": "agent",
                "resolved_server": selected_server,
                "resolved_ingestion_date": selected_ingestion,
                "compare_servers": [],
                "compare_dates": [],
            }

        prompt = _build_agent_profile_prompt(q, request_kind, context)
        return {
            "answer": _call_llm(prompt, mode="diagnostic"),
            "found": True,
            "mode": "agent",
            "resolved_server": selected_server,
            "resolved_ingestion_date": selected_ingestion,
            "compare_servers": [],
            "compare_dates": [],
        }

    # -----------------------------------------------------
    # Mode 3: no-vector comparison path using deterministic profiles
    # -----------------------------------------------------
    if selected_server and request_kind == "server_compare":
        compare_servers = _resolve_servers_for_compare(q, selected_server)
        compare_dates = _resolve_compare_dates(
            q,
            compare_servers[0] if compare_servers else selected_server,
            selected_ingestion,
        )

        if compare_dates and len(compare_dates) == 2 and len(compare_servers) <= 1:
            resolved_server = compare_servers[0] if compare_servers else selected_server
            prompt = _build_prompt_compare_scope(
                question=q,
                intent="compare",
                compare_servers=[resolved_server],
                compare_dates=compare_dates,
                left_rows=[],
                right_rows=[],
            )
            return {
                "answer": _call_llm(prompt, mode="diagnostic"),
                "found": True,
                "mode": "agent_compare",
                "resolved_server": resolved_server,
                "resolved_ingestion_date": None,
                "compare_servers": [resolved_server],
                "compare_dates": compare_dates,
            }

        # If a complete comparison target is not clear, answer with active context and ask for the missing side.
        context = _build_agent_context(selected_server, selected_ingestion)
        prompt = _build_agent_profile_prompt(
            q,
            "server_compare",
            context,
        ) + "\n\nIf the comparison target is missing, ask for the missing server or ingestion date and still summarize the active scope briefly."
        return {
            "answer": _call_llm(prompt, mode="diagnostic"),
            "found": True,
            "mode": "agent",
            "resolved_server": selected_server,
            "resolved_ingestion_date": selected_ingestion,
            "compare_servers": compare_servers or [selected_server],
            "compare_dates": compare_dates,
        }

    # -----------------------------------------------------
    # Mode 4: legacy vector-enriched path for explicitly diagnostic queries
    # Optional enrichment only; falls back to no-vector context if retrieval is unavailable.
    # -----------------------------------------------------
    intent = _detect_query_intent(q)
    server_grounded_intents = {"cpu", "io", "waits", "queries", "memory", "tempdb"}

    if intent == "config":
        config_server_signals = [
            "this server",
            "selected server",
            "current server",
            "our server",
            "that server",
            "on this server",
            "for this server",
            "configured",
            "configuration here",
            "setting here",
            "current maxdop",
            "cost threshold here",
            "server memory here",
        ]
        mentions_selected_server = bool(selected_server and selected_server.lower() in q_lower)
        intent = "config_server" if any(sig in q_lower for sig in config_server_signals) or mentions_selected_server else "config_general"

    if selected_server and (intent in server_grounded_intents or intent == "config_server"):
        resolved_server = _resolve_server_from_question(q, selected_server)
        resolved_ingestion_date = _resolve_single_ingestion_date(
            question=q,
            resolved_server=resolved_server,
            selected_ingestion_date=selected_ingestion,
        )

        rows: List[Dict[str, Any]] = []
        if VectorSearchClient is not None:
            try:
                filters: Dict[str, Any] = {}
                if resolved_server:
                    filters["server_name"] = resolved_server
                if resolved_ingestion_date:
                    filters["ingestion_date"] = resolved_ingestion_date
                rows = _search_vector_index(q, filters, num_results=num_results)
                rows = _rerank_rows_by_intent(rows, intent, top_k=6)
            except Exception:
                rows = []

        if rows:
            prompt = _build_prompt_single_scope(
                question=q,
                intent=intent,
                resolved_server=resolved_server,
                resolved_ingestion_date=resolved_ingestion_date,
                rows=rows,
            )
        else:
            context = _build_agent_context(resolved_server or selected_server, resolved_ingestion_date)
            prompt = _build_agent_profile_prompt(q, intent, context)

        return {
            "answer": _call_llm(prompt, mode="diagnostic"),
            "found": True,
            "mode": "agent",
            "resolved_server": resolved_server,
            "resolved_ingestion_date": resolved_ingestion_date,
            "compare_servers": [],
            "compare_dates": [],
        }

    # -----------------------------------------------------
    # Mode 5: clearly general SQL / normal answer
    # -----------------------------------------------------
    general_prompt = f"""
You are a senior SQL Server expert.

Answer the user's question directly and naturally.
Do not force a diagnostic report structure.
Keep the answer clear, practical, and concise.
Format as plain text only. Do not use Markdown, asterisks for bullets or bold, or decorative symbols.
If a list is useful, use short numbered lines instead of bullet symbols.

User question:
{q}
""".strip()

    return {
        "answer": _call_llm(general_prompt, mode="general"),
        "found": False,
        "mode": "agent",
        "resolved_server": selected_server or None,
        "resolved_ingestion_date": selected_ingestion,
        "compare_servers": [],
        "compare_dates": [],
    }
