"""Deterministic SQL query hotspot analysis for report generation.

This module intentionally does not call an LLM.  It turns extracted expensive-query
rows into an evidence-first contract that the report service can render directly or
pass to section-specific prompts.  The recommendations are methodology mappings,
not quotations or copied guidance from any third-party author/tool.
"""
from __future__ import annotations

import math
import re
from typing import Any, Dict, Iterable, List, Optional, Tuple


MetricMap = Dict[str, Optional[float]]


_METHOD_MATRIX = [
    {
        "methodology": "Glenn Berry / Dr DMV",
        "lens": "DMV evidence first: classify by worker time, logical reads, elapsed time, executions, waits, memory, and configuration context.",
        "report_use": "Use observed metrics and wait categories to choose the next diagnostic query or controlled tuning experiment.",
    },
    {
        "methodology": "Brent Ozar",
        "lens": "sp_BlitzCache-style triage: sort by reads, CPU, duration, executions, spills, and memory grants; prioritize highest operational impact first.",
        "report_use": "Frame each hotspot as a priority finding with evidence, impact, owner, how-to-stop-it actions, and validation.",
    },
    {
        "methodology": "Ola Hallengren",
        "lens": "Maintenance safety: verify backup/integrity posture, statistics freshness, and index-maintenance suitability before broad changes.",
        "report_use": "Convert query findings into safe maintenance or statistics/index validation tasks instead of blind rebuilds or unsupported DDL.",
    },
    {
        "methodology": "Microsoft",
        "lens": "Query Store and execution-plan validation: compare runtime stats/plans, measure before and after, and avoid operator-level claims without plan evidence.",
        "report_use": "Require actual execution plans, Query Store or DMV trend review, SET STATISTICS IO/TIME, and controlled rollback criteria.",
    },
]


_NUMERIC_RE = re.compile(r"[-+]?\d[\d,]*(?:\.\d+)?")


def _safe_number(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
            return None
        return float(value)
    text = str(value).strip()
    if not text or text.lower() in {"nan", "none", "null", "n/a", "--", "—"}:
        return None
    match = _NUMERIC_RE.search(text)
    if not match:
        return None
    try:
        return float(match.group(0).replace(",", ""))
    except ValueError:
        return None


def _clean_text(value: Any, *, max_len: int = 600) -> Optional[str]:
    if value is None:
        return None
    text = str(value).replace("\r", " ").replace("\n", " ").strip()
    text = re.sub(r"\s+", " ", text)
    if not text or text.lower() in {"nan", "none", "null"}:
        return None
    return text[:max_len]


def _first_text(row: Dict[str, Any], keys: Iterable[str], *, max_len: int = 600) -> Optional[str]:
    for key in keys:
        if key in row:
            text = _clean_text(row.get(key), max_len=max_len)
            if text:
                return text
    return None


def _first_number(row: Dict[str, Any], keys: Iterable[str]) -> Optional[float]:
    for key in keys:
        if key in row:
            value = _safe_number(row.get(key))
            if value is not None:
                return value
    return None


def _metric_map(row: Dict[str, Any]) -> MetricMap:
    return {
        "total_worker_time": _first_number(row, ["total_worker_time", "Total Worker Time", "Total Worker Time (ms)", "Worker Time"]),
        "avg_worker_time": _first_number(row, ["avg_worker_time", "Avg Worker Time", "Average Worker Time", "Avg Worker Time (ms)"]),
        "total_logical_reads": _first_number(row, ["total_logical_reads", "Total Logical Reads", "Logical Reads"]),
        "avg_logical_reads": _first_number(row, ["avg_logical_reads", "Avg Logical Reads", "Average Logical Reads", "Logical Reads/Execution"]),
        "total_elapsed_time": _first_number(row, ["total_elapsed_time", "Total Elapsed Time", "Elapsed Time", "Duration"]),
        "avg_elapsed_time": _first_number(row, ["avg_elapsed_time", "Avg Elapsed Time", "Average Elapsed Time", "Avg Elapsed Time (ms)", "avg_duration_ms", "duration_seconds"]),
        "execution_count": _first_number(row, ["execution_count", "Execution Count", "Executions", "execution_count_total"]),
        "total_physical_reads": _first_number(row, ["total_physical_reads", "Total Physical Reads", "Physical Reads"]),
        "total_logical_writes": _first_number(row, ["total_logical_writes", "Total Logical Writes", "Logical Writes", "Writes"]),
        "granted_memory_kb": _first_number(row, ["granted_memory_kb", "Granted Memory KB", "Max Grant KB", "Memory Grant KB"]),
        "used_memory_kb": _first_number(row, ["used_memory_kb", "Used Memory KB", "Max Used Grant KB", "Used Grant KB"]),
        "spills": _first_number(row, ["spills", "Spills", "Total Spills", "TempDB Spills"]),
        "tempdb_allocations": _first_number(row, ["tempdb_allocations", "TempDB Allocations", "Tempdb Allocations"]),
    }


def _metric_name(row: Dict[str, Any]) -> str:
    return str(row.get("metric_name") or row.get("primary_metric") or row.get("bucket") or "").lower()


def _dimension_from_metric(row: Dict[str, Any], metrics: MetricMap) -> str:
    marker = " ".join([_metric_name(row), str(row.get("bucket") or "").lower()])
    if any(token in marker for token in ["logical read", "read", "io"]):
        return "reads"
    if any(token in marker for token in ["worker", "cpu"]):
        return "cpu"
    if any(token in marker for token in ["elapsed", "duration", "latency"]):
        return "duration"
    if any(token in marker for token in ["execution", "executions", "frequency"]):
        return "executions"
    if any(token in marker for token in ["grant", "memory"]):
        return "memory"
    if any(token in marker for token in ["spill", "tempdb"]):
        return "tempdb"

    candidates: List[Tuple[str, float]] = []
    if metrics.get("total_logical_reads") is not None or metrics.get("avg_logical_reads") is not None:
        candidates.append(("reads", max(metrics.get("total_logical_reads") or 0, (metrics.get("avg_logical_reads") or 0) * 100)))
    if metrics.get("total_worker_time") is not None or metrics.get("avg_worker_time") is not None:
        candidates.append(("cpu", max(metrics.get("total_worker_time") or 0, (metrics.get("avg_worker_time") or 0) * 100)))
    if metrics.get("total_elapsed_time") is not None or metrics.get("avg_elapsed_time") is not None:
        candidates.append(("duration", max(metrics.get("total_elapsed_time") or 0, (metrics.get("avg_elapsed_time") or 0) * 100)))
    if metrics.get("execution_count") is not None:
        candidates.append(("executions", metrics.get("execution_count") or 0))
    if metrics.get("spills"):
        candidates.append(("tempdb", (metrics.get("spills") or 0) * 1000))
    if not candidates:
        return "unknown"
    candidates.sort(key=lambda item: item[1], reverse=True)
    return candidates[0][0]


def _context_flags(context: Dict[str, Any]) -> Dict[str, bool]:
    waits = context.get("waits") or []
    wait_names = {str(w.get("wait_type") or "").upper() for w in waits if isinstance(w, dict)}
    config = context.get("configuration") or {}
    utilization = context.get("utilization") or {}
    database_settings = context.get("database_settings") or {}
    backup = context.get("backup") or {}

    maxdop = _safe_number(config.get("maxdop"))
    ctfp = _safe_number(config.get("cost_threshold"))
    ple = _safe_number(utilization.get("ple_sec") or utilization.get("cache_ple_seconds"))
    cpu_pct = _safe_number(utilization.get("max_cpu_pct"))
    mem_pct = _safe_number(utilization.get("max_memory_pct"))

    return {
        "parallelism_waits": bool(wait_names.intersection({"CXPACKET", "CXCONSUMER"})),
        "io_waits": any(w.startswith("PAGEIOLATCH") or w in {"IOCOMPLETION", "ASYNC_IO_COMPLETION"} for w in wait_names),
        "tempdb_latch_waits": any(w.startswith("PAGELATCH") for w in wait_names),
        "cpu_pressure": bool(cpu_pct is not None and cpu_pct >= 85),
        "memory_pressure": bool((mem_pct is not None and mem_pct >= 85) or (ple is not None and ple < 300)),
        "default_parallelism_risk": bool((maxdop is not None and maxdop == 0) or (ctfp is not None and ctfp <= 5)),
        "page_verify_gap": bool((database_settings.get("user_db_none_count") or 0) > 0),
        "backup_validation_gap": str(config.get("backup_checksum_default") or "").strip().lower() in {"no", "false", "0", "off", "disabled"},
        "missing_full_backups": bool((backup.get("databases_missing_full_backup") or 0) > 0),
    }


def _patterns_for(row: Dict[str, Any], metrics: MetricMap, dimension: str, flags: Dict[str, bool]) -> List[str]:
    patterns: List[str] = []
    bucket = str(row.get("bucket") or "").lower()

    if dimension == "cpu" or "worker" in bucket or metrics.get("total_worker_time") or metrics.get("avg_worker_time"):
        patterns.append("high_cpu")
    if dimension == "reads" or "read" in bucket or metrics.get("total_logical_reads") or metrics.get("avg_logical_reads"):
        patterns.append("high_logical_reads")
    if dimension == "duration" or "elapsed" in bucket or metrics.get("avg_elapsed_time") or metrics.get("total_elapsed_time"):
        patterns.append("long_duration")
    if (metrics.get("execution_count") or 0) >= 1000 or dimension == "executions":
        patterns.append("high_frequency")
    if (metrics.get("spills") or 0) > 0:
        patterns.append("tempdb_spill_candidate")
    if (metrics.get("granted_memory_kb") or 0) >= 524288:
        patterns.append("large_memory_grant_candidate")
    if patterns and flags.get("default_parallelism_risk") and ("high_cpu" in patterns or flags.get("parallelism_waits")):
        patterns.append("parallelism_candidate")
    if "high_logical_reads" in patterns:
        patterns.append("index_or_predicate_candidate")
    if "long_duration" in patterns and not metrics.get("total_worker_time"):
        patterns.append("wait_or_blocking_candidate")
    if "high_cpu" in patterns and flags.get("cpu_pressure"):
        patterns.append("cpu_pressure_correlated")
    if "high_logical_reads" in patterns and flags.get("memory_pressure"):
        patterns.append("buffer_churn_candidate")
    if not row.get("query_plan") and not row.get("plan_xml"):
        patterns.append("actual_plan_required")
    return list(dict.fromkeys(patterns))


def _severity(patterns: List[str], flags: Dict[str, bool]) -> str:
    score = 0
    high_patterns = {"high_cpu", "high_logical_reads", "long_duration", "tempdb_spill_candidate", "large_memory_grant_candidate"}
    score += sum(2 for p in patterns if p in high_patterns)
    score += 1 if "high_frequency" in patterns else 0
    score += 1 if "parallelism_candidate" in patterns else 0
    score += 1 if "cpu_pressure_correlated" in patterns or "buffer_churn_candidate" in patterns else 0
    score += 1 if flags.get("io_waits") and "high_logical_reads" in patterns else 0
    score += 1 if flags.get("tempdb_latch_waits") and "tempdb_spill_candidate" in patterns else 0
    if score >= 6:
        return "critical"
    if score >= 4:
        return "high"
    if score >= 2:
        return "medium"
    return "low"


def _confidence(row: Dict[str, Any], metrics: MetricMap, patterns: List[str]) -> str:
    metric_count = sum(1 for value in metrics.values() if value is not None)
    has_identity = bool(row.get("object_name") or row.get("query_text") or row.get("query_hash"))
    has_plan = bool(row.get("query_plan") or row.get("plan_xml") or row.get("plan_hash"))
    if metric_count >= 4 and has_identity and has_plan:
        return "high"
    if metric_count >= 2 and has_identity:
        return "medium"
    if patterns:
        return "medium" if has_identity else "low"
    return "low"


def _evidence_strings(row: Dict[str, Any], metrics: MetricMap, dimension: str) -> List[str]:
    evidence: List[str] = []
    if row.get("metric_name") and row.get("metric_value") not in (None, ""):
        evidence.append(f"Primary extracted metric: {row.get('metric_name')} = {row.get('metric_value')}.")
    metric_labels = {
        "total_worker_time": "total worker time",
        "avg_worker_time": "avg worker time",
        "total_logical_reads": "total logical reads",
        "avg_logical_reads": "avg logical reads",
        "total_elapsed_time": "total elapsed time",
        "avg_elapsed_time": "avg elapsed time",
        "execution_count": "execution count",
        "total_physical_reads": "physical reads",
        "total_logical_writes": "logical writes",
        "granted_memory_kb": "granted memory KB",
        "spills": "spills",
    }
    for key, label in metric_labels.items():
        value = metrics.get(key)
        if value is not None:
            evidence.append(f"{label}: {value:g}.")
    if row.get("source_sheet"):
        evidence.append(f"Source sheet: {row.get('source_sheet')}.")
    if not evidence:
        evidence.append(f"Query was included in the {dimension} hotspot bucket, but detailed numeric columns were limited.")
    return evidence[:8]


def _likely_causes(patterns: List[str], flags: Dict[str, bool]) -> List[str]:
    causes: List[str] = []
    if "high_logical_reads" in patterns:
        causes.append("Excessive logical reads are consistent with missing/inefficient index access, non-SARGable predicates, large scans, key lookups, or row-width issues; validate with the actual execution plan.")
    if "high_cpu" in patterns:
        causes.append("High worker time is consistent with CPU-heavy operators, repeated executions, scalar computation, parallel plan overhead, or inefficient join/aggregation choices; validate with Query Store and actual plans.")
    if "long_duration" in patterns:
        causes.append("Long elapsed time may reflect blocking, I/O waits, memory grants, client/network waits, or CPU saturation; compare elapsed time to worker time before assigning root cause.")
    if "high_frequency" in patterns:
        causes.append("High execution frequency can make individually cheap statements expensive in aggregate; consider batching, caching, or reducing round trips.")
    if "tempdb_spill_candidate" in patterns:
        causes.append("Spill evidence suggests memory grant or cardinality-estimation issues that should be validated in the actual execution plan and TempDB waits.")
    if "parallelism_candidate" in patterns:
        causes.append("Parallelism configuration or plan choices may be contributing; review MAXDOP, Cost Threshold for Parallelism, and per-query plan shape.")
    if flags.get("io_waits") and "high_logical_reads" in patterns:
        causes.append("Read-heavy hotspot aligns with I/O wait signals; reduce reads before scaling storage.")
    return causes[:6] or ["The row is a tuning candidate because it appears in the expensive-query evidence set; collect execution-plan detail before assigning a specific root cause."]


def _recommendations(patterns: List[str], flags: Dict[str, bool]) -> List[Dict[str, str]]:
    recs: List[Dict[str, str]] = [
        {
            "owner": "DBA",
            "methodology": "Microsoft",
            "action": "Capture the actual execution plan and Query Store/DMV runtime stats for the same query text or hash before changing code or indexes.",
            "risk": "Prevents unsupported operator-level conclusions from a CSV-only snapshot.",
            "validation": "Baseline CPU, logical reads, duration, executions, waits, and plan hash; compare the same metrics after remediation.",
        }
    ]
    if "high_logical_reads" in patterns:
        recs.append({
            "owner": "Developer",
            "methodology": "Brent Ozar / Glenn Berry",
            "action": "Tune for fewer logical reads first: verify predicates are SARGable, remove unnecessary columns, check join/filter selectivity, and validate covering-index candidates.",
            "risk": "Blind index creation can increase write overhead or duplicate existing indexes.",
            "validation": "Target lower logical reads per execution in STATISTICS IO and Query Store runtime stats.",
        })
    if "high_cpu" in patterns:
        recs.append({
            "owner": "Developer",
            "methodology": "Glenn Berry / Dr DMV",
            "action": "Review CPU-heavy operators, scalar expressions, joins, aggregations, and row counts in the actual plan; test query rewrites before changing server hardware.",
            "risk": "Scaling CPU before reducing worker time can hide the root cause and increase cost.",
            "validation": "Target lower worker time per execution without increasing reads, spills, or duration.",
        })
    if "parallelism_candidate" in patterns:
        recs.append({
            "owner": "DBA",
            "methodology": "Brent Ozar / Glenn Berry",
            "action": "Validate whether MAXDOP and Cost Threshold for Parallelism are causing avoidable parallel plans for this workload; test scoped changes before instance-wide changes.",
            "risk": "Instance-wide parallelism changes can help one workload and harm another.",
            "validation": "Compare CX waits, CPU, duration, and plan shape before/after in a controlled window.",
        })
    if "tempdb_spill_candidate" in patterns or "large_memory_grant_candidate" in patterns:
        recs.append({
            "owner": "DBA",
            "methodology": "Microsoft",
            "action": "Inspect memory grant warnings, spills, estimates versus actual rows, and TempDB usage in the actual plan.",
            "risk": "Memory grant hints or resource-governor changes without plan validation can reduce concurrency.",
            "validation": "Target fewer spills and right-sized grants while preserving concurrency and elapsed time.",
        })
    if "index_or_predicate_candidate" in patterns:
        recs.append({
            "owner": "DBA",
            "methodology": "Ola Hallengren",
            "action": "Validate statistics freshness and index fragmentation/coverage for the tables touched by this query; schedule safe stats/index maintenance only where evidence supports it.",
            "risk": "Routine rebuilds are not a substitute for query/index design and can consume maintenance windows.",
            "validation": "After maintenance or index changes, confirm plan quality and lower reads/CPU using Query Store or replayed tests.",
        })
    if flags.get("page_verify_gap") or flags.get("backup_validation_gap") or flags.get("missing_full_backups"):
        recs.append({
            "owner": "DBA",
            "methodology": "Ola Hallengren",
            "action": "Resolve recoverability/integrity validation gaps before high-risk tuning changes that alter indexes or plans broadly.",
            "risk": "Performance remediation should not precede backup and integrity assurance.",
            "validation": "Confirm current full/log backups, CHECKDB status, and checksum/page-verify posture before production rollout.",
        })
    return recs[:6]


def _normalize_hotspot(row: Dict[str, Any], rank: int, context: Dict[str, Any]) -> Dict[str, Any]:
    metrics = _metric_map(row)
    dimension = _dimension_from_metric(row, metrics)
    flags = _context_flags(context)
    patterns = _patterns_for(row, metrics, dimension, flags)
    severity = _severity(patterns, flags)
    confidence = _confidence(row, metrics, patterns)

    query_text = _first_text(row, ["query_text", "Query Text", "SQL Text", "Statement Text", "Short Query Text"], max_len=900)
    object_name = _first_text(row, ["object_name", "Stored Procedure Name", "Procedure Name", "Object Name", "Query Name", "Name"], max_len=220)

    return {
        "rank": rank,
        "source_sheet": row.get("source_sheet"),
        "bucket": row.get("bucket"),
        "database_name": _first_text(row, ["database_name", "Database Name", "Database"], max_len=160),
        "object_name": object_name or (query_text[:220] if query_text else None),
        "query_text": query_text,
        "query_hash": _first_text(row, ["query_hash", "Query Hash", "query_hash_hex"], max_len=120),
        "plan_hash": _first_text(row, ["plan_hash", "Query Plan Hash", "query_plan_hash", "Plan Hash"], max_len=120),
        "metrics": metrics,
        "classification": {
            "primary_dimension": dimension,
            "patterns": patterns,
            "severity": severity,
            "confidence": confidence,
        },
        "diagnosis": {
            "evidence": _evidence_strings(row, metrics, dimension),
            "likely_causes": _likely_causes(patterns, flags),
            "what_not_to_assume": [
                "Do not assume a specific execution-plan operator, missing index, spill, or parameter-sniffing root cause unless the actual plan or Query Store evidence confirms it.",
                "Do not generate production DDL from this snapshot alone; convert index/statistics ideas into validation tasks first.",
            ],
        },
        "recommendations": _recommendations(patterns, flags),
    }


def _summary(hotspots: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not hotspots:
        return {
            "total_hotspots_analyzed": 0,
            "primary_pressure_dimension": "unknown",
            "dominant_risks": [],
            "confidence": "low",
            "limitations": ["No query hotspot rows were available in the selected ingestion scope."],
        }

    dimension_counts: Dict[str, int] = {}
    risk_counts: Dict[str, int] = {}
    confidence_counts: Dict[str, int] = {}
    limitations = set()
    for item in hotspots:
        cls = item.get("classification") or {}
        dimension = cls.get("primary_dimension") or "unknown"
        dimension_counts[dimension] = dimension_counts.get(dimension, 0) + 1
        confidence = cls.get("confidence") or "low"
        confidence_counts[confidence] = confidence_counts.get(confidence, 0) + 1
        for pattern in cls.get("patterns") or []:
            if pattern != "actual_plan_required":
                risk_counts[pattern] = risk_counts.get(pattern, 0) + 1
            else:
                limitations.add("Actual execution plans were not present for one or more hotspots; operator-level diagnosis requires plan capture.")

    primary_dimension = sorted(dimension_counts.items(), key=lambda kv: kv[1], reverse=True)[0][0]
    dominant_risks = [k for k, _ in sorted(risk_counts.items(), key=lambda kv: kv[1], reverse=True)[:6]]
    confidence = "high" if confidence_counts.get("high", 0) >= max(1, len(hotspots) // 2) else "medium" if confidence_counts.get("medium", 0) else "low"
    return {
        "total_hotspots_analyzed": len(hotspots),
        "primary_pressure_dimension": primary_dimension,
        "dominant_risks": dominant_risks,
        "confidence": confidence,
        "limitations": sorted(limitations) or ["CSV/DMV snapshots are point-in-time or cumulative evidence; validate recommendations against current Query Store/runtime data."],
    }


def analyze_query_hotspots(
    hotspots: Iterable[Dict[str, Any]],
    *,
    context: Optional[Dict[str, Any]] = None,
    limit: int = 12,
) -> Dict[str, Any]:
    """Return deterministic expert-method analysis for extracted query hotspots."""
    context = context or {}
    input_rows = [row for row in hotspots or [] if isinstance(row, dict)]
    analyzed = [_normalize_hotspot(row, idx + 1, context) for idx, row in enumerate(input_rows[:limit])]
    return {
        "summary": _summary(analyzed),
        "hotspots": analyzed,
        "methodology_matrix": list(_METHOD_MATRIX),
    }


def build_query_analysis(
    hotspots: Iterable[Dict[str, Any]],
    *,
    context: Optional[Dict[str, Any]] = None,
    limit: int = 12,
) -> Dict[str, Any]:
    """Backward-compatible alias for deterministic query hotspot analysis.

    Some deployed Databricks App processes can briefly run older report-service code
    after a rollout.  That older code referenced ``build_query_analysis`` while the
    finalized public API was named ``analyze_query_hotspots``.  Keeping this alias
    prevents a NameError during mixed-version deployments and lets both names return
    the exact same evidence contract.
    """
    return analyze_query_hotspots(hotspots, context=context, limit=limit)
