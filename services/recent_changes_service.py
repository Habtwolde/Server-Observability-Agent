"""
services/recent_changes_service.py

Compares the current ingestion snapshot against the immediately preceding one
for the same server.  Produces a structured list of change findings, each
classified as HIGH / MEDIUM / LOW risk, with:
  - what changed and by how much
  - what it means for applications connecting to databases on this server
  - recommendations and diagnostics
  - persistent risk detection (risk that existed before and still exists now)

Public API
----------
    get_previous_ingestion_date(server_name, current_date) -> str | None
    build_changes_report(server_name, current_date) -> dict
"""

from __future__ import annotations

import math
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st

from db.connection import run_query
from services.metrics_service import build_server_profile, _sql_quote
from services.llm_service import chat_completion


# ---------------------------------------------------------------------------
# Severity constants
# ---------------------------------------------------------------------------
HIGH = "HIGH"
MEDIUM = "MEDIUM"
LOW = "LOW"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe_float(v) -> Optional[float]:
    try:
        f = float(v)
        return f if math.isfinite(f) else None
    except (TypeError, ValueError):
        return None


def _pct_delta(current: Optional[float], previous: Optional[float]) -> Optional[float]:
    """Relative change in percentage points (signed)."""
    if current is None or previous is None:
        return None
    return current - previous


def _rel_change(current: Optional[float], previous: Optional[float]) -> Optional[float]:
    """(current - previous) / previous * 100 — signed relative % change."""
    if current is None or previous is None or previous == 0:
        return None
    return (current - previous) / abs(previous) * 100.0


def _fmt(v, unit="") -> str:
    if v is None:
        return "N/A"
    if isinstance(v, float):
        return f"{v:.1f}{unit}"
    return f"{v}{unit}"


def _dir(delta: Optional[float]) -> str:
    if delta is None:
        return ""
    return "▲" if delta > 0 else "▼"


# ---------------------------------------------------------------------------
# Ingestion date resolution
# ---------------------------------------------------------------------------

@st.cache_data(ttl=300)
def get_previous_ingestion_date(server_name: str, current_date: str) -> Optional[str]:
    """Return the ingestion date immediately before *current_date* for *server_name*."""
    q = f"""
    SELECT DISTINCT CAST(ingestion_date AS STRING) AS ingestion_date
    FROM btris_dbx.observability.sql_diagnostics_files_delta
    WHERE server_name = '{_sql_quote(server_name)}'
      AND CAST(ingestion_date AS STRING) < '{_sql_quote(current_date)}'
    ORDER BY ingestion_date DESC
    LIMIT 1
    """
    df = run_query(q)
    if df.empty or "ingestion_date" not in df.columns:
        return None
    val = df["ingestion_date"].iloc[0]
    return str(val) if val is not None else None


# ---------------------------------------------------------------------------
# Configuration comparison
# ---------------------------------------------------------------------------

_CONFIG_META: Dict[str, Dict[str, Any]] = {
    "maxdop": {
        "label": "Max Degree of Parallelism (MAXDOP)",
        "unit": "",
        "desc_increase": "Higher MAXDOP allows queries to consume more CPU threads in parallel, which can benefit large analytical queries but may starve OLTP workloads.",
        "desc_decrease": "Lower MAXDOP restricts parallelism; OLTP applications will benefit from reduced blocking, but long-running analytical queries will slow down.",
        "risk_increase": MEDIUM,
        "risk_decrease": MEDIUM,
        "threshold_high": None,
    },
    "cost_threshold": {
        "label": "Cost Threshold for Parallelism",
        "unit": "",
        "desc_increase": "Higher cost threshold means fewer queries will go parallel. Applications running moderate-cost queries may see slightly higher serial execution times.",
        "desc_decrease": "Lower cost threshold causes more queries to execute in parallel, increasing CPU contention risk under concurrent application load.",
        "risk_increase": LOW,
        "risk_decrease": MEDIUM,
        "threshold_high": None,
    },
    "max_server_memory_mb": {
        "label": "Max Server Memory",
        "unit": " MB",
        "desc_increase": "More memory allocated to SQL Server; the buffer pool can cache larger working sets, reducing physical reads for application queries.",
        "desc_decrease": "Reduced SQL Server memory cap increases the risk of buffer pool pressure, forcing more physical I/O for all applications on this server.",
        "risk_increase": LOW,
        "risk_decrease": HIGH,
        "threshold_high": None,
    },
    "optimize_for_adhoc": {
        "label": "Optimize for Ad-Hoc Workloads",
        "unit": "",
        "desc_increase": "Now enabled — single-use query plans are cached as stubs first, reducing plan cache bloat for applications generating ad-hoc SQL.",
        "desc_decrease": "Now disabled — ad-hoc plans will be cached fully on first execution, which may increase plan cache pressure for ORM-heavy applications.",
        "risk_increase": LOW,
        "risk_decrease": MEDIUM,
        "threshold_high": None,
    },
    "backup_compression_default": {
        "label": "Backup Compression Default",
        "unit": "",
        "desc_increase": "Backup compression now enabled — backup I/O windows will be shorter, reducing the risk of backup jobs competing with application queries.",
        "desc_decrease": "Backup compression now disabled — backup jobs will generate more I/O, potentially impacting application query latency during backup windows.",
        "risk_increase": LOW,
        "risk_decrease": MEDIUM,
        "threshold_high": None,
    },
    "backup_checksum_default": {
        "label": "Backup Checksum Default",
        "unit": "",
        "desc_increase": "Backup checksums now enforced — backup integrity verification improved. Minimal application impact.",
        "desc_decrease": "Backup checksums now disabled — backup corruption may go undetected. Recovery reliability reduced.",
        "risk_increase": LOW,
        "risk_decrease": HIGH,
        "threshold_high": None,
    },
}


def _compare_configuration(
    cur_conf: Dict[str, Any], prev_conf: Dict[str, Any]
) -> List[Dict[str, Any]]:
    findings: List[Dict[str, Any]] = []

    # Numeric configs
    for key, meta in _CONFIG_META.items():
        cv = _safe_float(cur_conf.get(key))
        pv = _safe_float(prev_conf.get(key))
        if cv is None and pv is None:
            continue
        if cv == pv:
            continue
        if cv is None or pv is None:
            # One side missing — treat as changed with unknown magnitude
            delta = None
            direction = "changed"
            risk = MEDIUM
            label = meta["label"]
            desc = f"Configuration value `{label}` could not be compared — one snapshot is missing data."
            findings.append(_finding(
                category="Server Configuration",
                metric=label,
                previous=_fmt(pv, meta["unit"]),
                current=_fmt(cv, meta["unit"]),
                delta_str="N/A",
                risk=risk,
                headline=f"`{label}` changed (one value unavailable)",
                impact=desc,
                recommendation=f"Verify current `{label}` setting via `sp_configure` or SQL Server properties.",
                diagnostics=[f"SELECT name, value_in_use FROM sys.configurations WHERE name = '{key}';"],
            ))
            continue

        delta = cv - pv
        if delta == 0:
            continue
        direction = "increased" if delta > 0 else "decreased"
        risk = meta["risk_increase"] if delta > 0 else meta["risk_decrease"]
        impact = meta["desc_increase"] if delta > 0 else meta["desc_decrease"]
        findings.append(_finding(
            category="Server Configuration",
            metric=meta["label"],
            previous=_fmt(pv, meta["unit"]),
            current=_fmt(cv, meta["unit"]),
            delta_str=f"{_dir(delta)} {abs(delta):.0f}{meta['unit']}",
            risk=risk,
            headline=f"`{meta['label']}` {direction} from {_fmt(pv, meta['unit'])} → {_fmt(cv, meta['unit'])}",
            impact=impact,
            recommendation=(
                f"Review whether the `{meta['label']}` change was intentional. "
                f"Confirm with the DBA responsible for this server."
            ),
            diagnostics=[
                f"SELECT name, value, value_in_use, description "
                f"FROM sys.configurations WHERE name LIKE '%{key.replace('_',' ')}%';",
            ],
        ))

    # Boolean / string configs (page verify, etc.)
    for key in ["optimize_for_adhoc", "backup_compression_default", "backup_checksum_default"]:
        cv = str(cur_conf.get(key) or "").strip()
        pv = str(prev_conf.get(key) or "").strip()
        if cv == pv or (not cv and not pv):
            continue
        meta = _CONFIG_META.get(key, {})
        risk = MEDIUM
        headline = f"`{meta.get('label', key)}` changed from `{pv or 'Unknown'}` → `{cv or 'Unknown'}`"
        impact = meta.get("desc_increase", "Configuration state changed.")
        findings.append(_finding(
            category="Server Configuration",
            metric=meta.get("label", key),
            previous=pv or "Unknown",
            current=cv or "Unknown",
            delta_str="State changed",
            risk=risk,
            headline=headline,
            impact=impact,
            recommendation=f"Confirm this change was intentional and approved.",
            diagnostics=[
                "SELECT name, value_in_use FROM sys.configurations "
                "WHERE name IN ('optimize for ad hoc workloads', 'backup compression default', "
                "'backup checksum default');",
            ],
        ))

    return findings


# ---------------------------------------------------------------------------
# Utilization comparison
# ---------------------------------------------------------------------------

def _compare_utilization(
    cur_util: Dict[str, Any],
    prev_util: Dict[str, Any],
    cur_pressure: Dict[str, Any],
    prev_pressure: Dict[str, Any],
) -> List[Dict[str, Any]]:
    findings: List[Dict[str, Any]] = []

    # CPU
    cpu_c = _safe_float(cur_util.get("max_cpu_pct"))
    cpu_p = _safe_float(prev_util.get("max_cpu_pct"))
    delta_cpu = _pct_delta(cpu_c, cpu_p)
    if delta_cpu is not None and abs(delta_cpu) >= 5:
        risk = HIGH if (cpu_c or 0) >= 85 else (MEDIUM if abs(delta_cpu) >= 15 else LOW)
        direction = "increased" if delta_cpu > 0 else "decreased"
        findings.append(_finding(
            category="CPU Utilization",
            metric="Peak CPU %",
            previous=_fmt(cpu_p, "%"),
            current=_fmt(cpu_c, "%"),
            delta_str=f"{_dir(delta_cpu)} {abs(delta_cpu):.1f}pp",
            risk=risk,
            headline=f"Peak CPU {direction} by {abs(delta_cpu):.1f} percentage points "
                     f"({_fmt(cpu_p,'%')} → {_fmt(cpu_c,'%')})",
            impact=(
                "Applications executing CPU-intensive queries (joins, aggregations, sorts) "
                "will experience longer wait times and slower response when CPU headroom shrinks. "
                "Connection pool saturation may follow under concurrent load."
                if delta_cpu > 0 else
                "CPU headroom has improved. Applications should see better query concurrency "
                "and lower contention on multi-threaded workloads."
            ),
            recommendation=(
                "Identify top CPU-consuming queries via the Most Expensive Queries tab. "
                "Consider query tuning, index additions, or MAXDOP adjustment if CPU remains elevated."
                if delta_cpu > 0 else
                "Continue monitoring CPU trend. Validate that the reduction is due to workload "
                "optimisation and not a loss of active connections."
            ),
            diagnostics=[
                "SELECT TOP 10 total_worker_time/execution_count AS avg_cpu_us, "
                "SUBSTRING(st.text, (qs.statement_start_offset/2)+1, 200) AS query "
                "FROM sys.dm_exec_query_stats qs "
                "CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) st "
                "ORDER BY avg_cpu_us DESC;",
            ],
        ))

    # Memory
    mem_c = _safe_float(cur_util.get("max_memory_pct"))
    mem_p = _safe_float(prev_util.get("max_memory_pct"))
    delta_mem = _pct_delta(mem_c, mem_p)
    if delta_mem is not None and abs(delta_mem) >= 5:
        risk = HIGH if (mem_c or 0) >= 85 else (MEDIUM if abs(delta_mem) >= 15 else LOW)
        direction = "increased" if delta_mem > 0 else "decreased"
        findings.append(_finding(
            category="Memory Utilization",
            metric="Peak Memory %",
            previous=_fmt(mem_p, "%"),
            current=_fmt(mem_c, "%"),
            delta_str=f"{_dir(delta_mem)} {abs(delta_mem):.1f}pp",
            risk=risk,
            headline=f"Peak memory utilization {direction} by {abs(delta_mem):.1f} percentage points "
                     f"({_fmt(mem_p,'%')} → {_fmt(mem_c,'%')})",
            impact=(
                "High memory pressure causes SQL Server to aggressively evict buffer pool pages. "
                "Applications will experience increased physical I/O, slower query response times, "
                "and risk of memory grant queue build-up under concurrent load."
                if delta_mem > 0 else
                "Memory utilization has eased. Buffer pool can retain more data pages, "
                "reducing physical I/O for application query workloads."
            ),
            recommendation=(
                "Audit top memory-consuming databases and queries. "
                "Review 'max server memory' configuration and consider adding RAM if trend continues."
                if delta_mem > 0 else
                "Confirm memory reduction is intentional (workload decrease, query tuning, "
                "connection drain) and not indicative of a service disruption."
            ),
            diagnostics=[
                "SELECT physical_memory_in_use_kb/1024 AS mem_used_mb, "
                "page_fault_count FROM sys.dm_os_process_memory;",
                "SELECT type, pages_kb FROM sys.dm_os_memory_clerks "
                "ORDER BY pages_kb DESC;",
            ],
        ))

    # PLE
    ple_c = _safe_float(cur_util.get("cache_ple_seconds"))
    ple_p = _safe_float(prev_util.get("cache_ple_seconds"))
    rel = _rel_change(ple_c, ple_p)
    if rel is not None and abs(rel) >= 15:
        risk = HIGH if (ple_c or 999) <= 300 else (MEDIUM if (ple_c or 999) <= 600 else LOW)
        direction = "dropped" if rel < 0 else "improved"
        findings.append(_finding(
            category="Buffer Cache Health",
            metric="Page Life Expectancy (PLE)",
            previous=_fmt(ple_p, "s"),
            current=_fmt(ple_c, "s"),
            delta_str=f"{_dir(rel)} {abs(rel):.0f}% change",
            risk=risk,
            headline=f"PLE {direction} by {abs(rel):.0f}% ({_fmt(ple_p,'s')} → {_fmt(ple_c,'s')})",
            impact=(
                "Lower PLE means database pages are being evicted from the buffer pool faster. "
                "Every application query that touches evicted pages must perform physical disk reads "
                "instead of in-memory reads, causing significant latency spikes under load."
                if rel < 0 else
                "Higher PLE indicates the buffer pool is retaining data pages longer. "
                "Applications benefit from improved cache hit rates and lower physical I/O."
            ),
            recommendation=(
                "Investigate large table scans, index rebuilds, or bulk operations that may "
                "be flushing the cache. Review 'max server memory'. "
                "Run DBCC MEMORYSTATUS for detailed analysis."
                if rel < 0 else
                "Positive trend. Continue monitoring to confirm stability."
            ),
            diagnostics=[
                "SELECT object_name, counter_name, cntr_value AS ple_seconds "
                "FROM sys.dm_os_performance_counters "
                "WHERE counter_name = 'Page life expectancy';",
            ],
        ))

    # Memory grants pending
    gp_c = _safe_float(cur_pressure.get("memory_grants_pending"))
    gp_p = _safe_float(prev_pressure.get("memory_grants_pending"))
    if gp_c is not None and gp_p is not None:
        if gp_c > 0 and gp_c > (gp_p or 0):
            risk = HIGH if gp_c >= 5 else MEDIUM
            findings.append(_finding(
                category="Memory Pressure",
                metric="Memory Grants Pending",
                previous=_fmt(gp_p, ""),
                current=_fmt(gp_c, ""),
                delta_str=f"▲ {gp_c - (gp_p or 0):.0f}",
                risk=risk,
                headline=f"Memory grants pending increased to {_fmt(gp_c,'')} "
                         f"(was {_fmt(gp_p,'')})",
                impact=(
                    "Applications with query plans requiring memory grants (sorts, hashes, "
                    "batch-mode operations) will queue waiting for grants. This causes "
                    "application-visible latency spikes and can cascade into timeout errors."
                ),
                recommendation=(
                    "Identify queries requesting excessive memory grants. "
                    "Check for missing or stale statistics, parameter sniffing issues, "
                    "or overly parallel query plans. "
                    "Consider Resource Governor memory limits per application workload group."
                ),
                diagnostics=[
                    "SELECT session_id, requested_memory_kb, granted_memory_kb, "
                    "wait_order, is_next_candidate "
                    "FROM sys.dm_exec_query_memory_grants "
                    "ORDER BY requested_memory_kb DESC;",
                ],
            ))

    return findings


# ---------------------------------------------------------------------------
# I/O comparison
# ---------------------------------------------------------------------------

def _compare_io(
    cur_io: Dict[str, Any], prev_io: Dict[str, Any]
) -> List[Dict[str, Any]]:
    findings: List[Dict[str, Any]] = []

    checks = [
        ("avg_read_latency_ms", "Average Read Latency", "ms",
         "Elevated read latency means applications must wait longer for SELECT queries to retrieve data pages. "
         "Any workload doing table or index scans — including ORMs — will be directly impacted.",
         "Check for I/O-bound queries, fragmented indexes, or disk subsystem degradation. "
         "Review storage controller queue depth and verify backup schedules are not overlapping peak hours.",
         [
             "SELECT DB_NAME(vfs.database_id) AS db, vfs.file_id, "
             "io_stall_read_ms/NULLIF(num_of_reads,0) AS avg_read_ms "
             "FROM sys.dm_io_virtual_file_stats(NULL,NULL) vfs "
             "ORDER BY avg_read_ms DESC;",
         ]),
        ("avg_write_latency_ms", "Average Write Latency", "ms",
         "High write latency directly impacts any application performing INSERT, UPDATE, or DELETE operations. "
         "Transaction commit times increase, and connection pool threads may stall waiting for log writes.",
         "Investigate log file placement and sizing. Verify the transaction log is not on a shared spindle. "
         "Review CHECKPOINT and log flush behavior. Consider separating data and log files onto dedicated drives.",
         [
             "SELECT DB_NAME(vfs.database_id) AS db, vfs.file_id, "
             "io_stall_write_ms/NULLIF(num_of_writes,0) AS avg_write_ms "
             "FROM sys.dm_io_virtual_file_stats(NULL,NULL) vfs "
             "ORDER BY avg_write_ms DESC;",
         ]),
        ("drive_max_overall_latency_ms", "Drive Max Overall Latency", "ms",
         "Peak drive latency exceeding thresholds indicates I/O subsystem stress. "
         "All databases on this server share the physical I/O path — every application is affected simultaneously.",
         "Identify the highest-I/O databases and review disk queue lengths. "
         "Confirm storage tier (SSD vs HDD), check for RAID controller cache bypass, "
         "and review concurrent backup or ETL jobs during the snapshot window.",
         [
             "SELECT physical_name, io_stall, num_of_reads + num_of_writes AS io_ops "
             "FROM sys.dm_io_virtual_file_stats(NULL,NULL) vfs "
             "JOIN sys.master_files mf ON vfs.database_id = mf.database_id "
             "AND vfs.file_id = mf.file_id ORDER BY io_stall DESC;",
         ]),
    ]

    thresholds = {
        "avg_read_latency_ms": (10.0, 20.0),
        "avg_write_latency_ms": (10.0, 20.0),
        "drive_max_overall_latency_ms": (20.0, 50.0),
    }

    for key, label, unit, impact, recommendation, diag in checks:
        cv = _safe_float(cur_io.get(key))
        pv = _safe_float(prev_io.get(key))
        rel = _rel_change(cv, pv)
        if rel is None or abs(rel) < 20:
            continue
        warn_t, bad_t = thresholds.get(key, (10.0, 20.0))
        risk = HIGH if (cv or 0) >= bad_t else (MEDIUM if (cv or 0) >= warn_t else LOW)
        direction = "increased" if rel > 0 else "decreased"
        findings.append(_finding(
            category="I/O Performance",
            metric=label,
            previous=_fmt(pv, unit),
            current=_fmt(cv, unit),
            delta_str=f"{_dir(rel)} {abs(rel):.0f}% change",
            risk=risk,
            headline=f"{label} {direction} by {abs(rel):.0f}% ({_fmt(pv,unit)} → {_fmt(cv,unit)})",
            impact=impact if rel > 0 else
                   f"{label} has improved. Applications should experience lower storage wait times.",
            recommendation=recommendation if rel > 0 else "Positive trend. Continue monitoring.",
            diagnostics=diag if rel > 0 else [],
        ))

    return findings


# ---------------------------------------------------------------------------
# Wait statistics comparison
# ---------------------------------------------------------------------------

def _compare_waits(
    cur_waits: Optional[pd.DataFrame], prev_waits: Optional[pd.DataFrame]
) -> List[Dict[str, Any]]:
    findings: List[Dict[str, Any]] = []

    def _waits_to_dict(df: Optional[pd.DataFrame]) -> Dict[str, float]:
        if not isinstance(df, pd.DataFrame) or df.empty:
            return {}
        cols = list(df.columns)

        def _col(candidates):
            for c in candidates:
                if c in cols:
                    return c
            return None

        wt_col = _col(["wait_type", "WaitType", "wait_category"])
        pct_col = _col(["wait_pct", "WaitPct", "wait_percent", "pct"])
        if not wt_col or not pct_col:
            return {}
        result = {}
        for _, row in df.iterrows():
            wt = str(row.get(wt_col, "")).strip()
            pct = _safe_float(row.get(pct_col))
            if wt and pct is not None:
                result[wt] = pct
        return result

    cur_d = _waits_to_dict(cur_waits)
    prev_d = _waits_to_dict(prev_waits)

    if not cur_d and not prev_d:
        return findings

    all_wait_types = set(cur_d) | set(prev_d)

    # Wait types with significant new appearance or growth
    _wait_impact = {
        "PAGEIOLATCH_SH": (
            "Shared page I/O latch waits signal the buffer pool is reading pages from disk. "
            "Read-intensive application queries (reports, analytics, ORM lazy-loads) will stall.",
            HIGH,
        ),
        "PAGEIOLATCH_EX": (
            "Exclusive page I/O latch waits indicate write I/O pressure. "
            "Applications performing INSERT/UPDATE/DELETE will experience latency.",
            HIGH,
        ),
        "LCK_M_X": (
            "Exclusive lock waits point to high write contention. "
            "Applications with concurrent DML on the same rows will see blocking and timeout errors.",
            HIGH,
        ),
        "LCK_M_S": (
            "Shared lock waits indicate read/write contention. "
            "Applications may experience intermittent blocking under concurrent workloads.",
            MEDIUM,
        ),
        "CXPACKET": (
            "Parallel query packet waits increased. Queries are waiting on parallel thread synchronisation. "
            "Applications running heavy analytical queries may see uneven performance.",
            MEDIUM,
        ),
        "CXCONSUMER": (
            "Parallel consumer waits suggest imbalanced parallel query execution. "
            "This can manifest as queries that run significantly longer than expected.",
            MEDIUM,
        ),
        "SOS_SCHEDULER_YIELD": (
            "CPU scheduler yields indicate high CPU contention. "
            "All applications sharing this server may experience CPU queuing.",
            HIGH,
        ),
        "WRITELOG": (
            "Transaction log write waits increased. Applications performing DML will be "
            "directly impacted — commit times will increase, which compounds under connection pool pressure.",
            HIGH,
        ),
        "ASYNC_IO_COMPLETION": (
            "Async I/O completion waits suggest storage latency on asynchronous operations. "
            "Backup, bulk-load, and large-index-rebuild operations may be the source.",
            MEDIUM,
        ),
        "RESOURCE_SEMAPHORE": (
            "Memory resource semaphore waits mean queries are queuing for memory grants. "
            "Applications with complex queries will see significant execution delay.",
            HIGH,
        ),
        "TEMPDB_OBJECT_STORE_LOCK_CONTENTION_MUTEX": (
            "TempDB metadata contention — applications that heavily use temp tables, "
            "table variables, or row versioning will experience concurrency bottlenecks.",
            HIGH,
        ),
    }

    for wt in sorted(all_wait_types):
        cv = cur_d.get(wt, 0.0)
        pv = prev_d.get(wt, 0.0)
        delta = cv - pv
        if abs(delta) < 3.0:
            continue

        impact_info, base_risk = _wait_impact.get(wt, (
            f"Wait type `{wt}` contribution changed significantly. "
            "Review whether this wait type maps to a bottleneck relevant to application workloads.",
            MEDIUM,
        ))
        risk = HIGH if cv >= 20 else (MEDIUM if cv >= 10 else base_risk if delta > 0 else LOW)
        direction = "increased" if delta > 0 else "decreased"
        findings.append(_finding(
            category="Wait Statistics",
            metric=f"Wait: {wt}",
            previous=_fmt(pv, "%"),
            current=_fmt(cv, "%"),
            delta_str=f"{_dir(delta)} {abs(delta):.1f}pp",
            risk=risk,
            headline=f"`{wt}` wait {direction}: {_fmt(pv,'%')} → {_fmt(cv,'%')} ({delta:+.1f}pp)",
            impact=impact_info if delta > 0 else f"`{wt}` wait pressure has reduced.",
            recommendation=(
                "Drill into the specific wait using the Wait Statistics breakdown in the Overview tab. "
                "Correlate with query execution plan changes or infrastructure events."
                if delta > 0 else "Monitor for continued improvement."
            ),
            diagnostics=[
                f"SELECT wait_type, waiting_tasks_count, wait_time_ms, signal_wait_time_ms "
                f"FROM sys.dm_os_wait_stats WHERE wait_type = '{wt}';",
            ],
        ))

    return findings


# ---------------------------------------------------------------------------
# Database settings comparison
# ---------------------------------------------------------------------------

def _compare_database_settings(
    cur_db: Dict[str, Any], prev_db: Dict[str, Any]
) -> List[Dict[str, Any]]:
    findings: List[Dict[str, Any]] = []

    cur_none = set(cur_db.get("user_dbs_with_page_verify_none", []))
    prev_none = set(prev_db.get("user_dbs_with_page_verify_none", []))

    newly_none = cur_none - prev_none
    recovered = prev_none - cur_none

    if newly_none:
        findings.append(_finding(
            category="Database Configuration",
            metric="PAGE_VERIFY = NONE",
            previous=f"{len(prev_none)} databases",
            current=f"{len(cur_none)} databases (+{len(newly_none)} new)",
            delta_str=f"▲ {len(newly_none)} new",
            risk=HIGH,
            headline=f"{len(newly_none)} additional database(s) now have PAGE_VERIFY = NONE: "
                     f"{', '.join(sorted(newly_none)[:5])}",
            impact=(
                "Databases without PAGE_VERIFY CHECKSUM cannot detect torn pages or storage "
                "corruption during reads. Applications connecting to these databases have no "
                "protection against silently reading corrupted data, which can cause data integrity "
                "violations at the application layer."
            ),
            recommendation=(
                "Immediately set PAGE_VERIFY CHECKSUM on all user databases: "
                "ALTER DATABASE [dbname] SET PAGE_VERIFY CHECKSUM. "
                "This is a zero-downtime online operation on modern SQL Server versions."
            ),
            diagnostics=[
                "SELECT name, page_verify_option_desc FROM sys.databases "
                "WHERE page_verify_option_desc <> 'CHECKSUM' AND database_id > 4;",
            ],
        ))

    if recovered:
        findings.append(_finding(
            category="Database Configuration",
            metric="PAGE_VERIFY = NONE (resolved)",
            previous=f"{len(prev_none)} databases",
            current=f"{len(cur_none)} databases (-{len(recovered)} resolved)",
            delta_str=f"▼ {len(recovered)} resolved",
            risk=LOW,
            headline=f"PAGE_VERIFY improved: {len(recovered)} database(s) no longer set to NONE",
            impact="Data integrity protection improved for these databases.",
            recommendation="Confirm all remaining databases also have CHECKSUM enabled.",
            diagnostics=[],
        ))

    cur_user_count = _safe_float(cur_db.get("user_db_none_count")) or 0
    prev_user_count = _safe_float(prev_db.get("user_db_none_count")) or 0
    delta_count = cur_user_count - prev_user_count
    if abs(delta_count) >= 1 and not newly_none and not recovered:
        findings.append(_finding(
            category="Database Configuration",
            metric="Databases with PAGE_VERIFY = NONE (count change)",
            previous=_fmt(prev_user_count, ""),
            current=_fmt(cur_user_count, ""),
            delta_str=f"{_dir(delta_count)} {abs(delta_count):.0f}",
            risk=MEDIUM if delta_count > 0 else LOW,
            headline=f"Count of user DBs with PAGE_VERIFY = NONE changed "
                     f"({_fmt(prev_user_count,'')} → {_fmt(cur_user_count,'')})",
            impact="Database integrity protection posture changed.",
            recommendation="Review all databases and enforce PAGE_VERIFY CHECKSUM.",
            diagnostics=[
                "SELECT name, page_verify_option_desc FROM sys.databases "
                "WHERE database_id > 4 ORDER BY name;",
            ],
        ))

    return findings


# ---------------------------------------------------------------------------
# Backup comparison
# ---------------------------------------------------------------------------

def _compare_backup(
    cur_bk: Dict[str, Any], prev_bk: Dict[str, Any]
) -> List[Dict[str, Any]]:
    findings: List[Dict[str, Any]] = []

    cur_missing = _safe_float(cur_bk.get("databases_missing_full_backup")) or 0
    prev_missing = _safe_float(prev_bk.get("databases_missing_full_backup")) or 0
    delta = cur_missing - prev_missing

    if delta > 0:
        findings.append(_finding(
            category="Backup Posture",
            metric="Databases Missing Full Backup",
            previous=_fmt(prev_missing, ""),
            current=_fmt(cur_missing, ""),
            delta_str=f"▲ {abs(delta):.0f} more",
            risk=HIGH,
            headline=f"{int(cur_missing)} database(s) are missing a full backup "
                     f"({int(delta)} more than last run)",
            impact=(
                "Databases without a recent full backup have no guaranteed recovery point. "
                "Any application data loss event (corruption, accidental delete, ransomware) "
                "would result in unrecoverable data beyond the last known good backup."
            ),
            recommendation=(
                "Immediately identify which databases are missing backups and trigger ad-hoc full backups. "
                "Verify the backup job schedule and check for failed backup agent jobs."
            ),
            diagnostics=[
                "SELECT name, recovery_model_desc FROM sys.databases d "
                "WHERE NOT EXISTS ("
                "  SELECT 1 FROM msdb.dbo.backupset bs "
                "  WHERE bs.database_name = d.name AND bs.type = 'D' "
                "  AND bs.backup_finish_date >= DATEADD(DAY, -1, GETUTCDATE())"
                ");",
            ],
        ))
    elif delta < 0:
        findings.append(_finding(
            category="Backup Posture",
            metric="Databases Missing Full Backup (resolved)",
            previous=_fmt(prev_missing, ""),
            current=_fmt(cur_missing, ""),
            delta_str=f"▼ {abs(delta):.0f} resolved",
            risk=LOW,
            headline=f"Backup coverage improved: {int(abs(delta))} more database(s) now have full backups",
            impact="Recovery posture has improved.",
            recommendation="Confirm all databases have backups within their required RPO window.",
            diagnostics=[],
        ))

    cur_oldest = _safe_float(cur_bk.get("oldest_full_backup_days"))
    prev_oldest = _safe_float(prev_bk.get("oldest_full_backup_days"))
    if cur_oldest is not None and cur_oldest > 7:
        risk = HIGH if cur_oldest > 14 else MEDIUM
        findings.append(_finding(
            category="Backup Posture",
            metric="Oldest Full Backup Age",
            previous=_fmt(prev_oldest, " days"),
            current=_fmt(cur_oldest, " days"),
            delta_str=f"▲ {(cur_oldest or 0) - (prev_oldest or 0):.0f} days older",
            risk=risk,
            headline=f"Oldest full backup is {int(cur_oldest)} days old",
            impact=(
                "A stale backup means that a recovery scenario could force a restore point "
                "weeks in the past. Applications that depend on data integrity guarantees "
                "face significant business risk."
            ),
            recommendation=(
                "Trigger a full backup for any database with backup age exceeding the agreed RPO. "
                "Review backup job schedules, disk space availability, and backup agent job history."
            ),
            diagnostics=[
                "SELECT d.name, MAX(bs.backup_finish_date) AS last_full_backup, "
                "DATEDIFF(DAY, MAX(bs.backup_finish_date), GETUTCDATE()) AS age_days "
                "FROM sys.databases d LEFT JOIN msdb.dbo.backupset bs "
                "ON d.name = bs.database_name AND bs.type = 'D' "
                "GROUP BY d.name ORDER BY age_days DESC NULLS FIRST;",
            ],
        ))

    return findings


# ---------------------------------------------------------------------------
# Workload comparison
# ---------------------------------------------------------------------------

def _compare_workload(
    cur_wl: Dict[str, Any], prev_wl: Dict[str, Any]
) -> List[Dict[str, Any]]:
    findings: List[Dict[str, Any]] = []

    dur_c = _safe_float(cur_wl.get("max_duration_s"))
    dur_p = _safe_float(prev_wl.get("max_duration_s"))
    rel_dur = _rel_change(dur_c, dur_p)
    if rel_dur is not None and abs(rel_dur) >= 25:
        risk = HIGH if (dur_c or 0) >= 60 else MEDIUM
        direction = "increased" if rel_dur > 0 else "decreased"
        findings.append(_finding(
            category="Workload",
            metric="Max Query Duration",
            previous=_fmt(dur_p, "s"),
            current=_fmt(dur_c, "s"),
            delta_str=f"{_dir(rel_dur)} {abs(rel_dur):.0f}%",
            risk=risk,
            headline=f"Longest query duration {direction} by {abs(rel_dur):.0f}% "
                     f"({_fmt(dur_p,'s')} → {_fmt(dur_c,'s')})",
            impact=(
                "Long-running queries hold locks, consume memory grants, and tie up worker threads. "
                "Applications sharing this server may experience connection pool exhaustion "
                "if a long-running query blocks downstream requests."
                if rel_dur > 0 else
                "Longest query duration improved. Application query response time should benefit."
            ),
            recommendation=(
                "Review the top queries in the Most Expensive Queries tab. "
                "Investigate execution plan changes, parameter sniffing, or missing indexes."
                if rel_dur > 0 else "Continue monitoring query duration trend."
            ),
            diagnostics=[
                "SELECT TOP 10 total_elapsed_time/execution_count AS avg_elapsed_us, "
                "SUBSTRING(st.text, (qs.statement_start_offset/2)+1, 200) AS query "
                "FROM sys.dm_exec_query_stats qs "
                "CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) st "
                "ORDER BY avg_elapsed_us DESC;",
            ],
        ))

    reads_c = _safe_float(cur_wl.get("max_logical_reads"))
    reads_p = _safe_float(prev_wl.get("max_logical_reads"))
    rel_reads = _rel_change(reads_c, reads_p)
    if rel_reads is not None and rel_reads >= 30:
        risk = HIGH if (reads_c or 0) >= 1_000_000 else MEDIUM
        findings.append(_finding(
            category="Workload",
            metric="Max Logical Reads (single query)",
            previous=_fmt(reads_p, ""),
            current=_fmt(reads_c, ""),
            delta_str=f"▲ {rel_reads:.0f}%",
            risk=risk,
            headline=f"Max logical reads per query increased by {rel_reads:.0f}% "
                     f"({_fmt(reads_p,'')} → {_fmt(reads_c,'')})",
            impact=(
                "Queries reading far more pages than before may be suffering from plan regressions, "
                "missing indexes, or stale statistics. Heavy read workloads evict cached pages "
                "from the buffer pool, increasing I/O pressure on all applications sharing this server."
            ),
            recommendation=(
                "Identify the high-read query in the Most Expensive Queries tab. "
                "Run SET STATISTICS IO ON and check for full table or clustered index scans. "
                "Consider adding a non-clustered index covering the query's filter and projection columns."
            ),
            diagnostics=[
                "SELECT TOP 10 total_logical_reads/execution_count AS avg_reads, "
                "SUBSTRING(st.text, (qs.statement_start_offset/2)+1, 200) AS query "
                "FROM sys.dm_exec_query_stats qs "
                "CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) st "
                "ORDER BY avg_reads DESC;",
            ],
        ))

    return findings


# ---------------------------------------------------------------------------
# Persistent risk detection
# ---------------------------------------------------------------------------

_RISK_CHECKS: List[Tuple[str, str, Any, str, str, str, str, str, List[str]]] = [
    # (profile_path, label, threshold_fn, risk_level, headline, impact, recommendation, diag)
    (
        "utilization.max_cpu_pct",
        "Persistent High CPU",
        lambda v: v is not None and v >= 85,
        HIGH,
        "CPU utilization remains ≥ 85% — persistent saturation risk",
        "Ongoing CPU saturation limits query concurrency. Applications will continue to experience "
        "elevated latency and risk of timeout under any increase in request volume.",
        "Prioritise CPU-bound query tuning or consider hardware upgrade if workload is legitimate.",
        ["SELECT TOP 5 total_worker_time/execution_count AS avg_cpu, "
         "SUBSTRING(st.text,(qs.statement_start_offset/2)+1,200) AS query "
         "FROM sys.dm_exec_query_stats qs "
         "CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) st ORDER BY avg_cpu DESC;"],
    ),
    (
        "utilization.max_memory_pct",
        "Persistent High Memory",
        lambda v: v is not None and v >= 85,
        HIGH,
        "Memory utilization remains ≥ 85% — sustained memory pressure",
        "Sustained memory pressure keeps PLE low and increases physical I/O for all connected applications.",
        "Review memory allocation across databases. Increase max server memory or add RAM if justified.",
        ["SELECT physical_memory_in_use_kb/1024 AS used_mb FROM sys.dm_os_process_memory;"],
    ),
    (
        "utilization.cache_ple_seconds",
        "Persistent Low PLE",
        lambda v: v is not None and v <= 300,
        HIGH,
        "PLE remains ≤ 300s — persistent buffer cache churn",
        "The buffer pool is not retaining pages long enough for cache reuse. "
        "Applications perform significantly more physical I/O than necessary on every query.",
        "Investigate large table scans or index operations that flush cache. Review memory allocation.",
        ["SELECT object_name, counter_name, cntr_value FROM sys.dm_os_performance_counters "
         "WHERE counter_name = 'Page life expectancy';"],
    ),
    (
        "io_stats.avg_read_latency_ms",
        "Persistent Elevated Read Latency",
        lambda v: v is not None and v >= 20,
        HIGH,
        "Read latency remains ≥ 20ms — sustained I/O read pressure",
        "Application SELECT queries continue to face slow disk reads. "
        "Response time SLAs for read-heavy workloads are at sustained risk.",
        "Check disk queue depth, storage firmware, and drive health. Review expensive read queries.",
        ["SELECT physical_name, io_stall_read_ms/NULLIF(num_of_reads,0) AS avg_read_ms "
         "FROM sys.dm_io_virtual_file_stats(NULL,NULL) vfs "
         "JOIN sys.master_files mf ON vfs.database_id = mf.database_id "
         "AND vfs.file_id = mf.file_id ORDER BY avg_read_ms DESC;"],
    ),
    (
        "io_stats.avg_write_latency_ms",
        "Persistent Elevated Write Latency",
        lambda v: v is not None and v >= 20,
        HIGH,
        "Write latency remains ≥ 20ms — sustained I/O write pressure",
        "Transaction commit times remain elevated. Applications performing DML will continue to experience "
        "higher latency per transaction, risking connection pool saturation under load.",
        "Isolate the transaction log to a dedicated drive. Review log growth events and VLF count.",
        ["SELECT DB_NAME(vfs.database_id) AS db, "
         "io_stall_write_ms/NULLIF(num_of_writes,0) AS avg_write_ms "
         "FROM sys.dm_io_virtual_file_stats(NULL,NULL) vfs ORDER BY avg_write_ms DESC;"],
    ),
    (
        "pressure.memory_grants_pending",
        "Persistent Memory Grant Queue",
        lambda v: v is not None and v > 0,
        HIGH,
        "Memory grants remain queued — sustained memory grant pressure",
        "Queries are continuously competing for memory grants. This creates a sustained throughput ceiling "
        "for any application relying on complex queries with sort or hash operations.",
        "Use Resource Governor to cap per-workload memory. Tune queries causing excessive grant requests.",
        ["SELECT session_id, requested_memory_kb, granted_memory_kb "
         "FROM sys.dm_exec_query_memory_grants ORDER BY requested_memory_kb DESC;"],
    ),
]


def _nested_get(obj: Dict, path: str) -> Any:
    """Navigate a dotted path like 'utilization.max_cpu_pct'."""
    keys = path.split(".")
    for k in keys:
        if not isinstance(obj, dict):
            return None
        obj = obj.get(k)
    return obj


def _detect_persistent_risks(
    cur_profile: Dict[str, Any], prev_profile: Dict[str, Any]
) -> List[Dict[str, Any]]:
    findings: List[Dict[str, Any]] = []
    for path, label, threshold_fn, risk, headline, impact, recommendation, diag in _RISK_CHECKS:
        cv = _nested_get(cur_profile, path)
        pv = _nested_get(prev_profile, path)
        if threshold_fn(cv) and threshold_fn(pv):
            findings.append(_finding(
                category="Persistent Risk",
                metric=label,
                previous=_fmt(_safe_float(pv), ""),
                current=_fmt(_safe_float(cv), ""),
                delta_str="Unchanged — risk persists",
                risk=risk,
                headline=f"⚠ {headline}",
                impact=impact,
                recommendation=recommendation,
                diagnostics=diag,
                persistent=True,
            ))
    return findings


# ---------------------------------------------------------------------------
# Finding builder
# ---------------------------------------------------------------------------

def _finding(
    *,
    category: str,
    metric: str,
    previous: str,
    current: str,
    delta_str: str,
    risk: str,
    headline: str,
    impact: str,
    recommendation: str,
    diagnostics: List[str],
    persistent: bool = False,
) -> Dict[str, Any]:
    return {
        "category": category,
        "metric": metric,
        "previous": previous,
        "current": current,
        "delta_str": delta_str,
        "risk": risk,
        "headline": headline,
        "impact": impact,
        "recommendation": recommendation,
        "diagnostics": diagnostics,
        "persistent": persistent,
    }


# ---------------------------------------------------------------------------
# Risk ordering
# ---------------------------------------------------------------------------

_RISK_ORDER = {HIGH: 0, MEDIUM: 1, LOW: 2}


def _sort_findings(findings: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return sorted(
        findings,
        key=lambda f: (
            _RISK_ORDER.get(f["risk"], 99),
            0 if f.get("persistent") else 1,
            f["category"],
        ),
    )


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

@st.cache_data(ttl=300)
def build_changes_report(server_name: str, current_date: str) -> Dict[str, Any]:
    """
    Build a structured changes report comparing *current_date* against the
    immediately preceding ingestion date for *server_name*.

    Returns a dict with keys:
        server_name         str
        current_date        str
        previous_date       str | None
        findings            List[Dict]   — sorted HIGH → MEDIUM → LOW
        has_previous        bool
        summary_counts      Dict[str, int]
    """
    report: Dict[str, Any] = {
        "server_name": server_name,
        "current_date": current_date,
        "previous_date": None,
        "findings": [],
        "has_previous": False,
        "summary_counts": {HIGH: 0, MEDIUM: 0, LOW: 0},
    }

    previous_date = get_previous_ingestion_date(server_name, current_date)
    if not previous_date:
        return report

    report["previous_date"] = previous_date
    report["has_previous"] = True

    with st.spinner("Loading current snapshot metrics…"):
        cur = build_server_profile(server_name, current_date)
    with st.spinner("Loading previous snapshot metrics for comparison…"):
        prev = build_server_profile(server_name, previous_date)

    all_findings: List[Dict[str, Any]] = []

    all_findings += _detect_persistent_risks(cur, prev)
    all_findings += _compare_utilization(
        cur.get("utilization") or {},
        prev.get("utilization") or {},
        cur.get("pressure") or {},
        prev.get("pressure") or {},
    )
    all_findings += _compare_io(
        cur.get("io_stats") or {},
        prev.get("io_stats") or {},
    )
    all_findings += _compare_waits(
        cur.get("waits_df"),
        prev.get("waits_df"),
    )
    all_findings += _compare_configuration(
        cur.get("configuration") or {},
        prev.get("configuration") or {},
    )
    all_findings += _compare_database_settings(
        cur.get("database_settings") or {},
        prev.get("database_settings") or {},
    )
    all_findings += _compare_backup(
        cur.get("backup_summary") or {},
        prev.get("backup_summary") or {},
    )
    all_findings += _compare_workload(
        cur.get("workload") or {},
        prev.get("workload") or {},
    )

    sorted_findings = _sort_findings(all_findings)

    counts = {HIGH: 0, MEDIUM: 0, LOW: 0}
    for f in sorted_findings:
        r = f.get("risk", LOW)
        counts[r] = counts.get(r, 0) + 1

    report["findings"] = sorted_findings
    report["summary_counts"] = counts

    return report


# ---------------------------------------------------------------------------
# AI executive briefing
# ---------------------------------------------------------------------------

def _build_briefing_prompt(
    server_name: str,
    current_date: str,
    previous_date: str,
    findings: List[Dict[str, Any]],
    counts: Dict[str, int],
) -> List[Dict[str, str]]:
    """Construct the LLM messages payload for an executive-level briefing."""

    # Compact summary of each finding — enough for the LLM without overwhelming tokens
    finding_lines: List[str] = []
    for i, f in enumerate(findings[:20], 1):
        risk   = f.get("risk", LOW)
        cat    = f.get("category", "")
        metric = f.get("metric", "")
        hl     = f.get("headline", "")
        prev_v = f.get("previous", "")
        cur_v  = f.get("current", "")
        delta  = f.get("delta_str", "")
        impact = f.get("impact", "")
        rec    = f.get("recommendation", "")
        tag    = " [PERSISTENT RISK]" if f.get("persistent") else ""

        finding_lines.append(
            f"{i}. [{risk}{tag}] {cat} — {hl}\n"
            f"   Change: {prev_v} → {cur_v} ({delta})\n"
            f"   Application impact: {impact}\n"
            f"   Recommendation: {rec}"
        )

    findings_text = "\n\n".join(finding_lines) if finding_lines else "No significant changes detected."

    high_n = counts.get(HIGH, 0)
    med_n  = counts.get(MEDIUM, 0)
    low_n  = counts.get(LOW, 0)

    system_msg = (
        "You are a senior SQL Server DBA and cloud observability expert. "
        "You write concise, technically precise executive briefings for engineering leadership. "
        "Your analysis prioritises application impact over raw metrics. "
        "You follow the diagnostic methodology of Glenn Berry (DMV-evidence-first, "
        "precise thresholds), Brent Ozar (priority-first, evidence-anchored action items), "
        "and Ola Hallengren (backup and maintenance safety). "
        "You do NOT invent data, speculate beyond the evidence, or pad with generic advice."
    )

    user_msg = f"""You are reviewing a server observability snapshot comparison for SQL Server instance `{server_name}`.

Comparison window: {previous_date} → {current_date}
Finding summary: {high_n} HIGH risk, {med_n} MEDIUM risk, {low_n} LOW risk

FINDINGS:
{findings_text}

Write a single executive briefing paragraph (4–7 sentences) that:
1. Opens with the overall risk posture — whether the server has deteriorated, improved, or is stable.
2. Names the 1–3 most significant changes and what they mean for applications connecting to databases on this server (e.g. connection latency, transaction throughput, query failures).
3. Flags any persistent risks (marked [PERSISTENT RISK]) that demand immediate action.
4. Closes with the single highest-priority recommended action for the on-call DBA.

Tone: concise, direct, and factual. No bullet points. No markdown headers. Plain prose only.
Do not repeat the server name or dates — they are shown separately in the UI.
Do not start with "In this report" or "Based on the findings" — lead with the risk posture directly."""

    return [
        {"role": "system", "content": system_msg},
        {"role": "user",   "content": user_msg},
    ]


def generate_ai_briefing(report: Dict[str, Any]) -> str:
    """
    Generate a one-paragraph AI executive briefing for the given changes report.

    Returns the LLM-generated text string.
    Raises on LLM failure — callers should handle gracefully.
    """
    findings = report.get("findings") or []
    counts   = report.get("summary_counts") or {}

    messages = _build_briefing_prompt(
        server_name   = report["server_name"],
        current_date  = report["current_date"],
        previous_date = report["previous_date"],
        findings      = findings,
        counts        = counts,
    )

    return chat_completion(
        messages,
        temperature = 0.20,
        max_tokens  = 420,
    )
