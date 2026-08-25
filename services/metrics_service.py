from __future__ import annotations

import json
import re
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd

from db.connection import run_query

# =============================================================================
# services/metrics_service.py
#
# Purpose:
# - Keep backward compatibility for current Streamlit app tabs
# - Produce cleaner, report-friendly evidence for report_service.py
# - Support deterministic SQL Server Health Assessment report generation
# =============================================================================


# ----------------------------
# Helper utilities
# ----------------------------
def _sql_quote(value: Any) -> str:
    return str(value).replace("'", "''")


def _pick_column(existing_cols: List[str], candidates: List[str]) -> Optional[str]:
    """Return first candidate that exists in existing_cols (case-insensitive)."""
    lower_map = {str(c).strip().lower(): c for c in existing_cols}
    for cand in candidates:
        if not cand:
            continue
        key = str(cand).strip().lower()
        if key in lower_map:
            return lower_map[key]
    return None


def _cols_map(df: pd.DataFrame) -> Dict[str, str]:
    return {str(c).strip().lower(): c for c in list(df.columns)} if isinstance(df, pd.DataFrame) else {}


def _num_series(df: pd.DataFrame, col: Optional[str]) -> pd.Series:
    if df is None or df.empty or not col or col not in df.columns:
        return pd.Series(dtype="float64")
    return pd.to_numeric(df[col], errors="coerce")


def _last_non_null(series: pd.Series) -> Any:
    try:
        s = series.dropna()
        if s.empty:
            return None
        return s.iloc[-1]
    except Exception:
        return None


def _safe_int(v: Any) -> Optional[int]:
    try:
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return None
        return int(float(v))
    except Exception:
        return None


def _safe_float(v: Any) -> Optional[float]:
    try:
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return None
        return float(v)
    except Exception:
        return None


def _safe_max_numeric(series: pd.Series) -> Optional[float]:
    try:
        s = pd.to_numeric(series, errors="coerce")
        if s.dropna().empty:
            return None
        return float(s.max())
    except Exception:
        return None


def _safe_min_numeric(series: pd.Series) -> Optional[float]:
    try:
        s = pd.to_numeric(series, errors="coerce")
        if s.dropna().empty:
            return None
        return float(s.min())
    except Exception:
        return None


def _safe_mean_numeric(series: pd.Series) -> Optional[float]:
    try:
        s = pd.to_numeric(series, errors="coerce").dropna()
        if s.empty:
            return None
        return float(s.mean())
    except Exception:
        return None


def _to_io_human(total_mb: Optional[float]) -> str:
    if total_mb is None:
        return "—"
    try:
        v = float(total_mb)
    except Exception:
        return "—"
    if v >= 1_000_000:
        return f"{v / 1_000_000:.1f} TB"
    if v >= 1_000:
        return f"{v / 1_000:.1f} GB"
    return f"{v:.0f} MB"


def _normalize_text(v: Any) -> str:
    return str(v).strip() if v is not None else ""


def _normalize_name(v: Any) -> str:
    return re.sub(r"\s+", " ", _normalize_text(v)).strip().lower()


def _coalesce(*vals: Any) -> Any:
    for v in vals:
        if v is None:
            continue
        if isinstance(v, float) and pd.isna(v):
            continue
        if isinstance(v, str) and not v.strip():
            continue
        return v
    return None


def _parse_dt_maybe(v: Any) -> Optional[datetime]:
    """Best-effort parse to datetime (supports common Excel/SQL string formats)."""
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    if isinstance(v, datetime):
        return v
    s = str(v).strip()
    if not s:
        return None
    try:
        ts = pd.to_datetime(s, errors="coerce")
        if pd.isna(ts):
            return None
        return ts.to_pydatetime()
    except Exception:
        return None


def _dedupe_preserve_order(values: List[Any]) -> List[Any]:
    out: List[Any] = []
    seen = set()
    for v in values:
        key = json.dumps(v, sort_keys=True, default=str) if isinstance(v, (dict, list)) else str(v)
        if key in seen:
            continue
        seen.add(key)
        out.append(v)
    return out


# ----------------------------
# Snapshot / sheet discovery
# ----------------------------
def _get_latest_snapshot(server: str) -> Optional[str]:
    q = f"""
    SELECT CAST(snapshot_date AS string) AS snapshot_date
    FROM ent_log_analytics.observability.v_latest_sql_diagnostics
    WHERE server_name = '{_sql_quote(server)}'
    LIMIT 1
    """
    df = run_query(q)
    if df.empty or "snapshot_date" not in df.columns:
        return None
    return str(df["snapshot_date"].iloc[0])


def _get_latest_snapshot_for_sheet(server_name: str, sheet_name: str) -> Optional[str]:
    q = f"""
    SELECT MAX(CAST(snapshot_date AS string)) AS snapshot_date
    FROM ent_log_analytics.observability.sql_diagnostics_bronze
    WHERE server_name = '{_sql_quote(server_name)}'
      AND sheet_name = '{_sql_quote(sheet_name)}'
    """
    df = run_query(q)
    if df.empty or "snapshot_date" not in df.columns:
        return None
    v = df["snapshot_date"].iloc[0]
    return str(v) if v is not None else None


def list_available_sheets_any(server_name: str) -> List[str]:
    q = f"""
    SELECT DISTINCT sheet_name
    FROM ent_log_analytics.observability.sql_diagnostics_bronze
    WHERE server_name = '{_sql_quote(server_name)}'
    """
    df = run_query(q)
    if df.empty or "sheet_name" not in df.columns:
        return []
    return df["sheet_name"].dropna().astype(str).tolist()


def list_available_sheets(server_name: str, snapshot: str) -> List[str]:
    q = f"""
    SELECT DISTINCT sheet_name
    FROM ent_log_analytics.observability.sql_diagnostics_bronze
    WHERE server_name = '{_sql_quote(server_name)}'
      AND CAST(snapshot_date AS string) = '{_sql_quote(snapshot)}'
    """
    df = run_query(q)
    if df.empty or "sheet_name" not in df.columns:
        return []
    return df["sheet_name"].dropna().astype(str).tolist()


def resolve_sheet_name(available: Iterable[str], patterns: List[str]) -> Optional[str]:
    avail = [str(x) for x in available if x is not None]
    for pat in patterns:
        rx = re.compile(pat, flags=re.IGNORECASE)
        for s in avail:
            if rx.search(s):
                return s
    return None


def resolve_sheet_names(available: Iterable[str], patterns: List[str]) -> List[str]:
    avail = [str(x) for x in available if x is not None]
    out: List[str] = []
    seen = set()
    for pat in patterns:
        rx = re.compile(pat, flags=re.IGNORECASE)
        for s in avail:
            if rx.search(s) and s.lower() not in seen:
                out.append(s)
                seen.add(s.lower())
    return out


# ----------------------------
# Bronze sheet fetch (expands row_json)
# ----------------------------
def _fetch_sheet(server: str, snapshot: str, sheet_name: str) -> pd.DataFrame:
    if not sheet_name:
        return pd.DataFrame()

    q = f"""
    SELECT *
    FROM ent_log_analytics.observability.sql_diagnostics_bronze
    WHERE server_name = '{_sql_quote(server)}'
      AND CAST(snapshot_date AS string) = '{_sql_quote(snapshot)}'
      AND sheet_name = '{_sql_quote(sheet_name)}'
    """
    df = run_query(q)
    if df.empty:
        return df

    if "row_json" not in df.columns:
        return df

    records: List[dict] = []
    for _, r in df.iterrows():
        raw = r.get("row_json", None)
        if raw is None or (isinstance(raw, float) and pd.isna(raw)):
            records.append({})
            continue
        if isinstance(raw, dict):
            records.append(raw)
            continue
        try:
            records.append(json.loads(raw))
        except Exception:
            records.append({"_row_json_parse_error": str(raw)})

    expanded = pd.DataFrame.from_records(records)
    meta_cols = [c for c in ["server_name", "snapshot_date", "sheet_name", "ingested_ts"] if c in df.columns]
    meta = df[meta_cols].reset_index(drop=True) if meta_cols else pd.DataFrame(index=df.index)

    for c in list(expanded.columns):
        if c in meta.columns:
            expanded.rename(columns={c: f"{c}__value"}, inplace=True)

    return pd.concat([meta.reset_index(drop=True), expanded.reset_index(drop=True)], axis=1)


def _fetch_sheet_latest(server_name: str, sheet_name: str) -> Tuple[pd.DataFrame, Optional[str]]:
    snap = _get_latest_snapshot_for_sheet(server_name, sheet_name)
    if not snap:
        return pd.DataFrame(), None
    try:
        return _fetch_sheet(server_name, snap, sheet_name), snap
    except Exception:
        return pd.DataFrame(), snap


# ----------------------------
# Version / instance parsing
# ----------------------------
def _parse_version_blob(v: str) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "sql_banner": None,
        "product_version": None,
        "product_level": None,
        "os_version_banner": None,
    }
    if not v or not isinstance(v, str):
        return out

    lines = [x.strip() for x in v.strip().splitlines() if x.strip()]
    first_line = lines[0] if lines else v.strip()

    year_match = re.search(r"SQL Server\s+(\d{4})", first_line, flags=re.IGNORECASE)
    year = year_match.group(1) if year_match else ""

    cu_match = re.search(r"\bCU(\d+)\b", first_line, flags=re.IGNORECASE)
    cu = f"CU{cu_match.group(1)}" if cu_match else ""

    ver_match = re.search(r"(\d+\.\d+\.\d+\.\d+)", first_line)
    if ver_match:
        out["product_version"] = ver_match.group(1)

    level_match = re.search(r"\((RTM|SP\d+|CU\d+)\)", first_line, flags=re.IGNORECASE)
    if level_match:
        out["product_level"] = level_match.group(1).upper()
    elif cu:
        out["product_level"] = cu

    if year and cu:
        out["sql_banner"] = f"SQL Server {year} {cu}"
    elif year:
        out["sql_banner"] = f"SQL Server {year}"
    else:
        out["sql_banner"] = "SQL Server"

    if len(lines) > 1:
        out["os_version_banner"] = lines[1]

    return out


def _extract_instance_info(
    df_server_props: pd.DataFrame,
    df_hw: pd.DataFrame,
    df_host: pd.DataFrame,
    df_version: pd.DataFrame,
) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "sql_banner": None,
        "edition": None,
        "cpu_count": None,
        "total_ram_mb": None,
        "os_name": None,
        "host_name": None,
        "product_version": None,
        "product_level": None,
        "os_version_banner": None,
        "sql_start_time": None,
    }

    if isinstance(df_version, pd.DataFrame) and not df_version.empty:
        for c in df_version.columns:
            if _normalize_name(c) in {
                "sql server and os version info",
                "sql server version",
                "version",
                "version info",
            }:
                blob = df_version[c].dropna().astype(str)
                if not blob.empty:
                    out.update({k: v for k, v in _parse_version_blob(blob.iloc[0]).items() if v is not None})
                break

    if isinstance(df_server_props, pd.DataFrame) and not df_server_props.empty:
        cols = _cols_map(df_server_props)
        edition_col = cols.get("edition")
        if edition_col:
            raw = _last_non_null(df_server_props[edition_col])
            if raw is not None:
                clean = str(raw).split(":")[0].strip()
                if "Enterprise" in clean:
                    out["edition"] = "Enterprise Edition"
                elif "Standard" in clean:
                    out["edition"] = "Standard Edition"
                elif "Developer" in clean:
                    out["edition"] = "Developer Edition"
                else:
                    out["edition"] = clean

        host_col = _pick_column(list(df_server_props.columns), ["Server Name", "Instance Name", "Machine Name", "Host Name"])
        if host_col:
            out["host_name"] = _coalesce(out.get("host_name"), _last_non_null(df_server_props[host_col]))

        start_col = _pick_column(list(df_server_props.columns), ["SQL Server Start Time", "sqlserver_start_time", "start time"])
        if start_col:
            out["sql_start_time"] = _coalesce(out.get("sql_start_time"), _last_non_null(df_server_props[start_col]))

    if isinstance(df_hw, pd.DataFrame) and not df_hw.empty:
        cols = _cols_map(df_hw)
        for key in ["logical cpu count", "scheduler count", "physical core count"]:
            if key in cols:
                out["cpu_count"] = _coalesce(out.get("cpu_count"), _safe_int(_last_non_null(_num_series(df_hw, cols[key]))))
                if out["cpu_count"] is not None:
                    break

        for key in ["physical memory (mb)", "physical memory mb", "total physical memory (mb)", "total physical memory mb"]:
            if key in cols:
                out["total_ram_mb"] = _coalesce(out.get("total_ram_mb"), _safe_int(_last_non_null(_num_series(df_hw, cols[key]))))
                if out["total_ram_mb"] is not None:
                    break

    if isinstance(df_host, pd.DataFrame) and not df_host.empty:
        cols = _cols_map(df_host)
        for key in ["host_distribution", "host distribution", "os name", "operating system"]:
            if key in cols:
                raw = _last_non_null(df_host[cols[key]])
                if raw is not None:
                    out["os_name"] = str(raw)
                    break
        for key in ["host_name", "host name", "machine name", "server name"]:
            if key in cols:
                raw = _last_non_null(df_host[cols[key]])
                if raw is not None:
                    out["host_name"] = _coalesce(out.get("host_name"), str(raw))
                    break

    return out


# ----------------------------
# Metric extractors
# ----------------------------
def _extract_cpu_max(df: pd.DataFrame) -> Optional[float]:
    if df.empty:
        return None
    cpu_col = _pick_column(
        list(df.columns),
        [
            "SQL Server Process CPU Utilization",
            "sql_server_process_cpu_utilization",
            "sql server process cpu utilization",
            "sqlservercpu",
            "cpu_percent",
            "cpu",
            "max_cpu_utilization",
            "max_cpu",
            "SQL Server CPU",
        ],
    )
    return _safe_max_numeric(df[cpu_col]) if cpu_col else None


def _extract_memory_pct(df_proc_mem: pd.DataFrame, df_sys_mem: pd.DataFrame) -> Optional[float]:
    if df_proc_mem.empty or df_sys_mem.empty:
        return None

    proc_col = _pick_column(
        list(df_proc_mem.columns),
        [
            "SQL Server Memory Usage (MB)",
            "sql_server_memory_usage_mb",
            "sql_server_memory_mb",
            "sql_memory_mb",
            "SQL Server Memory Usage",
            "sql_memory_usage_mb",
            "sql_memory_used_mb",
        ],
    )
    phys_col = _pick_column(
        list(df_sys_mem.columns),
        [
            "Physical Memory (MB)",
            "physical_memory_mb",
            "total_physical_memory_mb",
            "Total Physical Memory (MB)",
            "physical_memory",
        ],
    )
    if not proc_col or not phys_col:
        return None

    sql_used = _num_series(df_proc_mem, proc_col).dropna()
    phys_total = _num_series(df_sys_mem, phys_col).dropna()
    if sql_used.empty or phys_total.empty:
        return None
    sql_val = float(sql_used.iloc[-1])
    phys_val = float(phys_total.iloc[-1])
    if phys_val == 0:
        return None
    return float(sql_val / phys_val * 100.0)


def _extract_ple(df: pd.DataFrame) -> Optional[float]:
    if df.empty:
        return None
    ple_col = _pick_column(list(df.columns), ["Page Life Expectancy", "page_life_expectancy", "PLE", "ple_seconds"])
    return _safe_min_numeric(df[ple_col]) if ple_col else None


def _extract_io_stats(df_drive: pd.DataFrame, df_file: pd.DataFrame, df_db: pd.DataFrame) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "drive_max_overall_latency_ms": None,
        "drive_avg_overall_latency_ms": None,
        "avg_read_latency_ms": None,
        "avg_write_latency_ms": None,
        "avg_io_latency_ms": None,
        "total_read_io_mb": None,
        "total_write_io_mb": None,
        "total_io_mb": None,
        "total_io_str": "—",
    }

    if not df_drive.empty:
        col = _pick_column(list(df_drive.columns), ["Overall Latency", "overall latency", "overall_latency_ms"])
        if col:
            s = _num_series(df_drive, col).dropna()
            if not s.empty:
                out["drive_max_overall_latency_ms"] = float(s.max())
                out["drive_avg_overall_latency_ms"] = float(s.mean())

    if not df_file.empty:
        read_col = _pick_column(list(df_file.columns), ["avg_read_latency_ms", "Avg Read Latency", "Avg Read Latency (ms)"])
        write_col = _pick_column(list(df_file.columns), ["avg_write_latency_ms", "Avg Write Latency", "Avg Write Latency (ms)"])
        io_col = _pick_column(list(df_file.columns), ["avg_io_latency_ms", "Avg IO Latency", "Avg I/O Latency (ms)"])

        for col, key in [
            (read_col, "avg_read_latency_ms"),
            (write_col, "avg_write_latency_ms"),
            (io_col, "avg_io_latency_ms"),
        ]:
            if col:
                s = _num_series(df_file, col).dropna()
                if not s.empty:
                    out[key] = float(s.mean())

    if not df_db.empty:
        read_col = _pick_column(list(df_db.columns), ["Read I/O (MB)", "read io (mb)"])
        write_col = _pick_column(list(df_db.columns), ["Write I/O (MB)", "write io (mb)"])
        total_col = _pick_column(list(df_db.columns), ["Total I/O (MB)", "total io (mb)"])

        for col, key in [
            (read_col, "total_read_io_mb"),
            (write_col, "total_write_io_mb"),
            (total_col, "total_io_mb"),
        ]:
            if col:
                s = _num_series(df_db, col).dropna()
                if not s.empty:
                    out[key] = float(s.sum())

    out["total_io_str"] = _to_io_human(out.get("total_io_mb"))
    return out


def _extract_waits(
    server_name: str,
    available_sheets: Optional[List[str]] = None,
    snapshot: Optional[str] = None,
) -> pd.DataFrame:
    """Return top waits with normalized columns."""
    try:
        candidates: List[str] = []
        if available_sheets:
            candidates = [s for s in available_sheets if isinstance(s, str) and "wait" in s.lower()]
        if not candidates:
            candidates = [s for s in list_available_sheets_any(server_name) if "wait" in s.lower()]
        if not candidates:
            return pd.DataFrame()

        best_sheet = None
        best_snap = None
        best_rows = -1

        for s in candidates:
            snap = snapshot or _get_latest_snapshot_for_sheet(server_name, s)
            if not snap:
                continue
            try:
                q = f"""
                SELECT COUNT(*) AS n
                FROM ent_log_analytics.observability.sql_diagnostics_bronze
                WHERE server_name = '{_sql_quote(server_name)}'
                  AND CAST(snapshot_date AS string) = '{_sql_quote(snap)}'
                  AND sheet_name = '{_sql_quote(s)}'
                """
                cnt_df = run_query(q)
                n = int(cnt_df["n"].iloc[0]) if (not cnt_df.empty and "n" in cnt_df.columns and cnt_df["n"].iloc[0] is not None) else 0
            except Exception:
                n = 0

            if best_snap is None or str(snap) > str(best_snap) or (str(snap) == str(best_snap) and n > best_rows):
                best_sheet, best_snap, best_rows = s, snap, n

        if not best_sheet or not best_snap or best_rows <= 0:
            return pd.DataFrame()

        df = _fetch_sheet(server_name, best_snap, best_sheet)
        if df.empty:
            return pd.DataFrame()

        waits_df = df.rename(
            columns={
                "WaitType": "wait_type",
                "Wait Type": "wait_type",
                "Wait Percentage": "wait_pct",
                "Wait%": "wait_pct",
                "AvgWait_Sec": "avg_wait_s",
                "AvgRes_Sec": "avg_resource_s",
                "AvgSig_Sec": "avg_signal_s",
            }
        ).copy()

        # rescue names if the direct rename did not catch
        if "wait_type" not in waits_df.columns:
            wt = _pick_column(list(waits_df.columns), ["wait_type", "WaitType", "Wait Type"])
            if wt:
                waits_df["wait_type"] = waits_df[wt]

        if "wait_pct" not in waits_df.columns:
            wp = _pick_column(list(waits_df.columns), ["wait_pct", "Wait Percentage", "Wait%", "%"])
            if wp:
                waits_df["wait_pct"] = waits_df[wp]

        for c in ["wait_pct", "avg_wait_s", "avg_resource_s", "avg_signal_s"]:
            if c in waits_df.columns:
                waits_df[c] = pd.to_numeric(waits_df[c], errors="coerce")

        if "wait_pct" in waits_df.columns:
            waits_df = waits_df.sort_values("wait_pct", ascending=False)
        elif "avg_wait_s" in waits_df.columns:
            waits_df = waits_df.sort_values("avg_wait_s", ascending=False)

        keep = [c for c in ["wait_type", "wait_pct", "avg_wait_s", "avg_resource_s", "avg_signal_s"] if c in waits_df.columns]
        return waits_df[keep].head(15).reset_index(drop=True) if keep else pd.DataFrame()
    except Exception:
        return pd.DataFrame()


# ----------------------------
# Configuration and DB property extraction
# ----------------------------
def _extract_configuration_values(df_conf: pd.DataFrame) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "maxdop": None,
        "cost_threshold": None,
        "max_server_memory_mb": None,
        "optimize_for_adhoc": None,
        "backup_compression_default": None,
        "backup_checksum_default": None,
        "remote_admin_connections": None,
    }
    if df_conf.empty:
        return out

    cols = _cols_map(df_conf)
    name_col = cols.get("name")
    viu_col = cols.get("value_in_use") or cols.get("value in use")
    val_col = cols.get("value")
    if not name_col or not (viu_col or val_col):
        return out

    use_col = viu_col or val_col
    tmp = df_conf[[name_col, use_col]].copy()
    tmp[name_col] = tmp[name_col].astype(str).map(_normalize_name)

    def _get_exact(name: str) -> Any:
        m = tmp[tmp[name_col] == _normalize_name(name)]
        return None if m.empty else m[use_col].iloc[-1]

    out["maxdop"] = _get_exact("max degree of parallelism")
    out["cost_threshold"] = _get_exact("cost threshold for parallelism")
    out["max_server_memory_mb"] = _get_exact("max server memory (mb)")
    out["optimize_for_adhoc"] = _get_exact("optimize for ad hoc workloads")
    out["backup_compression_default"] = _get_exact("backup compression default")
    out["backup_checksum_default"] = _get_exact("backup checksum default")
    out["remote_admin_connections"] = _get_exact("remote admin connections")
    return out


def _boolish_state(v: Any) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip().lower()
    if s in {"1", "true", "enabled", "yes", "on"}:
        return "Enabled"
    if s in {"0", "false", "disabled", "no", "off"}:
        return "Disabled"
    return str(v)


def _extract_page_verify_summary(server_name: str, available: List[str], snapshot: Optional[str] = None) -> Dict[str, Any]:
    result: Dict[str, Any] = {
        "system_dbs_page_verify": None,
        "user_dbs_page_verify": None,
        "user_dbs_with_page_verify_none": [],
        "system_db_values": [],
        "user_db_values": [],
        "system_db_none_count": 0,
        "user_db_none_count": 0,
        "user_db_checksum_count": 0,
    }

    page_verify_sheets = resolve_sheet_names(
        available,
        [
            r"page[_\s-]*verify",
            r"database.*page[_\s-]*verify",
            r"database.*settings",
            r"database.*properties",
        ],
    )

    for sheet in page_verify_sheets:
        if not snapshot:
            continue
        df = _fetch_sheet(server_name, snapshot, sheet)
        if df.empty:
            continue

        db_col = _pick_column(list(df.columns), ["Database Name", "database_name", "Database", "name"])
        pv_col = _pick_column(
            list(df.columns),
            ["Page Verify", "page_verify", "page_verify_option_desc", "page verify option", "Page Verify Option"],
        )
        if not db_col or not pv_col:
            continue

        tmp = df[[db_col, pv_col]].copy()
        tmp[db_col] = tmp[db_col].astype(str).str.strip()
        tmp[pv_col] = tmp[pv_col].astype(str).str.strip().str.upper()
        tmp = tmp[tmp[db_col] != ""]
        if tmp.empty:
            continue

        sys_db_names = {"master", "model", "msdb", "tempdb"}
        tmp["is_system"] = tmp[db_col].str.lower().isin(sys_db_names)

        sys_vals = tmp.loc[tmp["is_system"], pv_col].dropna().astype(str).unique().tolist()
        user_vals = tmp.loc[~tmp["is_system"], pv_col].dropna().astype(str).unique().tolist()
        none_dbs = tmp.loc[(~tmp["is_system"]) & (tmp[pv_col] == "NONE"), db_col].dropna().astype(str).tolist()

        result["system_db_values"] = sys_vals
        result["user_db_values"] = user_vals
        result["system_db_none_count"] = int((tmp.loc[tmp["is_system"], pv_col] == "NONE").sum())
        result["user_db_none_count"] = int((tmp.loc[~tmp["is_system"], pv_col] == "NONE").sum())
        result["user_db_checksum_count"] = int((tmp.loc[~tmp["is_system"], pv_col] == "CHECKSUM").sum())
        result["user_dbs_with_page_verify_none"] = none_dbs[:20]
        result["system_dbs_page_verify"] = ", ".join(sys_vals[:3]) if sys_vals else None
        result["user_dbs_page_verify"] = ", ".join(user_vals[:3]) if user_vals else None
        break

    return result


# ----------------------------
# Workload / hotspot extraction
# ----------------------------
def _detect_workload_sheet_roles(available: List[str]) -> Dict[str, Optional[str]]:
    return {
        "top_worker_time": resolve_sheet_name(available, [r"top\s+worker\s+time\s+queries"]),
        "top_logical_reads": resolve_sheet_name(available, [r"top\s+logical\s+reads\s+queries", r"top\s+io\s+statements"]),
        "top_elapsed": resolve_sheet_name(available, [r"top\s+avg\s+elapsed\s+time", r"top\s+elapsed\s+time", r"top\s+duration"]),
        "most_expensive": resolve_sheet_name(available, [r"most\s+expensive\s+queries"]),
    }


def _extract_hotspot_rows(df: pd.DataFrame, sheet_name: str, role: str, limit: int = 5) -> List[Dict[str, Any]]:
    """Extract report-grade query hotspot rows from a diagnostics sheet.

    The ingestion CSVs are not guaranteed to use one exact column naming scheme,
    so this function keeps the original compatibility fields while adding a
    normalized metric payload for deterministic report analysis.
    """
    if df.empty:
        return []

    cols = list(df.columns)

    def pick(candidates: List[str]) -> Optional[str]:
        return _pick_column(cols, candidates)

    obj_col = pick([
        "Stored Procedure Name",
        "Procedure Name",
        "Object Name",
        "Query Name",
        "Database Object",
        "Name",
        "Short Query Text",
        "Query Text",
        "SQL Text",
        "Statement Text",
    ])
    query_text_col = pick(["Query Text", "SQL Text", "Statement Text", "Short Query Text", "Text", "Query"])
    db_col = pick(["Database Name", "database_name", "Database", "DB Name"])
    query_hash_col = pick(["Query Hash", "query_hash", "query_hash_hex"])
    plan_hash_col = pick(["Query Plan Hash", "Plan Hash", "query_plan_hash", "plan_hash"])
    plan_col = pick(["Query Plan", "Plan XML", "query_plan", "plan_xml", "Showplan XML"])

    metric_candidates = {
        "top_worker_time": ["Total Worker Time", "Total Worker Time (ms)", "Avg Worker Time", "Worker Time", "total_worker_time"],
        "top_logical_reads": ["Total Logical Reads", "Avg Logical Reads", "Logical Reads", "logical_reads"],
        "top_elapsed": ["Avg Elapsed Time", "Avg Elapsed Time (ms)", "Total Elapsed Time", "Elapsed Time", "avg_elapsed_time", "duration_seconds", "avg_duration_ms"],
        "most_expensive": ["Total Worker Time", "Total Logical Reads", "Avg Elapsed Time", "Execution Count", "duration_seconds", "avg_duration_ms"],
    }
    metric_col = pick(metric_candidates.get(role, []))

    metric_aliases: Dict[str, List[str]] = {
        "total_worker_time": ["Total Worker Time", "Total Worker Time (ms)", "total_worker_time", "Worker Time"],
        "avg_worker_time": ["Avg Worker Time", "Average Worker Time", "Avg Worker Time (ms)", "avg_worker_time"],
        "total_logical_reads": ["Total Logical Reads", "total_logical_reads", "Logical Reads"],
        "avg_logical_reads": ["Avg Logical Reads", "Average Logical Reads", "Logical Reads/Execution", "avg_logical_reads"],
        "total_elapsed_time": ["Total Elapsed Time", "Total Elapsed Time (ms)", "Elapsed Time", "Duration", "total_elapsed_time"],
        "avg_elapsed_time": ["Avg Elapsed Time", "Avg Elapsed Time (ms)", "Average Elapsed Time", "avg_elapsed_time", "avg_duration_ms", "duration_seconds"],
        "execution_count": ["Execution Count", "Executions", "execution_count", "execution_count_total"],
        "total_physical_reads": ["Total Physical Reads", "Physical Reads", "total_physical_reads"],
        "total_logical_writes": ["Total Logical Writes", "Logical Writes", "Writes", "total_logical_writes"],
        "granted_memory_kb": ["Granted Memory KB", "Max Grant KB", "Memory Grant KB", "granted_memory_kb"],
        "used_memory_kb": ["Used Memory KB", "Max Used Grant KB", "Used Grant KB", "used_memory_kb"],
        "spills": ["Spills", "Total Spills", "TempDB Spills", "spills"],
        "tempdb_allocations": ["TempDB Allocations", "Tempdb Allocations", "tempdb_allocations"],
    }
    canonical_cols = {name: pick(aliases) for name, aliases in metric_aliases.items()}

    work = df.copy()
    sort_col = metric_col
    if not sort_col:
        # Prefer the strongest available metric for stable ordering.
        for candidate in [
            canonical_cols.get("total_logical_reads"),
            canonical_cols.get("total_worker_time"),
            canonical_cols.get("total_elapsed_time"),
            canonical_cols.get("execution_count"),
        ]:
            if candidate:
                sort_col = candidate
                break
    if sort_col:
        work["__metric"] = pd.to_numeric(work[sort_col], errors="coerce")
        work = work.sort_values("__metric", ascending=False, na_position="last")
    work = work.head(limit)

    out: List[Dict[str, Any]] = []
    for _, row in work.iterrows():
        metric_value = row.get(metric_col) if metric_col else None
        obj_name = None
        if obj_col and pd.notna(row.get(obj_col)):
            obj_name = str(row.get(obj_col)).replace("\n", " ").strip()
        query_text = None
        if query_text_col and pd.notna(row.get(query_text_col)):
            query_text = str(row.get(query_text_col)).replace("\n", " ").strip()

        item: Dict[str, Any] = {
            "source_sheet": sheet_name,
            "bucket": role,
            "object_name": obj_name,
            "database_name": str(row.get(db_col)) if db_col and pd.notna(row.get(db_col)) else None,
            "query_text": query_text,
            "query_hash": str(row.get(query_hash_col)) if query_hash_col and pd.notna(row.get(query_hash_col)) else None,
            "plan_hash": str(row.get(plan_hash_col)) if plan_hash_col and pd.notna(row.get(plan_hash_col)) else None,
            "query_plan": str(row.get(plan_col))[:2500] if plan_col and pd.notna(row.get(plan_col)) else None,
            "metric_name": metric_col,
            "metric_value": None
            if metric_value is None or (isinstance(metric_value, float) and pd.isna(metric_value))
            else str(metric_value),
        }

        for canonical_name, source_col in canonical_cols.items():
            item[canonical_name] = _safe_float(row.get(source_col)) if source_col else None

        # Include a compact raw preview for auditability without bloating the LLM payload.
        raw_preview: Dict[str, Any] = {}
        for source_col in [obj_col, db_col, query_text_col, query_hash_col, plan_hash_col, metric_col]:
            if source_col and source_col in row.index and pd.notna(row.get(source_col)):
                raw_preview[str(source_col)] = str(row.get(source_col))[:500]
        item["raw_preview"] = raw_preview
        out.append(item)
    return out


def _extract_workload_summary(server_name: str, available: List[str], snapshot: Optional[str] = None) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "top_query_count": 0,
        "max_duration_s": None,
        "max_logical_reads": None,
        "top_cpu_queries_count": 0,
        "top_elapsed_queries_count": 0,
        "top_io_queries_count": 0,
        "high_impact_hotspots": [],
    }

    roles = _detect_workload_sheet_roles(available)
    hotspot_rows: List[Dict[str, Any]] = []

    for role, sheet in roles.items():
        if not sheet:
            continue
        if not snapshot:
            continue
        df = _fetch_sheet(server_name, snapshot, sheet)
        if df.empty:
            continue

        if role == "top_worker_time":
            out["top_cpu_queries_count"] = int(df.shape[0])
        elif role == "top_elapsed":
            out["top_elapsed_queries_count"] = int(df.shape[0])
        elif role == "top_logical_reads":
            out["top_io_queries_count"] = int(df.shape[0])
        elif role == "most_expensive":
            out["top_query_count"] = int(df.shape[0])

        if out["top_query_count"] == 0:
            out["top_query_count"] = int(df.shape[0])

        dur_col = _pick_column(
            list(df.columns),
            ["Avg Elapsed Time", "Avg Elapsed Time (ms)", "Total Elapsed Time", "Elapsed Time", "avg_elapsed_time", "duration_seconds", "avg_duration_ms"],
        )
        if dur_col:
            s = _num_series(df, dur_col).dropna()
            if not s.empty:
                val = float(s.max())
                if "ms" in str(dur_col).lower() or val > 10000:
                    val = val / 1000.0
                out["max_duration_s"] = max([x for x in [out.get("max_duration_s"), val] if x is not None], default=val)

        reads_col = _pick_column(list(df.columns), ["Total Logical Reads", "Avg Logical Reads", "Logical Reads", "logical_reads"])
        if reads_col:
            s = _num_series(df, reads_col).dropna()
            if not s.empty:
                val = int(s.max())
                out["max_logical_reads"] = max([x for x in [out.get("max_logical_reads"), val] if x is not None], default=val)

        hotspot_rows.extend(_extract_hotspot_rows(df, sheet, role))

    deduped: List[Dict[str, Any]] = []
    seen = set()
    for row in hotspot_rows:
        key = (row.get("object_name"), row.get("metric_name"), row.get("metric_value"), row.get("bucket"))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)
        if len(deduped) >= 12:
            break

    out["high_impact_hotspots"] = deduped
    return out


# ----------------------------
# Report-grade additive evidence
# ----------------------------
def _extract_last_backup_summary(server_name: str, available: List[str], snapshot: Optional[str] = None) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "sheet": None,
        "databases_total": 0,
        "databases_missing_full_backup": 0,
        "oldest_full_backup_days": None,
        "latest_full_backup": None,
        "rows_preview": [],
    }

    sheet = resolve_sheet_name(available, [r"last\s+backup\s+by\s+database", r"backup\s+by\s+database", r"\blast\s+backup\b"])
    if not sheet:
        return out

    if not snapshot:
        out["sheet"] = sheet
        return out

    df = _fetch_sheet(server_name, snapshot, sheet)
    if df.empty:
        out["sheet"] = sheet
        return out

    db_col = _pick_column(list(df.columns), ["Database Name", "database_name", "Database", "name"])
    full_col = _pick_column(list(df.columns), ["Last Full Backup", "last_full_backup", "last full backup", "last_full_backup_date"])
    diff_col = _pick_column(list(df.columns), ["Last Diff Backup", "last_diff_backup", "last differential backup"])
    log_col = _pick_column(list(df.columns), ["Last Log Backup", "last_log_backup", "last log backup"])

    if not db_col:
        out["sheet"] = sheet
        return out

    tmp = df.copy()
    tmp[db_col] = tmp[db_col].astype(str).str.strip()
    tmp = tmp[tmp[db_col] != ""]
    out["sheet"] = sheet
    out["databases_total"] = int(tmp.shape[0])

    now = datetime.utcnow()
    if full_col and full_col in tmp.columns:
        tmp["_full_dt"] = tmp[full_col].map(_parse_dt_maybe)
        missing_full = tmp["_full_dt"].isna().sum()
        out["databases_missing_full_backup"] = int(missing_full)
        try:
            valid = tmp["_full_dt"].dropna()
            if not valid.empty:
                oldest = min(valid)
                latest = max(valid)
                out["oldest_full_backup_days"] = int((now - oldest).days)
                out["latest_full_backup"] = latest.isoformat(sep=" ")
        except Exception:
            pass

    preview_cols = [c for c in [db_col, full_col, diff_col, log_col] if c and c in tmp.columns]
    try:
        prev = tmp[preview_cols].head(12)
        out["rows_preview"] = prev.fillna("").astype(str).to_dict(orient="records")
    except Exception:
        out["rows_preview"] = []

    return out


def _extract_tempdb_summary(server_name: str, available: List[str], snapshot: Optional[str] = None) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "files_sheet": None,
        "sizes_sheet": None,
        "data_files_count": None,
        "tempdb_total_size_mb": None,
        "version_store_mb": None,
        "files": [],
    }

    files_sheet = resolve_sheet_name(available, [r"tempdb\s+data\s+files", r"\btempdb\b.*data\s+files"])
    sizes_sheet = resolve_sheet_name(available, [r"tempdb.*file\s+sizes", r"tempdb\s+data\s+file\s+sizes"])
    version_store_sheet = resolve_sheet_name(available, [r"version\s+store\s+space\s+usage"])

    df_files = pd.DataFrame()
    df_sizes = pd.DataFrame()
    df_vs = pd.DataFrame()

    if files_sheet and snapshot:
        df_files = _fetch_sheet(server_name, snapshot, files_sheet)
        out["files_sheet"] = files_sheet

    if sizes_sheet and snapshot:
        df_sizes = _fetch_sheet(server_name, snapshot, sizes_sheet)
        out["sizes_sheet"] = sizes_sheet

    if version_store_sheet and snapshot:
        df_vs = _fetch_sheet(server_name, snapshot, version_store_sheet)

    candidates = []
    if isinstance(df_files, pd.DataFrame) and not df_files.empty:
        candidates.append(df_files)
    if isinstance(df_sizes, pd.DataFrame) and not df_sizes.empty:
        candidates.append(df_sizes)

    if candidates:
        base = candidates[0].copy()

        file_col = _pick_column(list(base.columns), ["File Name", "Logical Name", "name", "logical_name"])
        size_col = _pick_column(list(base.columns), ["Size (MB)", "size_mb", "File Size (MB)", "size (mb)"])
        growth_col = _pick_column(list(base.columns), ["Growth", "growth", "Growth (MB)", "growth_mb"])
        is_pct_col = _pick_column(list(base.columns), ["Is Percent Growth", "is_percent_growth", "percent growth"])

        if file_col:
            base[file_col] = base[file_col].astype(str).str.strip()
            base = base[base[file_col] != ""]

        files: List[Dict[str, Any]] = []
        for _, r in base.head(16).iterrows():
            files.append(
                {
                    "file": str(r.get(file_col)) if file_col else None,
                    "size_mb": None if not size_col else _safe_int(r.get(size_col)),
                    "growth": str(r.get(growth_col)) if growth_col and pd.notna(r.get(growth_col)) else None,
                    "is_percent_growth": str(r.get(is_pct_col)) if is_pct_col and pd.notna(r.get(is_pct_col)) else None,
                }
            )

        out["files"] = files
        out["data_files_count"] = len([x for x in files if x.get("file")])

    if not df_sizes.empty:
        size_col = _pick_column(list(df_sizes.columns), ["File Size (MB)", "Size (MB)", "size_mb"])
        if size_col:
            s = _num_series(df_sizes, size_col).dropna()
            if not s.empty:
                out["tempdb_total_size_mb"] = float(s.sum())

    if not df_vs.empty:
        vs_col = _pick_column(list(df_vs.columns), ["Version Store Space in tempdb (MB)", "Version Store MB", "version_store_mb", "ReservedSpaceMB"])
        if vs_col:
            s = _num_series(df_vs, vs_col).dropna()
            if not s.empty:
                out["version_store_mb"] = float(s.max())

    return out


def _extract_io_details(df_drive: pd.DataFrame, df_file: pd.DataFrame, df_db: pd.DataFrame) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "worst_drives": [],
        "worst_files": [],
        "top_io_databases": [],
        "max_drive_latency_ms": None,
        "avg_drive_latency_ms": None,
        "avg_file_read_latency_ms": None,
        "avg_file_write_latency_ms": None,
        "top_db_by_io": None,
    }

    if isinstance(df_drive, pd.DataFrame) and not df_drive.empty:
        lat_col = _pick_column(list(df_drive.columns), ["Overall Latency", "overall latency", "overall_latency_ms"])
        name_col = _pick_column(list(df_drive.columns), ["Drive", "Drive Letter", "drive", "volume", "mount"])
        if lat_col:
            work = df_drive.copy()
            work["_lat"] = pd.to_numeric(work[lat_col], errors="coerce")
            non_null = work["_lat"].dropna()
            if not non_null.empty:
                out["max_drive_latency_ms"] = float(non_null.max())
                out["avg_drive_latency_ms"] = float(non_null.mean())
            work = work.sort_values("_lat", ascending=False, na_position="last").head(8)
            for _, r in work.iterrows():
                out["worst_drives"].append(
                    {
                        "drive": str(r.get(name_col)) if name_col and pd.notna(r.get(name_col)) else None,
                        "overall_latency_ms": None if pd.isna(r.get("_lat")) else float(r.get("_lat")),
                    }
                )

    if isinstance(df_file, pd.DataFrame) and not df_file.empty:
        io_col = _pick_column(list(df_file.columns), ["Avg IO Latency", "Avg I/O Latency (ms)", "avg_io_latency_ms"])
        file_col = _pick_column(list(df_file.columns), ["File Name", "Database File", "file_name", "Logical Name", "logical_name"])
        read_col = _pick_column(list(df_file.columns), ["Avg Read Latency", "Avg Read Latency (ms)", "avg_read_latency_ms"])
        write_col = _pick_column(list(df_file.columns), ["Avg Write Latency", "Avg Write Latency (ms)", "avg_write_latency_ms"])

        if read_col:
            s = _num_series(df_file, read_col).dropna()
            if not s.empty:
                out["avg_file_read_latency_ms"] = float(s.mean())
        if write_col:
            s = _num_series(df_file, write_col).dropna()
            if not s.empty:
                out["avg_file_write_latency_ms"] = float(s.mean())

        if io_col:
            work = df_file.copy()
            work["_iolat"] = pd.to_numeric(work[io_col], errors="coerce")
            work = work.sort_values("_iolat", ascending=False, na_position="last").head(10)
            for _, r in work.iterrows():
                out["worst_files"].append(
                    {
                        "file": str(r.get(file_col)) if file_col and pd.notna(r.get(file_col)) else None,
                        "avg_io_latency_ms": None if pd.isna(r.get("_iolat")) else float(r.get("_iolat")),
                    }
                )

    if isinstance(df_db, pd.DataFrame) and not df_db.empty:
        db_col = _pick_column(list(df_db.columns), ["Database Name", "database_name", "Database"])
        total_col = _pick_column(list(df_db.columns), ["Total I/O (MB)", "total io (mb)"])
        if db_col and total_col:
            work = df_db.copy()
            work["_tio"] = pd.to_numeric(work[total_col], errors="coerce")
            work = work.sort_values("_tio", ascending=False, na_position="last").head(10)
            for _, r in work.iterrows():
                out["top_io_databases"].append(
                    {
                        "database": str(r.get(db_col)) if pd.notna(r.get(db_col)) else None,
                        "total_io_mb": None if pd.isna(r.get("_tio")) else float(r.get("_tio")),
                    }
                )
            if not work.empty and pd.notna(work.iloc[0].get(db_col)) and pd.notna(work.iloc[0].get("_tio")):
                out["top_db_by_io"] = f"{work.iloc[0][db_col]} ({float(work.iloc[0]['_tio']):,.2f} MB)"

    return out


def _summarize_waits_for_report(waits_df: Any) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "categories": [],
        "top_waits": [],
        "dominant_category": None,
        "dominant_category_pct": None,
    }

    if not isinstance(waits_df, pd.DataFrame) or waits_df.empty:
        return out

    df = waits_df.copy()
    if "wait_type" not in df.columns:
        return out

    pct_col = "wait_pct" if "wait_pct" in df.columns else None
    if pct_col:
        df[pct_col] = pd.to_numeric(df[pct_col], errors="coerce")
    else:
        return out

    def cat(w: str) -> str:
        w = (w or "").upper()
        if w.startswith("CX") or "PARALLEL" in w:
            return "Parallelism / synchronization"
        if "PAGEIOLATCH" in w or "IOCOMPLETION" in w:
            return "Storage / I/O"
        if "PAGELATCH" in w or w.startswith("LATCH"):
            return "Latch / TempDB contention"
        if w.startswith("SOS_SCHEDULER_YIELD") or "SCHEDULER" in w:
            return "CPU scheduler pressure"
        if w.startswith("LCK_") or "LOCK" in w:
            return "Locking / blocking"
        if "WRITELOG" in w or w == "LOG":
            return "Transaction log"
        return "Other"

    df["_cat"] = df["wait_type"].astype(str).map(cat)
    cat_sum = df.groupby("_cat")[pct_col].sum().sort_values(ascending=False)

    out["categories"] = [{"category": k, "wait_pct": float(v)} for k, v in cat_sum.head(8).items()]
    if out["categories"]:
        out["dominant_category"] = out["categories"][0]["category"]
        out["dominant_category_pct"] = out["categories"][0]["wait_pct"]

    top = df.sort_values(pct_col, ascending=False).head(10)
    out["top_waits"] = [
        {"wait_type": str(r["wait_type"]), "wait_pct": None if pd.isna(r[pct_col]) else float(r[pct_col])}
        for _, r in top.iterrows()
    ]
    return out


def _report_ready_hotspots(workload: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows = workload.get("high_impact_hotspots") if isinstance(workload, dict) else None
    if not isinstance(rows, list) or not rows:
        return []

    def bucket_label(b: str) -> str:
        mapping = {
            "top_worker_time": "Top CPU (worker time)",
            "top_logical_reads": "Top logical reads",
            "top_elapsed": "Top elapsed time",
            "most_expensive": "Most expensive queries",
        }
        return mapping.get(b, b or "hotspot")

    out: List[Dict[str, Any]] = []
    for r in rows:
        if not isinstance(r, dict):
            continue
        obj = r.get("object_name")
        obj_s = str(obj) if obj is not None else ""
        obj_s = obj_s.replace("\n", " ").strip()
        if len(obj_s) > 120:
            obj_s = obj_s[:117] + "…"

        out.append(
            {
                "bucket": bucket_label(str(r.get("bucket") or "")),
                "object_name": obj_s or None,
                "database_name": r.get("database_name"),
                "metric_name": r.get("metric_name"),
                "metric_value": r.get("metric_value"),
                "source_sheet": r.get("source_sheet"),
            }
        )
    return _dedupe_preserve_order(out)


def _summarize_database_distribution(server_name: str, available: List[str], snapshot: Optional[str] = None) -> Dict[str, Any]:
    out = {
        "total_databases": None,
        "system_databases": None,
        "user_databases": None,
    }
    sheet = resolve_sheet_name(available, [r"database\s+properties"])
    if not sheet:
        return out

    if not snapshot:
        return out

    df = _fetch_sheet(server_name, snapshot, sheet)
    if df.empty:
        return out

    db_col = _pick_column(list(df.columns), ["Database Name", "database_name", "Database", "name"])
    if not db_col:
        return out

    names = df[db_col].dropna().astype(str).str.strip()
    if names.empty:
        return out

    sys_set = {"master", "model", "msdb", "tempdb"}
    out["total_databases"] = int(names.nunique())
    out["system_databases"] = int(names.str.lower().isin(sys_set).sum())
    out["user_databases"] = int((~names.str.lower().isin(sys_set)).sum())
    return out


def _build_operational_health(profile: Dict[str, Any]) -> Dict[str, Any]:
    cfg = profile.get("configuration") or {}
    dbs = profile.get("database_settings") or {}
    pressure = profile.get("pressure") or {}
    util = profile.get("utilization") or {}
    waits = profile.get("wait_summary") or {}

    min_ple = pressure.get("min_ple") or util.get("cache_ple_seconds")
    max_cpu = util.get("max_cpu_pct")
    max_mem = util.get("max_memory_pct")

    return {
        "backup_checksum_default": cfg.get("backup_checksum_default"),
        "page_verify_user_db_none_count": dbs.get("user_db_none_count"),
        "memory_grants_pending": pressure.get("memory_grants_pending"),
        "min_ple": min_ple,
        "max_cpu_pct": max_cpu,
        "max_memory_pct": max_mem,
        "dominant_wait_category": waits.get("dominant_category"),
        "dominant_wait_category_pct": waits.get("dominant_category_pct"),
        "cpu_pressure_flag": bool(_safe_float(max_cpu) is not None and float(max_cpu) >= 80.0),
        "memory_pressure_flag": bool(_safe_float(max_mem) is not None and float(max_mem) >= 85.0),
        "low_ple_flag": bool(_safe_float(min_ple) is not None and float(min_ple) < 300.0),
    }


# ----------------------------
# Public API
# ----------------------------
def build_server_profile(server_name: str, ingestion_date: str | None = None) -> Dict[str, Any]:
    """
    App-facing and report-facing contract.

    Existing tab-safe keys are preserved.
    Additional report-friendly aliases are added to support generation of a
    SQL Server Health Assessment report that matches the target sample.
    """
    profile: Dict[str, Any] = {
        "server": server_name,
        "server_name": server_name,  # explicit alias for report layer
        "snapshot": None,
        "instance": {},
        "utilization": {
            "max_cpu_pct": None,
            "max_memory_pct": None,
            "cache_ple_seconds": None,
        },
        "pressure": {
            "min_ple": None,
            "memory_grants_pending": None,
        },
        "configuration": {
            "maxdop": None,
            "cost_threshold": None,
            "max_server_memory_mb": None,
            "optimize_for_adhoc": None,
            "backup_compression_default": None,
            "backup_checksum_default": None,
            "remote_admin_connections": None,
        },
        "workload": {},
        "io_stats": {},
        "waits_df": None,  # UI compatibility
        "top_waits": [],
        "wait_summary": {},
        "backup_summary": {},
        "tempdb": {},
        "query_hotspots": [],
        "database_distribution": {},
        "operational_health": {},
        "missing_indexes": {},
        "database_settings": {
            "system_dbs_page_verify": None,
            "user_dbs_page_verify": None,
            "user_dbs_with_page_verify_none": [],
            "system_db_values": [],
            "user_db_values": [],
            "system_db_none_count": 0,
            "user_db_none_count": 0,
            "user_db_checksum_count": 0,
        },
        "evidence": {
            "source_sheets": {},
        },
        "report_evidence": {
            "backup": {},
            "tempdb": {},
            "io_details": {},
            "waits_summary": {},
            "hotspots_report_rows": [],
        },
        "notes": [],
    }

    # ---------------------------------------------------
    # Resolve snapshot based on ingestion_date if provided
    # ---------------------------------------------------
    if ingestion_date:
        q = f"""
        SELECT CAST(snapshot_date AS string) AS snapshot_date
        FROM ent_log_analytics.observability.sql_diagnostics_files_delta
        WHERE server_name = '{_sql_quote(server_name)}'
        AND CAST(ingestion_date AS string) = '{_sql_quote(ingestion_date)}'
        LIMIT 1
        """
        df_snap = run_query(q)

        snapshot = None
        if not df_snap.empty and "snapshot_date" in df_snap.columns:
            snapshot = str(df_snap["snapshot_date"].iloc[0])
    else:
        snapshot = _get_latest_snapshot(server_name)

    profile["snapshot"] = snapshot

    if not snapshot:
        profile["notes"].append("No snapshot found for selected ingestion.")
        return profile

    available = list_available_sheets_any(server_name)
    if not available:
        profile["notes"].append("No bronze sheets were found for the selected server.")
        return profile

    server_props_sheet = resolve_sheet_name(available, [r"server\s+properties"])
    hardware_sheet = resolve_sheet_name(available, [r"hardware\s+info"])
    host_sheet = resolve_sheet_name(available, [r"host\s+info"])
    version_sheet = resolve_sheet_name(available, [r"version\s+info"])
    config_sheet = resolve_sheet_name(available, [r"configuration\s+values", r"\bconfig\b"])
    system_memory_sheet = resolve_sheet_name(available, [r"system\s+memory"])
    process_memory_sheet = resolve_sheet_name(available, [r"process\s+memory"])
    cpu_history_sheet = resolve_sheet_name(available, [r"cpu\s+utilization\s+history"])
    ple_sheet = resolve_sheet_name(available, [r"\bPLE\b", r"page\s+life\s+expectancy"])
    drive_latency_sheet = resolve_sheet_name(available, [r"drive\s+level\s+latency"])
    io_file_latency_sheet = resolve_sheet_name(available, [r"io\s+latency\s+by\s+file"])
    io_db_usage_sheet = resolve_sheet_name(available, [r"io\s+usage\s+by\s+database"])

    sheet_refs = {
        "server_properties": server_props_sheet,
        "hardware": hardware_sheet,
        "host": host_sheet,
        "version": version_sheet,
        "configuration": config_sheet,
        "system_memory": system_memory_sheet,
        "process_memory": process_memory_sheet,
        "cpu_history": cpu_history_sheet,
        "ple": ple_sheet,
        "drive_latency": drive_latency_sheet,
        "io_file_latency": io_file_latency_sheet,
        "io_db_usage": io_db_usage_sheet,
    }
    profile["evidence"]["source_sheets"] = {k: v for k, v in sheet_refs.items() if v}

    df_server = _fetch_sheet(server_name, snapshot, server_props_sheet) if server_props_sheet else pd.DataFrame()
    df_hw = _fetch_sheet(server_name, snapshot, hardware_sheet) if hardware_sheet else pd.DataFrame()
    df_host = _fetch_sheet(server_name, snapshot, host_sheet) if host_sheet else pd.DataFrame()
    df_version = _fetch_sheet(server_name, snapshot, version_sheet) if version_sheet else pd.DataFrame()
    df_conf = _fetch_sheet(server_name, snapshot, config_sheet) if config_sheet else pd.DataFrame()
    df_sys_mem = _fetch_sheet(server_name, snapshot, system_memory_sheet) if system_memory_sheet else pd.DataFrame()
    df_proc_mem = _fetch_sheet(server_name, snapshot, process_memory_sheet) if process_memory_sheet else pd.DataFrame()
    df_cpu = _fetch_sheet(server_name, snapshot, cpu_history_sheet) if cpu_history_sheet else pd.DataFrame()
    df_ple = _fetch_sheet(server_name, snapshot, ple_sheet) if ple_sheet else pd.DataFrame()
    df_drive = _fetch_sheet(server_name, snapshot, drive_latency_sheet) if drive_latency_sheet else pd.DataFrame()
    df_io_file = _fetch_sheet(server_name, snapshot, io_file_latency_sheet) if io_file_latency_sheet else pd.DataFrame()
    df_io_db = _fetch_sheet(server_name, snapshot, io_db_usage_sheet) if io_db_usage_sheet else pd.DataFrame()

    profile["instance"] = _extract_instance_info(df_server, df_hw, df_host, df_version)

    profile["configuration"].update(_extract_configuration_values(df_conf))
    for k in ["optimize_for_adhoc", "backup_compression_default", "backup_checksum_default", "remote_admin_connections"]:
        profile["configuration"][k] = _boolish_state(profile["configuration"].get(k))

    cpu_max = _extract_cpu_max(df_cpu)
    mem_pct = _extract_memory_pct(df_proc_mem, df_sys_mem)
    ple_sec = _extract_ple(df_ple)

    profile["utilization"]["max_cpu_pct"] = cpu_max
    profile["utilization"]["max_memory_pct"] = mem_pct
    profile["utilization"]["cache_ple_seconds"] = ple_sec
    profile["pressure"]["min_ple"] = ple_sec

    # memory grants pending
    try:
        q = f"""
        SELECT DISTINCT sheet_name
        FROM ent_log_analytics.observability.sql_diagnostics_bronze
        WHERE server_name = '{_sql_quote(server_name)}'
          AND CAST(snapshot_date AS string) = '{_sql_quote(snapshot)}'
          AND lower(sheet_name) LIKE '%memory grant%'
        """
        df_sheets = run_query(q)
        mg_pending_val = None
        if not df_sheets.empty and "sheet_name" in df_sheets.columns:
            mg_sheet = df_sheets["sheet_name"].iloc[0]
            profile["evidence"]["source_sheets"]["memory_grants"] = mg_sheet
            df_mg = _fetch_sheet(server_name, snapshot, mg_sheet)
            col = _pick_column(list(df_mg.columns), ["Memory Grants Pending", "memory_grants_pending", "memory_grants_pending_count", "grants_pending"])
            if col:
                vals = _num_series(df_mg, col).dropna()
                if not vals.empty:
                    mg_pending_val = int(vals.max())
        profile["pressure"]["memory_grants_pending"] = mg_pending_val
    except Exception:
        profile["pressure"]["memory_grants_pending"] = None

    profile["workload"] = _extract_workload_summary(server_name, available, snapshot=snapshot)
    profile["io_stats"] = _extract_io_stats(df_drive, df_io_file, df_io_db)
    profile["waits_df"] = _extract_waits(server_name, available_sheets=available, snapshot=snapshot)
    profile["database_settings"] = _extract_page_verify_summary(server_name, available, snapshot=snapshot)

    if profile["instance"].get("edition") and profile["instance"].get("sql_banner"):
        profile["instance"]["sql_and_edition"] = f"{profile['instance']['sql_banner']} {profile['instance']['edition']}"

    try:
        profile["report_evidence"]["backup"] = _extract_last_backup_summary(server_name, available, snapshot=snapshot)
        profile["backup_summary"] = profile["report_evidence"]["backup"]
        if profile["report_evidence"]["backup"].get("sheet"):
            profile["evidence"]["source_sheets"]["last_backup_by_db"] = profile["report_evidence"]["backup"]["sheet"]
    except Exception:
        profile["report_evidence"]["backup"] = {}
        profile["backup_summary"] = {}

    try:
        profile["report_evidence"]["tempdb"] = _extract_tempdb_summary(server_name, available, snapshot=snapshot)
        profile["tempdb"] = profile["report_evidence"]["tempdb"]
        if profile["report_evidence"]["tempdb"].get("files_sheet"):
            profile["evidence"]["source_sheets"]["tempdb_files"] = profile["report_evidence"]["tempdb"]["files_sheet"]
        if profile["report_evidence"]["tempdb"].get("sizes_sheet"):
            profile["evidence"]["source_sheets"]["tempdb_sizes"] = profile["report_evidence"]["tempdb"]["sizes_sheet"]
    except Exception:
        profile["report_evidence"]["tempdb"] = {}
        profile["tempdb"] = {}

    try:
        profile["report_evidence"]["io_details"] = _extract_io_details(df_drive, df_io_file, df_io_db)
    except Exception:
        profile["report_evidence"]["io_details"] = {}

    try:
        profile["report_evidence"]["waits_summary"] = _summarize_waits_for_report(profile.get("waits_df"))
        profile["wait_summary"] = profile["report_evidence"]["waits_summary"]
        profile["top_waits"] = profile["report_evidence"]["waits_summary"].get("top_waits", [])
    except Exception:
        profile["report_evidence"]["waits_summary"] = {}
        profile["wait_summary"] = {}
        profile["top_waits"] = []

    try:
        profile["report_evidence"]["hotspots_report_rows"] = _report_ready_hotspots(profile.get("workload") or {})
        profile["query_hotspots"] = profile["report_evidence"]["hotspots_report_rows"]
    except Exception:
        profile["report_evidence"]["hotspots_report_rows"] = []
        profile["query_hotspots"] = []

    try:
        profile["database_distribution"] = _summarize_database_distribution(server_name, available, snapshot=snapshot)
    except Exception:
        profile["database_distribution"] = {}

    try:
        profile["operational_health"] = _build_operational_health(profile)
    except Exception:
        profile["operational_health"] = {}

    # report-friendly aliases / convenience fields
    profile["instance"]["server_name"] = server_name
    profile["utilization"]["ple_sec"] = profile["pressure"].get("min_ple") or profile["utilization"].get("cache_ple_seconds")
    profile["report_evidence"]["snapshot"] = snapshot
    profile["report_evidence"]["server_name"] = server_name

    notes: List[str] = []
    if cpu_max is None:
        notes.append("CPU metric could not be resolved from the available CPU history sheet.")
    if mem_pct is None:
        notes.append("Memory utilization percent could not be computed from process/system memory sheets.")
    if ple_sec is None:
        notes.append("Page Life Expectancy was not found; cache health evidence is incomplete.")
    if not isinstance(profile.get("waits_df"), pd.DataFrame) or profile["waits_df"].empty:
        notes.append("Top wait statistics were not available from the ingested waits sheet.")
    if not (profile.get("workload") or {}).get("high_impact_hotspots"):
        notes.append("High-impact workload hotspot sheets were not resolved from the latest snapshot.")
    if profile["database_settings"].get("user_dbs_with_page_verify_none"):
        dbs = ", ".join(profile["database_settings"]["user_dbs_with_page_verify_none"][:5])
        notes.append(f"Detected user databases with PAGE_VERIFY = NONE: {dbs}.")
    if profile["configuration"].get("backup_checksum_default") == "Disabled":
        notes.append("Backup checksum default is disabled.")
    if profile["pressure"].get("memory_grants_pending") not in (None, 0):
        notes.append(f"Memory grants pending observed: {profile['pressure']['memory_grants_pending']}.")

    profile["notes"] = notes
    return profile