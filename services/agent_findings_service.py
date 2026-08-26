"""Read-only data service for Agent health, findings, and pipeline status."""
from __future__ import annotations

from typing import Any

import pandas as pd
import streamlit as st

from db.connection import clear_query_cache, run_query
from db.observability_sources import (
    CURRENT_FINDINGS_VIEW,
    HEALTH_SUMMARY_VIEW,
    RUNS_TABLE,
    TOP_FINDINGS_VIEW,
)


def _sql_quote(value: Any) -> str:
    return str(value).replace("'", "''")


def _safe_limit(value: int, minimum: int = 1, maximum: int = 200) -> int:
    return max(minimum, min(int(value), maximum))


def severity_rank(value: Any) -> int:
    return {
        "CRITICAL": 4,
        "HIGH": 3,
        "MEDIUM": 2,
        "LOW": 1,
    }.get(str(value or "").upper(), 0)


@st.cache_data(ttl=60, show_spinner=False)
def load_latest_run() -> pd.DataFrame:
    return run_query(
        f"""
        SELECT
            run_id,
            CAST(run_date AS STRING) AS run_date,
            source_timezone,
            run_status,
            run_trigger,
            expected_server_count,
            discovered_workbook_count,
            identified_server_count,
            valid_workbook_count,
            invalid_workbook_count,
            windows_event_file_count,
            missing_servers,
            duplicate_servers,
            unexpected_servers,
            started_ts,
            validation_completed_ts,
            processing_completed_ts,
            error_message,
            updated_ts
        FROM {RUNS_TABLE}
        ORDER BY run_date DESC, updated_ts DESC
        LIMIT 1
        """
    )


@st.cache_data(ttl=60, show_spinner=False)
def load_run_history(limit: int = 20) -> pd.DataFrame:
    safe_limit = _safe_limit(limit, 1, 100)
    return run_query(
        f"""
        SELECT
            run_id,
            CAST(run_date AS STRING) AS run_date,
            run_status,
            run_trigger,
            expected_server_count,
            discovered_workbook_count,
            valid_workbook_count,
            invalid_workbook_count,
            windows_event_file_count,
            processing_completed_ts,
            error_message
        FROM {RUNS_TABLE}
        ORDER BY run_date DESC, updated_ts DESC
        LIMIT {safe_limit}
        """
    )


@st.cache_data(ttl=60, show_spinner=False)
def load_fleet_health() -> pd.DataFrame:
    return run_query(
        f"""
        SELECT
            run_id,
            CAST(snapshot_date AS STRING) AS snapshot_date,
            canonical_server_name,
            health_status,
            health_score,
            critical_issue_count,
            high_issue_count,
            medium_issue_count,
            low_issue_count,
            data_quality_blocker_count,
            total_actionable_issue_count,
            latest_observed_ts,
            summary_context,
            evaluated_ts
        FROM {HEALTH_SUMMARY_VIEW}
        ORDER BY
            CASE health_status
                WHEN 'DATA_INCOMPLETE' THEN 1
                WHEN 'CRITICAL' THEN 2
                WHEN 'ATTENTION' THEN 3
                WHEN 'WATCH' THEN 4
                ELSE 5
            END,
            critical_issue_count DESC,
            high_issue_count DESC,
            canonical_server_name
        """
    )


@st.cache_data(ttl=60, show_spinner=False)
def load_server_options() -> list[str]:
    df = run_query(
        f"""
        SELECT DISTINCT canonical_server_name
        FROM {HEALTH_SUMMARY_VIEW}
        WHERE canonical_server_name IS NOT NULL
        ORDER BY canonical_server_name
        """
    )
    if df.empty or "canonical_server_name" not in df.columns:
        return []
    return df["canonical_server_name"].dropna().astype(str).tolist()


@st.cache_data(ttl=60, show_spinner=False)
def load_top_findings(server_name: str | None = None, top_n: int = 5) -> pd.DataFrame:
    safe_top_n = _safe_limit(top_n, 1, 5)
    server_filter = ""
    if server_name:
        server_filter = (
            "AND canonical_server_name = "
            f"'{_sql_quote(server_name.strip().upper())}'"
        )

    return run_query(
        f"""
        SELECT
            finding_id,
            run_id,
            CAST(snapshot_date AS STRING) AS snapshot_date,
            canonical_server_name,
            finding_rank,
            rule_id,
            rule_version,
            domain,
            severity,
            priority_score,
            rule_title,
            entity_type,
            entity_name,
            finding_summary,
            likely_cause,
            evidence_json,
            recommended_action,
            microsoft_reference_url,
            threshold_note,
            source_observed_ts,
            finding_context
        FROM {TOP_FINDINGS_VIEW}
        WHERE finding_rank <= {safe_top_n}
          {server_filter}
        ORDER BY
            CASE severity
                WHEN 'CRITICAL' THEN 4
                WHEN 'HIGH' THEN 3
                WHEN 'MEDIUM' THEN 2
                ELSE 1
            END DESC,
            priority_score DESC,
            canonical_server_name,
            finding_rank
        """
    )


@st.cache_data(ttl=60, show_spinner=False)
def load_server_health(server_name: str) -> pd.DataFrame:
    safe_server = _sql_quote(server_name.strip().upper())
    return run_query(
        f"""
        SELECT
            run_id,
            CAST(snapshot_date AS STRING) AS snapshot_date,
            canonical_server_name,
            health_status,
            health_score,
            critical_issue_count,
            high_issue_count,
            medium_issue_count,
            low_issue_count,
            data_quality_blocker_count,
            total_actionable_issue_count,
            latest_observed_ts,
            summary_context,
            evaluated_ts
        FROM {HEALTH_SUMMARY_VIEW}
        WHERE canonical_server_name = '{safe_server}'
        LIMIT 1
        """
    )


@st.cache_data(ttl=60, show_spinner=False)
def load_finding_domain_summary() -> pd.DataFrame:
    return run_query(
        f"""
        SELECT
            domain,
            severity,
            COUNT(*) AS finding_count,
            COUNT(DISTINCT canonical_server_name) AS affected_server_count
        FROM {CURRENT_FINDINGS_VIEW}
        GROUP BY domain, severity
        ORDER BY
            CASE severity
                WHEN 'CRITICAL' THEN 4
                WHEN 'HIGH' THEN 3
                WHEN 'MEDIUM' THEN 2
                ELSE 1
            END DESC,
            finding_count DESC,
            domain
        """
    )


def clear_agent_data_caches() -> None:
    clear_query_cache()
    for cached_function in (
        load_latest_run,
        load_run_history,
        load_fleet_health,
        load_server_options,
        load_top_findings,
        load_server_health,
        load_finding_domain_summary,
    ):
        cached_function.clear()
