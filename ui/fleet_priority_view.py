"""Fleet-wide priority view."""
from __future__ import annotations

import pandas as pd
import streamlit as st

from services.agent_findings_service import (
    load_finding_domain_summary,
    load_fleet_health,
    load_top_findings,
)


def _number(value: object) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return 0


def render_fleet_priority_view(top_n: int = 5) -> None:
    health_df = load_fleet_health()
    if health_df.empty:
        st.info("No Agent health summary is available yet. Run notebooks 01–05.")
        return

    status = health_df.get("health_status", pd.Series(dtype=str)).astype(str)
    critical_servers = int((status == "CRITICAL").sum())
    attention_servers = int((status == "ATTENTION").sum())
    incomplete_servers = int((status == "DATA_INCOMPLETE").sum())
    critical_issues = sum(_number(v) for v in health_df.get("critical_issue_count", []))

    cols = st.columns(5)
    cols[0].metric("Servers assessed", len(health_df))
    cols[1].metric("Critical servers", critical_servers)
    cols[2].metric("Attention required", attention_servers)
    cols[3].metric("Data incomplete", incomplete_servers)
    cols[4].metric("Critical findings", critical_issues)

    if incomplete_servers:
        st.warning(
            f"{incomplete_servers} server(s) have incomplete evidence. Their health score is intentionally withheld."
        )

    st.subheader("Fleet health")
    fleet_columns = [
        "canonical_server_name",
        "health_status",
        "health_score",
        "critical_issue_count",
        "high_issue_count",
        "medium_issue_count",
        "data_quality_blocker_count",
        "total_actionable_issue_count",
        "snapshot_date",
    ]
    st.dataframe(
        health_df[[column for column in fleet_columns if column in health_df.columns]],
        hide_index=True,
        use_container_width=True,
    )

    st.subheader(f"Top {top_n} findings per server")
    findings_df = load_top_findings(None, top_n=top_n)
    if findings_df.empty:
        st.success("No open priority findings were returned.")
    else:
        finding_columns = [
            "canonical_server_name",
            "finding_rank",
            "severity",
            "domain",
            "rule_title",
            "entity_name",
            "finding_summary",
            "recommended_action",
        ]
        st.dataframe(
            findings_df[[column for column in finding_columns if column in findings_df.columns]],
            hide_index=True,
            use_container_width=True,
        )

    with st.expander("Finding distribution by domain"):
        domain_df = load_finding_domain_summary()
        if domain_df.empty:
            st.caption("No current finding distribution is available.")
        else:
            st.dataframe(domain_df, hide_index=True, use_container_width=True)

