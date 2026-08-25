"""Focused server diagnosis and AI briefing view."""
from __future__ import annotations

import json
from html import escape
from typing import Any

import pandas as pd
import streamlit as st

from services.agent_ai_service import generate_server_briefing
from services.agent_findings_service import load_server_health, load_top_findings


def _value(row: pd.Series, name: str, default: Any = "") -> Any:
    value = row.get(name, default)
    try:
        if pd.isna(value):
            return default
    except (TypeError, ValueError):
        pass
    return value


def _as_int(value: Any) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return 0


def _render_finding(row: pd.Series) -> None:
    severity = str(_value(row, "severity", "UNKNOWN")).upper()
    css_severity = severity.lower() if severity in {"CRITICAL", "HIGH", "MEDIUM", "LOW"} else "low"
    title = escape(str(_value(row, "rule_title", "Priority finding")))
    summary = escape(str(_value(row, "finding_summary", "Evidence not supplied.")))
    cause = escape(str(_value(row, "likely_cause", "Cause not established.")))
    action = escape(str(_value(row, "recommended_action", "Review with the DBA team.")))
    domain = escape(str(_value(row, "domain", "GENERAL")))
    entity = escape(str(_value(row, "entity_name", "Server")))
    rule_id = escape(str(_value(row, "rule_id", "")))

    st.markdown(
        f"""
        <div class="finding-card finding-{css_severity}">
          <div class="finding-label">{escape(severity)} · {domain} · {rule_id}</div>
          <div class="finding-title">{title}</div>
          <div class="finding-meta">Affected entity: {entity}</div>
          <div class="finding-section"><strong>Issue:</strong> {summary}</div>
          <div class="finding-section"><strong>Likely cause:</strong> {cause}</div>
          <div class="finding-section"><strong>Immediate action:</strong> {action}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    reference = str(_value(row, "microsoft_reference_url", "")).strip()
    threshold = str(_value(row, "threshold_note", "")).strip()
    with st.expander("Evidence and Microsoft reference"):
        evidence = _value(row, "evidence_json", "")
        if evidence:
            try:
                st.json(json.loads(str(evidence)))
            except json.JSONDecodeError:
                st.code(str(evidence))
        else:
            st.caption("No structured evidence was supplied for this finding.")
        if threshold:
            st.caption(f"Threshold note: {threshold}")
        if reference.startswith(("https://", "http://")):
            st.markdown(f"[Open Microsoft guidance]({reference})")


def render_server_diagnosis_view(server_name: str | None, top_n: int = 5) -> None:
    if not server_name:
        st.info("Select a server in the sidebar to open its diagnosis.")
        return

    server = server_name.strip().upper()
    health_df = load_server_health(server)
    findings_df = load_top_findings(server, top_n=top_n)

    st.markdown(
        f'<div class="scope-banner"><strong>Selected server:</strong> {escape(server)}</div>',
        unsafe_allow_html=True,
    )

    if health_df.empty:
        st.warning("No health summary is available for this server.")
        return

    health = health_df.iloc[0]
    health_status = str(_value(health, "health_status", "UNKNOWN"))
    health_score = _value(health, "health_score", None)
    score_text = "Withheld" if health_score in (None, "") else f"{float(health_score):.0f}/100"

    cols = st.columns(5)
    cols[0].metric("Health status", health_status)
    cols[1].metric("Health score", score_text)
    cols[2].metric("Critical", _as_int(_value(health, "critical_issue_count", 0)))
    cols[3].metric("High", _as_int(_value(health, "high_issue_count", 0)))
    cols[4].metric("Data blockers", _as_int(_value(health, "data_quality_blocker_count", 0)))

    context = str(_value(health, "summary_context", ""))
    if context.startswith("HISTORICAL_SAMPLE_TEST") or health_status == "DATA_INCOMPLETE":
        st.warning(
            "This diagnosis contains test, historical, or incomplete evidence. Do not treat it as a production health conclusion."
        )

    st.subheader(f"Top {top_n} issues requiring attention")
    if findings_df.empty:
        st.success("No open priority findings were returned for this server.")
    else:
        for _, finding in findings_df.iterrows():
            _render_finding(finding)

    briefing_key = f"agent_briefing::{server}::{top_n}"
    if st.button("Generate AI DBA briefing", key=f"generate::{briefing_key}", type="primary"):
        with st.spinner("Generating a grounded briefing from the findings…"):
            st.session_state[briefing_key] = generate_server_briefing(server, top_n=top_n)

    if st.session_state.get(briefing_key):
        st.subheader("AI DBA briefing")
        st.markdown(st.session_state[briefing_key])

