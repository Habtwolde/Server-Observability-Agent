"""Daily pipeline status view."""
from __future__ import annotations

import pandas as pd
import streamlit as st

from services.agent_findings_service import (
    load_latest_notification,
    load_latest_run,
    load_run_history,
)


def _value(row: pd.Series, name: str, default: object = "—") -> object:
    value = row.get(name, default)
    try:
        return default if pd.isna(value) else value
    except (TypeError, ValueError):
        return value


def render_pipeline_status_view() -> None:
    latest_df = load_latest_run()
    if latest_df.empty:
        st.info("No Agent pipeline run has been registered.")
        return

    latest = latest_df.iloc[0]
    cols = st.columns(5)
    cols[0].metric("Run date", _value(latest, "run_date"))
    cols[1].metric("Status", _value(latest, "run_status"))
    cols[2].metric("Expected workbooks", _value(latest, "expected_server_count", 0))
    cols[3].metric("Valid workbooks", _value(latest, "valid_workbook_count", 0))
    cols[4].metric("Windows files", _value(latest, "windows_event_file_count", 0))

    status = str(_value(latest, "run_status", "UNKNOWN"))
    if status.startswith("TEST_"):
        st.warning("The latest run is a controlled test run, not a production daily run.")
    elif status == "HEALTH_RULES_EVALUATED":
        st.success("The latest production run completed through health-rule evaluation.")
    else:
        st.warning(f"The latest pipeline status is {status}.")

    error_message = str(_value(latest, "error_message", "")).strip()
    if error_message and error_message != "—":
        st.error(error_message)

    st.subheader("Latest priority email")
    try:
        notification_df = load_latest_notification()
    except Exception:
        notification_df = pd.DataFrame()

    if notification_df.empty:
        st.info("No priority email delivery has been recorded yet.")
    else:
        notification = notification_df.iloc[0]
        email_cols = st.columns(4)
        email_cols[0].metric("Delivery", _value(notification, "delivery_status"))
        email_cols[1].metric("Recipient", _value(notification, "recipient"))
        email_cols[2].metric("Affected servers", _value(notification, "affected_server_count", 0))
        email_cols[3].metric("Priority issues", _value(notification, "issue_count", 0))

        delivery_status = str(_value(notification, "delivery_status", "UNKNOWN"))
        if delivery_status == "SENT":
            st.success(f"Priority briefing sent at {_value(notification, 'sent_ts')}.")
        elif delivery_status.startswith("SKIPPED_"):
            st.info(f"Email delivery status: {delivery_status}.")
        elif delivery_status == "FAILED":
            st.error(str(_value(notification, "error_message", "Email delivery failed.")))
        else:
            st.warning(f"Email delivery status: {delivery_status}.")

    st.subheader("Recent pipeline runs")
    history_df = load_run_history(20)
    st.dataframe(history_df, hide_index=True, use_container_width=True)
