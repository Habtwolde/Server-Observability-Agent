from __future__ import annotations

import streamlit as st

from db.connection import DatabricksQueryError
from services.agent_findings_service import (
    clear_agent_data_caches,
    load_latest_run,
    load_server_options,
)

from ui.agent_assistant_panel import render_agent_assistant_panel
from ui.agent_styles import apply_agent_styles
from ui.alert_subscriptions_view import render_alert_subscriptions_view
from ui.fleet_priority_view import render_fleet_priority_view
from ui.pipeline_status_view import render_pipeline_status_view
from ui.server_diagnosis_view import render_server_diagnosis_view

st.set_page_config(
    page_title="SQL Server Observability Agent",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="expanded",
)
apply_agent_styles()

st.markdown(
    """
    <div class="agent-header">
      <div class="agent-title">SQL Server Observability Agent</div>
      <div class="agent-subtitle">Priority DBA findings grounded in the latest SQL diagnostics and Windows Events evidence.</div>
    </div>
    """,
    unsafe_allow_html=True,
)

with st.sidebar:
    st.header("Agent scope")
    if st.button("Refresh Agent data", use_container_width=True):
        clear_agent_data_caches()
        st.rerun()

try:
    server_options = load_server_options()
    latest_run_df = load_latest_run()

    with st.sidebar:
        selected_scope = st.selectbox(
            "Server",
            options=["All servers", *server_options],
            index=0,
        )
        selected_server = None if selected_scope == "All servers" else selected_scope
        top_n = st.radio(
            "Priority issues per server",
            options=[3, 5],
            index=1,
            horizontal=True,
        )

        if not latest_run_df.empty:
            latest_run = latest_run_df.iloc[0]
            st.divider()
            st.caption("Latest pipeline run")
            st.code(str(latest_run.get("run_id", "Unknown")), language=None)
            st.caption(str(latest_run.get("run_status", "Unknown")))

    render_agent_assistant_panel(selected_server, top_n=top_n)

    fleet_tab, diagnosis_tab, pipeline_tab, subscriptions_tab = st.tabs(
        [
            "Fleet priorities",
            "Server diagnosis",
            "Pipeline status",
            "Notification subscriptions",
        ]
    )

    with fleet_tab:
        render_fleet_priority_view(top_n=top_n)

    with diagnosis_tab:
        render_server_diagnosis_view(
            selected_server,
            top_n=top_n,
        )

    with pipeline_tab:
        render_pipeline_status_view()

    with subscriptions_tab:
        render_alert_subscriptions_view()



except DatabricksQueryError as exc:
    st.error(f"The Agent could not query Databricks: {exc}")
except Exception as exc:
    st.error(f"The Agent encountered an unexpected error: {exc}")
