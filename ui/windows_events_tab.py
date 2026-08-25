# ui/windows_events_tab.py
from __future__ import annotations

import streamlit as st
from services import windows_events_service as we_service


def _build_summary_context(summary: dict) -> str:
    """
    Compatibility wrapper:
    - Uses service helper when available.
    - Falls back to a local formatter if older service code is deployed.
    """
    service_fn = getattr(we_service, "build_summary_context", None)
    if callable(service_fn):
        return str(service_fn(summary))

    total = summary.get("alerts_total", 0)
    err = summary.get("alerts_error", 0)
    warn = summary.get("alerts_warning", 0)
    info = summary.get("alerts_info", 0)
    return (
        f"Windows Events found: {total}. "
        f"Errors: {err}. Warnings: {warn}. Informational: {info}. "
        f"Source: uploaded Windows Events CSV."
    )


_OVERLAY_CSS = """<style>
.we-overlay {
  position: fixed;
  top: 0; left: 0;
  width: 100vw; height: 100vh;
  background: rgba(255,255,255,0.72);
  backdrop-filter: blur(2px);
  z-index: 9999;
  display: flex;
  align-items: center;
  justify-content: center;
}
.we-overlay-card {
  border: 1px solid rgba(0,0,0,0.10);
  border-radius: 16px;
  background: rgba(255,255,255,0.92);
  padding: 16px 18px;
  box-shadow: 0 10px 30px rgba(0,0,0,0.08);
  min-width: 300px;
}
.we-spinner {
  width: 54px;
  height: 54px;
  border: 6px solid rgba(0,0,0,0.10);
  border-top: 6px solid #1f6feb;
  border-radius: 50%;
  animation: we-spin 0.9s linear infinite;
  margin: 0 auto 10px auto;
}

.we-spinner-text { font-weight: 650; opacity: 0.86; text-align: center; }
.we-spinner-sub { font-size: 0.86rem; opacity: 0.72; text-align: center; margin-top: 2px; }
.we-card {
  border: 1px solid rgba(15,23,42,0.08);
  border-left: 3px solid #3b82f6;
  border-radius: 0 8px 8px 0;
  padding: 10px 14px;
  background: linear-gradient(90deg, rgba(59,130,246,0.06) 0%, rgba(248,250,252,0.9) 100%);
  margin-top: 14px;
  margin-bottom: 10px;
  font-size: 0.94rem;
}
.we-actions [data-testid="stDownloadButton"] > button,
.we-actions [data-testid="stButton"] > button {
  border-radius: 6px !important;
  font-weight: 650 !important;
  min-height: 2.4rem !important;
  width: 100% !important;
}
@keyframes we-spin {
  0% { transform: rotate(0deg); }
  100% { transform: rotate(360deg); }
}
</style>"""

_AI_STATUS_CSS = """<style>
/* Spinner animation used by _render_llm_spinner */
.we-ai-icon {
  border-radius: 50%;
  display: inline-block;
  animation: we-ai-spin 0.9s linear infinite;
}
@keyframes we-ai-spin {
  0%   { transform: rotate(0deg); }
  100% { transform: rotate(360deg); }
}
</style>"""

def _render_llm_spinner() -> None:
    st.markdown(
        f"""
<div style="
  display:flex;
  align-items:center;
  gap:14px;
  padding:8px 2px 14px 2px;
  color:#374151;
  font-size:2.2rem;
  font-weight:500;
">
  <div class="we-ai-icon" style="width:42px;height:42px;border-width:6px;border-color:rgba(148,163,184,0.35);border-top-color:#3b82f6;border-right-color:#3b82f6;"></div>
</div>
        """,
        unsafe_allow_html=True,
    )

def render_windows_events_tab(server_name: str, ingestion_date: str | None = None) -> None:
    st.markdown(
        """
<div style="margin-bottom:6px;">
  <div style="font-size:1.55rem;font-weight:800;letter-spacing:-0.02em;color:#0f172a;margin:0 0 4px 0;">
    Windows &amp; Operational Events
  </div>
  <div style="font-size:0.96rem;color:#475569;line-height:1.5;">
    Event Viewer-style diagnostics from the Windows Events dataset, filtered to the active server.
  </div>
</div>
""",
        unsafe_allow_html=True,
    )

    # Always inject CSS — Streamlit re-renders fully on every rerun so
    # session_state guards would cause the styles to vanish after interactions.
    st.markdown(_OVERLAY_CSS, unsafe_allow_html=True)
    st.markdown(_AI_STATUS_CSS, unsafe_allow_html=True)

    with st.expander("Filters", expanded=True):
        col1, col2, col3 = st.columns([1.2, 1.0, 1.2])
        with col1:
            level = st.selectbox("Level", ["All", "Error", "Warning", "Information"], index=0)
        with col2:
            rows = st.selectbox("Rows", [25, 50, 100, 250], index=1)
        with col3:
            keyword = st.text_input("Search", placeholder="provider / message / event id ...")

    # Overlay placeholder lives AFTER the expander so it doesn't leave a blank
    # gap above the filters while the data loads.
    overlay = st.empty()

    def _show_overlay(title: str, subtitle: str = "Please wait…") -> None:
        overlay.markdown(
            f"""
<div class="we-overlay">
  <div class="we-overlay-card">
    <div class="we-spinner"></div>
    <div class="we-spinner-text">{title}</div>
    <div class="we-spinner-sub">{subtitle}</div>
  </div>
</div>
            """,
            unsafe_allow_html=True,
        )

    def _hide_overlay() -> None:
        overlay.empty()

    thresholds = we_service.EventThresholds()

    scope_note = f"ingestion date {ingestion_date}" if ingestion_date else "latest available scope"
    _show_overlay("Loading Windows Events", f"Querying Delta ({scope_note})…")
    try:
        events_df, summary = we_service.fetch_windows_events(
            server_name,
            thresholds,
            ingestion_date=ingestion_date,
        )
    finally:
        _hide_overlay()

    c1, c2, c3, _sep, c4, c5 = st.columns([1, 1, 1, 0.12, 1, 1])
    c1.metric("Alerts", summary.get("alerts_total", 0))
    c2.metric("Errors", summary.get("alerts_error", 0))
    c3.metric("Warnings", summary.get("alerts_warning", 0))

    cpu_max = summary.get("cpu_max", None)
    c4.metric("Max SQL CPU", "—" if cpu_max is None else f"{cpu_max:.1f}%")
    c5.metric("CPU Spikes", summary.get("cpu_spikes_warning", 0) + summary.get("cpu_spikes_critical", 0))

    if events_df.empty:
        st.info("No event-like records were found for this server in the latest snapshot.")
        st.markdown("#### Summary context")
        st.write(_build_summary_context(summary))
        return

    df = events_df.copy()

    if level != "All":
        normalized_level = level.lower()
        level_col = df.get("level")
        if level_col is not None:
            # "information" and "info" are the same level — match both spellings
            if normalized_level == "information":
                df = df[df["level"].astype(str).str.lower().isin(["information", "info"])]
            else:
                df = df[df["level"].astype(str).str.lower() == normalized_level]

    if keyword:
        k = keyword.strip().lower()
        mask = False
        for col in ("provider", "message", "id"):
            if col in df.columns:
                col_mask = df[col].astype(str).str.lower().str.contains(k, na=False)
                mask = col_mask if isinstance(mask, bool) else (mask | col_mask)

        if isinstance(mask, bool):
            st.warning("Search is unavailable because provider/message/id columns are missing in this dataset.")
        else:
            df = df[mask]

    if df.empty:
        st.warning("No matching events for the selected filters.")
        return

    st.markdown(
        f"""
<div class="we-card">
  <strong>Filtered results:</strong> {len(df):,} events shown for <code>{server_name}</code>.
</div>
""",
        unsafe_allow_html=True,
    )

    action_col, _ = st.columns([1.5, 3.5])
    with action_col:
        st.markdown('<div class="we-actions">', unsafe_allow_html=True)
        st.download_button(
            "Export filtered events (CSV)",
            data=df.to_csv(index=False).encode("utf-8"),
            file_name=f"windows_events_{server_name}.csv",
            mime="text/csv",
            use_container_width=True,        

        )
        st.markdown("</div>", unsafe_allow_html=True)

    st.markdown("### Events")
    show_cols = [
        "TimeCreated",
        "LevelDisplayName",
        "ProviderName",
        "ID",
        "LogName",
        "MachineName",
        "ContainerLog",
        "Message",
        "CreatedDate",
    ]
    show_cols = [c for c in show_cols if c in df.columns]
    if not show_cols:
        show_cols = ["time_created", "level", "provider", "id", "message"]
        show_cols = [c for c in show_cols if c in df.columns]

    st.dataframe(df[show_cols].head(int(rows)), use_container_width=True, hide_index=True)

    st.markdown("#### Summary context")
    st.write(_build_summary_context(summary))

    st.markdown("#### Event analysis")
    st.caption(
        "Uses all available event columns (Message, ID, ProviderName, LogName, "
        "MachineName, TimeCreated, ContainerLog, LevelDisplayName, CreatedDate) "
        "to summarize, flag future risks, and identify correlations."
    )

    analysis_key = f"we_ai_analysis::{server_name}"
    status_slot = st.empty()

    btn_col, _ = st.columns([1.5, 3.5], gap="small")
    with btn_col:
        analyze_clicked = st.button("Analyze events", key=f"we_ai_btn::{server_name}", use_container_width=True)

    if analyze_clicked:
        with status_slot:
            _render_llm_spinner()
        _show_overlay("Analyzing event patterns", "Reviewing all event columns and correlations…")
        try:
            analysis_md = we_service.analyze_windows_events_with_llm(
                server_name=server_name,
                events_df=df,
                summary=summary,
                filtered_view_count=len(df),
            )
            st.session_state[analysis_key] = analysis_md
        except Exception as e:
            st.session_state[analysis_key] = f"analysis failed: {e}"
        finally:
            _hide_overlay()
            status_slot.empty()

    analysis_md = st.session_state.get(analysis_key)
    if analysis_md:
        st.markdown(analysis_md)
    else:
        st.info("Click **Analyze events** to generate a detailed risk and correlation summary.")