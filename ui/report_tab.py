from __future__ import annotations

import html
import math
import streamlit as st

from services.metrics_service import build_server_profile
from services.report_service import (
    generate_report_section_narrative,
    get_report_filename,
    is_report_llm_enabled,
    merge_report_section_narratives,
    prepare_report_generation,
    render_report_docx_from_evidence,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _scope_key(selected_server: str, selected_ingestion_date: str | None) -> str:
    return f"{selected_server}::{selected_ingestion_date or 'latest'}"


def _html(value: object) -> str:
    """Escape dynamic values before injecting them into unsafe HTML blocks."""
    return html.escape(str(value), quote=True)


def _safe_pct(value: object) -> str:
    if not isinstance(value, (int, float)) or math.isnan(value) or math.isinf(value):
        return "—"
    value = max(0.0, min(float(value), 100.0))
    return f"{value:.1f}%"


def _safe_int_seconds(value: object) -> str:
    if not isinstance(value, (int, float)) or math.isnan(value) or math.isinf(value):
        return "—"
    return f"{int(value)}s"


def _reset_report_state_if_scope_changed(scope_key: str) -> None:
    previous_scope_key = st.session_state.get("_report_scope_key")
    if previous_scope_key != scope_key:
        st.session_state["_report_scope_key"] = scope_key
        st.session_state.pop("report_docx_bytes", None)
        st.session_state.pop("_report_docx_scope_key", None)
        st.session_state.pop("_report_error", None)
        st.session_state.pop("_report_generation_job", None)
        # Keep report output state scoped to the selected server/date snapshot.


def _load_profile(
    selected_server: str,
    selected_ingestion_date: str | None,
    show_overlay,
    hide_overlay,
):
    profile_cache = st.session_state.setdefault("_report_profile_cache", {})
    cache_key = _scope_key(selected_server, selected_ingestion_date)

    if cache_key in profile_cache:
        return profile_cache[cache_key]

    show_overlay("Loading server snapshot", "Querying Delta tables…")
    try:
        profile = build_server_profile(selected_server, selected_ingestion_date)
        profile_cache[cache_key] = profile
        return profile
    finally:
        hide_overlay()



def _report_job_overlay_text(job: dict) -> tuple[str, str]:
    sections = job.get("sections") or []
    total = len(sections)
    idx = min(int(job.get("current_index") or 0), total)
    if idx < total:
        section = sections[idx] or {}
        display_name = str(section.get("display_name") or section.get("key") or "next report section")
        return "Generating report", f"Section {idx + 1} of {total}: {display_name}"
    return "Generating report", "Finalizing DOCX package…"

def _safe_rerun() -> None:
    try:
        st.rerun()
    except AttributeError:
        st.experimental_rerun()


def _start_report_generation_job(
    selected_server: str,
    selected_ingestion_date: str | None,
    scope_key: str,
    show_overlay,
    hide_overlay,
) -> None:
    st.session_state.pop("_report_error", None)
    st.session_state.pop("report_docx_bytes", None)
    st.session_state.pop("_report_docx_scope_key", None)
    st.session_state.pop("_report_generation_job", None)

    show_overlay(
        "Preparing report",
        "Validating the selected ingestion and building report evidence…",
    )
    try:
        prepared = prepare_report_generation(selected_server, selected_ingestion_date or "")
        if not is_report_llm_enabled():
            docx_bytes = render_report_docx_from_evidence(
                prepared["style"],
                prepared["evidence"],
                prepared["baseline_narrative"],
            )
            st.session_state["report_docx_bytes"] = docx_bytes
            st.session_state["_report_docx_scope_key"] = scope_key
            return

        st.session_state["_report_generation_job"] = {
            "scope_key": scope_key,
            "status": "running",
            "style": prepared["style"],
            "evidence": prepared["evidence"],
            "baseline_narrative": prepared["baseline_narrative"],
            "sections": prepared["sections"],
            "section_results": {},
            "current_index": 0,
        }
    except Exception as e:
        st.session_state["report_docx_bytes"] = None
        st.session_state["_report_docx_scope_key"] = None
        st.session_state["_report_error"] = f"Report generation failed. Details: {str(e)}"
    finally:
        if not isinstance(st.session_state.get("_report_generation_job"), dict):
            hide_overlay()


def _finalize_report_generation_job(job: dict, scope_key: str) -> None:
    narrative = merge_report_section_narratives(
        job.get("section_results") or {},
        job.get("evidence") or {},
    )
    docx_bytes = render_report_docx_from_evidence(
        job.get("style") or {},
        job.get("evidence") or {},
        narrative,
    )
    st.session_state["report_docx_bytes"] = docx_bytes
    st.session_state["_report_docx_scope_key"] = scope_key
    job["status"] = "complete"
    st.session_state["_report_generation_job"] = job


def _process_report_generation_job(scope_key: str, show_overlay, hide_overlay) -> None:
    job = st.session_state.get("_report_generation_job")
    if not isinstance(job, dict) or job.get("status") != "running":
        return
    if job.get("scope_key") != scope_key:
        return

    sections = job.get("sections") or []
    idx = int(job.get("current_index") or 0)
    total = len(sections)

    # if idx >= total:
    #     try:
    #         _finalize_report_generation_job(job, scope_key)
    #     except Exception as e:
    #         st.session_state["_report_error"] = f"Report generation failed while rendering DOCX. Details: {str(e)}"
    #         job["status"] = "failed"
    #     return

    if idx >= total:
        try:
            _finalize_report_generation_job(job, scope_key)
        except Exception as e:
            st.session_state["_report_error"] = f"Report generation failed while rendering DOCX. Details: {str(e)}"
            job["status"] = "failed"
            st.session_state["_report_generation_job"] = job
            _safe_rerun()
        return


    section = sections[idx]
    section_key = str(section.get("key") or "")
    display_name = str(section.get("display_name") or section_key)
    show_overlay(*_report_job_overlay_text(job))
    rerun_requested = False
    try:
        result = generate_report_section_narrative(section_key, job.get("evidence") or {})
        section_results = job.setdefault("section_results", {})
        section_results[section_key] = result
        job["current_index"] = idx + 1
        st.session_state["_report_generation_job"] = job

        if job["current_index"] >= total:
            _finalize_report_generation_job(job, scope_key)
        else:
            rerun_requested = True
            _safe_rerun()
    # except Exception as e:
    #     st.session_state["_report_error"] = f"Report generation failed in section '{display_name}'. Details: {str(e)}"
    #     job["status"] = "failed"
    #     st.session_state["_report_generation_job"] = job

    except Exception as e:
        st.session_state["_report_error"] = f"Report generation failed in section '{display_name}'. Details: {str(e)}"
        job["status"] = "failed"
        st.session_state["_report_generation_job"] = job
        _safe_rerun()    
    finally:
        latest_job = st.session_state.get("_report_generation_job")
        still_running = isinstance(latest_job, dict) and latest_job.get("status") == "running"
        if not (rerun_requested or still_running):
            hide_overlay()


def _render_report_progress(scope_key: str) -> None:
    job = st.session_state.get("_report_generation_job")
    if not isinstance(job, dict) or job.get("scope_key") != scope_key:
        return
    sections = job.get("sections") or []
    total = max(1, len(sections))
    idx = min(int(job.get("current_index") or 0), total)
    status = str(job.get("status") or "running")
    if status == "running":
        next_name = "Finalizing DOCX"
        if idx < len(sections):
            next_name = str(sections[idx].get("display_name") or sections[idx].get("key"))
        st.progress(idx / total, text=f"Report analysis progress: {idx}/{total} sections complete. Next: {next_name}")
    elif status == "complete":
        st.success("Report analysis complete. Download is ready.")
    elif status == "failed":
        st.warning("Report generation stopped before completion. Review the error above and try again.")



# ---------------------------------------------------------------------------
# Main render
# ---------------------------------------------------------------------------

def render_report_tab(selected_server: str, selected_ingestion_date: str | None) -> None:
    st.markdown(
        """
<style>
:root {
  --card-border: rgba(15, 23, 42, 0.10);
  --card-bg: rgba(255,255,255,0.84);
  --muted: rgba(15, 23, 42, 0.68);
  --soft: rgba(15, 23, 42, 0.52);
  --accent: #1f6feb;
  --accent-2: #2f81f7;
  --success: #1a7f37;
  --warning: #b7791f;
  --danger: #c0392b;
  --shadow-sm: 0 2px 10px rgba(15, 23, 42, 0.05);
  --shadow-md: 0 10px 28px rgba(15, 23, 42, 0.08);
  --radius-lg: 18px;
  --radius-md: 14px;
  --radius-sm: 12px;
}

.report-page-title {
  font-size: 2.05rem;
  font-weight: 780;
  letter-spacing: -0.02em;
  margin: 0 0 0.3rem 0;
  color: #0f172a;
}

.report-page-subtitle {
  font-size: 0.98rem;
  color: var(--muted);
  margin: 0 0 1rem 0;
}

[class*="st-key-report_shell"] {
  position: relative;
  isolation: isolate;
}

.report-hero {
  padding: 20px 22px;
  border: 1px solid var(--card-border);
  border-radius: var(--radius-lg);
  background: linear-gradient(180deg, rgba(255,255,255,0.96), rgba(248,250,252,0.92));
  box-shadow: var(--shadow-sm);
}

.report-hero-top {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 16px;
  flex-wrap: wrap;
}

.report-hero-title {
  font-size: 1.4rem;
  font-weight: 760;
  color: #0f172a;
  margin: 0 0 6px 0;
}

.report-hero-sub {
  font-size: 0.95rem;
  color: var(--muted);
  margin: 0;
  line-height: 1.55;
}

.meta-grid {
  display: grid;
  grid-template-columns: repeat(4, minmax(150px, 1fr));
  gap: 10px;
  margin-top: 16px;
}

.meta-card {
  border: 1px solid var(--card-border);
  border-radius: var(--radius-sm);
  background: rgba(255,255,255,0.76);
  padding: 10px 12px;
}

.meta-label {
  font-size: 0.76rem;
  color: var(--soft);
  text-transform: uppercase;
  letter-spacing: 0.04em;
  margin-bottom: 4px;
}

.meta-value {
  font-size: 0.96rem;
  font-weight: 670;
  color: #0f172a;
  line-height: 1.3;
}

.pill {
  display:inline-flex;
  align-items:center;
  gap: 6px;
  padding: 6px 12px;
  border-radius: 999px;
  border: 1px solid rgba(0,0,0,0.10);
  font-size: 0.82rem;
  font-weight: 650;
  white-space: nowrap;
}

.pill-ok   { background: rgba(22,163,74,0.10);  color:#166534; }
.pill-warn { background: rgba(245,158,11,0.14); color:#92400e; }
.pill-bad  { background: rgba(220,38,38,0.10);  color:#991b1b; }

.section-card {
  border: 1px solid var(--card-border);
  border-radius: var(--radius-lg);
  background: var(--card-bg);
  box-shadow: var(--shadow-sm);
  padding: 18px;
}

.section-title {
  font-size: 1.02rem;
  font-weight: 720;
  color: #0f172a;
  margin: 18px 0 10px 0;
}

.kpi-card {
  padding: 14px 14px 12px 14px;
  border: 1px solid var(--card-border);
  border-radius: var(--radius-md);
  background: linear-gradient(180deg, rgba(255,255,255,0.96), rgba(248,250,252,0.90));
  min-height: 106px;
  display: flex;
  flex-direction: column;
  justify-content: space-between;
  box-sizing: border-box;
}

.kpi-label { font-size:.8rem; color:var(--soft); margin-bottom:6px; text-transform:uppercase; letter-spacing:.03em; }
.kpi-val   { font-size:1.85rem; font-weight:780; line-height:1.05; color:#0f172a; }
.kpi-sub   { font-size:.82rem; color:var(--muted); margin-top:6px; }

.workflow-grid { display:grid; grid-template-columns:repeat(3,minmax(0,1fr)); gap:12px; }

.workflow-card {
  border: 1px solid var(--card-border);
  border-radius: var(--radius-md);
  background: rgba(255,255,255,0.82);
  padding: 14px;
  min-height: 112px;
  display: flex;
  flex-direction: column;
  box-sizing: border-box;
}

.workflow-step  { font-size:.78rem; color:var(--soft); text-transform:uppercase; letter-spacing:.04em; margin-bottom:6px; }
.workflow-title { font-size:1rem; font-weight:720; color:#0f172a; margin-bottom:4px; }
.workflow-sub   { font-size:.86rem; color:var(--muted); line-height:1.45; }

.notice {
  padding: 12px 14px;
  border-radius: var(--radius-md);
  border: 1px solid rgba(59,130,246,0.16);
  background: rgba(59,130,246,0.08);
  color: #1e3a8a;
  font-size: .93rem;
  line-height: 1.45;
  margin-top: 4px;
  margin-bottom: 14px;
}

.notice-warning {
  border: 1px solid rgba(245,158,11,0.18);
  background: rgba(245,158,11,0.10);
  color: #92400e;
}

.notice-expert {
  border: 1px solid rgba(139,92,246,0.20);
  background: rgba(139,92,246,0.07);
  color: #4c1d95;
}

.action-card {
  border: 1px solid var(--card-border);
  border-radius: var(--radius-lg);
  background: linear-gradient(180deg, rgba(255,255,255,0.96), rgba(248,250,252,0.90));
  padding: 18px;
  box-shadow: var(--shadow-sm);
}

.action-title { font-size:1rem; font-weight:720; margin:0 0 4px 0; color:#0f172a; }
.action-sub   { font-size:.9rem; color:var(--muted); margin:0 0 12px 0; }

.report-actions-row {
  display: flex;
  align-items: center;
  gap: 12px;
  flex-wrap: wrap;
}

.report-actions-row [data-testid="stButton"],
.report-actions-row [data-testid="stDownloadButton"] { margin:0 !important; }

.report-actions-row [data-testid="stButton"] > button,
.report-actions-row [data-testid="stDownloadButton"] > button {
  height: 46px !important;
  border-radius: 12px !important;
  font-weight: 670 !important;
  padding: 0 18px !important;
  min-width: 250px !important;
  box-shadow: none !important;
}

.primary-btn [data-testid="stButton"] > button {
  background: linear-gradient(135deg, var(--accent), var(--accent-2)) !important;
  color: white !important;
  border: none !important;
}

.primary-btn [data-testid="stButton"] > button:hover {
  transform: translateY(-1px);
  box-shadow: var(--shadow-md) !important;
}

.download-btn [data-testid="stDownloadButton"] > button {
  background: linear-gradient(135deg, #15803d, #16a34a) !important;
  color: white !important;
  border: none !important;
}

.helper-text { font-size:.84rem; color:var(--soft); margin-top:10px; }

[class*="st-key-report_shell"] {
  position: relative !important;
  isolation: isolate !important;
}

.report-local-overlay {
  position: relative !important;
  width: 100%;
  margin: 22px 0 10px 0;
  padding: 0;
  background: transparent !important;
  z-index: 1 !important;
  display: block !important;
  pointer-events: none;
}

.report-overlay-card {
  width: 100%;
  min-width: 0;
  box-sizing: border-box;
  border: 1px solid rgba(15,23,42,0.10);
  border-radius: 18px;
  background: rgba(255,255,255,0.96) !important;
  padding: 16px 20px;
  box-shadow: 0 10px 28px rgba(15,23,42,0.08);
  display: flex;
  align-items: center;
  gap: 16px;
}

.report-spinner {
  flex: 0 0 auto;
  width: 38px;
  height: 38px;
  border: 5px solid rgba(15,23,42,0.10);
  border-top: 5px solid var(--accent);
  border-radius: 50%;
  animation: spin 0.9s linear infinite;
  margin: 0;
}

.report-spinner-text {
  font-weight: 800;
  color: #0f172a;
  text-align: left;
  font-size: 1rem;
  line-height: 1.25;
}

.report-spinner-sub {
  font-size: .92rem;
  color: #334155;
  text-align: left;
  margin-top: 4px;
  font-weight: 560;
  line-height: 1.35;
}

@keyframes spin {
  0% { transform:rotate(0deg); }
  100% { transform:rotate(360deg); }
}

@media (max-width:1100px) {
  .meta-grid { grid-template-columns:repeat(2,minmax(0,1fr)); }
  .workflow-grid { grid-template-columns:1fr; }
}
@media (max-width:760px) {
  .meta-grid { grid-template-columns:1fr; }
  .report-page-title { font-size:1.6rem; }
}
</style>
        """,
        unsafe_allow_html=True,
    )

    overlay_slot = {"slot": None}

    def _show_overlay(title: str, subtitle: str = "Please wait…") -> None:
        slot = overlay_slot.get("slot")
        if slot is None:
            return

        title_html = _html(title)
        subtitle_html = _html(subtitle)

        slot.markdown(
            f"""
<div class="report-local-overlay">
  <div class="report-overlay-card">
    <div class="report-spinner"></div>
    <div class="report-spinner-text">{title_html}</div>
    <div class="report-spinner-sub">{subtitle_html}</div>
  </div>
</div>
            """,
            unsafe_allow_html=True,
        )

    def _hide_overlay() -> None:
        slot = overlay_slot.get("slot")
        if slot is not None:
            slot.empty()

    if not selected_server:
        st.markdown('<div class="report-page-title">Server Health Assessment Report</div>', unsafe_allow_html=True)
        st.markdown(
            '<div class="report-page-subtitle">Generate a polished health assessment package from the selected server snapshot.</div>',
            unsafe_allow_html=True,
        )
        st.info("Select a server first.")
        return

    report_scope_key = _scope_key(selected_server, selected_ingestion_date)
    _reset_report_state_if_scope_changed(report_scope_key)


    profile = None
    try:
        profile = _load_profile(
            selected_server,
            selected_ingestion_date,
            _show_overlay,
            _hide_overlay,
        )
    except Exception as e:
        profile = None
        st.session_state["_report_error"] = f"Failed to load server profile. Details: {str(e)}"

    inst      = (profile or {}).get("instance") or {}
    util      = (profile or {}).get("utilization") or {}
    io_stats  = (profile or {}).get("io_stats") or {}
    snapshot  = (profile or {}).get("snapshot") or "—"

    sql_banner = inst.get("sql_banner") or "SQL Server"
    edition    = inst.get("edition") or "—"
    cpu        = inst.get("cpu_count")
    ram_mb     = inst.get("total_ram_mb")
    os_name    = inst.get("os_name") or "—"

    cpu_peak  = util.get("max_cpu_pct")
    mem_peak  = util.get("max_memory_pct")
    ple_s     = util.get("cache_ple_seconds") or util.get("ple_sec")
    io_total  = io_stats.get("total_io_str") if isinstance(io_stats.get("total_io_str"), str) else "—"

    cpu_str = f"{int(cpu)} cores" if isinstance(cpu, (int, float)) else "—"
    ram_str = f"{(float(ram_mb)/1024):.0f} GB RAM" if isinstance(ram_mb, (int, float)) else "—"

    pill_cls = "pill-ok"
    pill_txt = "Assessment ready"
    if isinstance(ple_s, (int, float)) and ple_s <= 300:
        pill_cls, pill_txt = "pill-bad", "Critical: low buffer cache"
    elif isinstance(cpu_peak, (int, float)) and cpu_peak >= 85:
        pill_cls, pill_txt = "pill-warn", "High CPU pressure"
    elif isinstance(mem_peak, (int, float)) and mem_peak >= 85:
        pill_cls, pill_txt = "pill-warn", "High memory pressure"

    server_html = _html(selected_server)
    snapshot_html = _html(snapshot)
    sql_banner_html = _html(sql_banner)
    edition_html = _html(edition)
    cpu_html = _html(cpu_str)
    ram_html = _html(ram_str)
    os_html = _html(os_name)
    io_total_html = _html(io_total)
    pill_txt_html = _html(pill_txt)

    # Streamlit versions before container-key support raise:
    # TypeError: LayoutsMixin.container() got an unexpected keyword argument 'key'
    try:
        report_shell = st.container(key="report_shell")
    except TypeError:
        report_shell = st.container()

    with report_shell:
        st.markdown('<div class="report-page-title">Server Health Assessment Report</div>', unsafe_allow_html=True)   

        st.markdown(
            '<div class="report-page-subtitle">Generate an executive-ready DOCX assessment from the selected ingestion snapshot using deterministic expert-analysis evidence plus resumable section-specific LLM analysis.</div>',
            unsafe_allow_html=True,
        )

        # Hero card
        st.markdown(
            f"""
    <div class="report-hero">
      <div class="report-hero-top">
        <div>
          <div class="report-hero-title">Server snapshot overview</div>
          <p class="report-hero-sub">
            Build a packaged assessment for <code>{server_html}</code> using the selected ingestion snapshot and current extracted evidence.
          </p>
        </div>
        <div><span class="pill {pill_cls}">{pill_txt_html}</span></div>
      </div>
      <div class="meta-grid">
        <div class="meta-card"><div class="meta-label">Server</div><div class="meta-value"><code>{server_html}</code></div></div>
        <div class="meta-card"><div class="meta-label">Snapshot</div><div class="meta-value"><code>{snapshot_html}</code></div></div>
        <div class="meta-card"><div class="meta-label">SQL Build</div><div class="meta-value">{sql_banner_html}</div></div>
        <div class="meta-card"><div class="meta-label">Edition</div><div class="meta-value">{edition_html}</div></div>
        <div class="meta-card"><div class="meta-label">CPU</div><div class="meta-value">{cpu_html}</div></div>
        <div class="meta-card"><div class="meta-label">Memory</div><div class="meta-value">{ram_html}</div></div>
        <div class="meta-card"><div class="meta-label">Operating System</div><div class="meta-value">{os_html}</div></div>
        <div class="meta-card"><div class="meta-label">Output</div><div class="meta-value">DOCX assessment</div></div>
      </div>
    </div>
            """,
            unsafe_allow_html=True,
        )

        # KPI row
        st.markdown('<div class="section-title">Key health indicators</div>', unsafe_allow_html=True)
        k1, k2, k3, k4 = st.columns(4, gap="small")
        with k1:
            st.markdown(f'<div class="kpi-card"><div class="kpi-label">CPU Peak</div><div class="kpi-val">{_safe_pct(cpu_peak)}</div><div class="kpi-sub">Maximum observed CPU utilisation</div></div>', unsafe_allow_html=True)
        with k2:
            st.markdown(f'<div class="kpi-card"><div class="kpi-label">Memory Peak</div><div class="kpi-val">{_safe_pct(mem_peak)}</div><div class="kpi-sub">Maximum observed memory utilisation</div></div>', unsafe_allow_html=True)
        with k3:
            st.markdown(f'<div class="kpi-card"><div class="kpi-label">Page Life Expectancy</div><div class="kpi-val">{_safe_int_seconds(ple_s)}</div><div class="kpi-sub">Buffer cache health proxy (Glen Berry: &lt;300 s = critical)</div></div>', unsafe_allow_html=True)
        with k4:
            st.markdown(f'<div class="kpi-card"><div class="kpi-label">Total I/O</div><div class="kpi-val">{io_total_html}</div><div class="kpi-sub">Database I/O volume across the snapshot scope</div></div>', unsafe_allow_html=True)

        # Workflow
        st.markdown('<div class="section-title">Workflow</div>', unsafe_allow_html=True)
        st.markdown(
            """
    <div class="workflow-grid">
      <div class="workflow-card">
        <div class="workflow-step">Step 1</div>
        <div class="workflow-title">Review scope</div>
        <div class="workflow-sub">Confirm the selected server and ingestion snapshot. Prompt customisation is intentionally disabled for consistent report output.</div>
      </div>
      <div class="workflow-card">
        <div class="workflow-step">Step 2</div>
        <div class="workflow-title">Generate</div>
        <div class="workflow-sub">The selected ingestion is validated first, then each expert section is enriched one at a time so reruns can resume safely.</div>
      </div>
      <div class="workflow-card">
        <div class="workflow-step">Step 3</div>
        <div class="workflow-title">Download</div>
        <div class="workflow-sub">Export the finished DOCX package for stakeholder review, distribution, or archival.</div>
      </div>
    </div>
            """,
            unsafe_allow_html=True,
        )
        # ----- Error / readiness notice -----
        report_error = st.session_state.get("_report_error")
        if report_error:
            st.error(report_error)

        snapshot_loaded = bool(profile and (profile.get("snapshot") or "").strip())
        ingestion_selected = bool(selected_ingestion_date)

        if snapshot_loaded and ingestion_selected and not report_error:
            st.markdown(
                '<div class="notice"><b>Ready.</b> The selected ingestion snapshot is loaded and report generation will use scoped evidence for this server/date.</div>',
                unsafe_allow_html=True,
            )
        elif not ingestion_selected:
            st.markdown(
                '<div class="notice notice-warning"><b>Not ready.</b> Select an ingestion date before generating a report.</div>',
                unsafe_allow_html=True,
            )
        else:
            st.markdown(
                '<div class="notice notice-warning"><b>Not ready.</b> The report pipeline is blocked until the selected ingestion snapshot loads successfully.</div>',
                unsafe_allow_html=True,
            )

        # ----- Action card -----
        active_job = st.session_state.get("_report_generation_job")
        job_running = (
            isinstance(active_job, dict)
            and active_job.get("scope_key") == report_scope_key
            and active_job.get("status") == "running"
        )
        can_generate = snapshot_loaded and ingestion_selected and not report_error and not job_running
        st.markdown(
            """
    <div class="action-card">
      <div class="action-title">Generate and export report</div>
      <div class="action-sub">The app validates the selected ingestion, enriches each report section with expert prompts, then renders the DOCX. Once done, the download button appears.</div>
    </div>
            """,
            unsafe_allow_html=True,
        )

        btn_cols = st.columns([1, 1, 2], gap="small")

        with btn_cols[0]:
            st.markdown('<div class="primary-btn">', unsafe_allow_html=True)
            generate_clicked = st.button(
                "Generate report",
                key="generate_report_btn",
                use_container_width=True,
                disabled=not can_generate,
            )
            st.markdown("</div>", unsafe_allow_html=True)

        # if generate_clicked:
        #     _start_report_generation_job(
        #         selected_server,
        #         selected_ingestion_date,
        #         report_scope_key,
        #         _show_overlay,
        #         _hide_overlay,
        #     )

        # _render_report_progress(report_scope_key)
        # _process_report_generation_job(report_scope_key, _show_overlay, _hide_overlay)

        # Bottom loading/status area.
        # Keep this directly above the horizontal progress bar.
        overlay_slot["slot"] = st.empty()

        active_job_for_overlay = st.session_state.get("_report_generation_job")
        if (
            isinstance(active_job_for_overlay, dict)
            and active_job_for_overlay.get("scope_key") == report_scope_key
            and active_job_for_overlay.get("status") == "running"
        ):
            _show_overlay(*_report_job_overlay_text(active_job_for_overlay))

        if generate_clicked:
            _start_report_generation_job(
                selected_server,
                selected_ingestion_date,
                report_scope_key,
                _show_overlay,
                _hide_overlay,
            )

        _render_report_progress(report_scope_key)
        _process_report_generation_job(report_scope_key, _show_overlay, _hide_overlay)         
        docx_bytes = (
            st.session_state.get("report_docx_bytes")
            if st.session_state.get("_report_docx_scope_key") == report_scope_key
            else None
        )

        if docx_bytes:
            with btn_cols[1]:
                st.markdown('<div class="download-btn">', unsafe_allow_html=True)
                st.download_button(
                    "⬇ Download DOCX",
                    data=docx_bytes,
                    file_name=get_report_filename(selected_server),
                    mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                    use_container_width=True,
                )
                st.markdown("</div>", unsafe_allow_html=True)
