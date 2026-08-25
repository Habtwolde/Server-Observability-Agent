"""
ui/recent_changes_tab.py

Renders the "Recent Changes" tab for the Server Observability Dashboard.

Displays a bulleted analysis of metrics that changed significantly between
the currently selected ingestion date and the previous one, ordered from
HIGH → MEDIUM → LOW risk.

Each finding includes:
  - What changed and by how much
  - What it means for applications connecting to databases on this server
  - Recommendations and future diagnostics
  - Persistent risks (present in both snapshots) are flagged prominently
"""

from __future__ import annotations

from html import escape
from typing import Any, Dict, List

import streamlit as st

from services.recent_changes_service import build_changes_report, generate_ai_briefing, HIGH, MEDIUM, LOW

# ---------------------------------------------------------------------------
# CSS
# ---------------------------------------------------------------------------
_CSS = """
<style>
:root {
  --rc-high:       #dc2626;
  --rc-high-bg:    rgba(220, 38, 38, 0.07);
  --rc-high-bdr:   rgba(220, 38, 38, 0.30);
  --rc-med:        #d97706;
  --rc-med-bg:     rgba(217, 119, 6, 0.07);
  --rc-med-bdr:    rgba(217, 119, 6, 0.30);
  --rc-low:        #16a34a;
  --rc-low-bg:     rgba(22, 163, 74, 0.06);
  --rc-low-bdr:    rgba(22, 163, 74, 0.28);
  --rc-text:       #0f172a;
  --rc-text-soft:  #475569;
  --rc-text-dim:   #64748b;
  --rc-panel:      rgba(255,255,255,0.92);
  --rc-shadow-sm:  0 1px 3px rgba(15,23,42,0.06);
  --rc-shadow-md:  0 8px 24px rgba(15,23,42,0.09);
}

.rc-page-title {
  margin: 0;
  font-size: 2rem;
  font-weight: 820;
  letter-spacing: -0.03em;
  color: var(--rc-text);
}

.rc-page-sub {
  margin: 6px 0 0 0;
  font-size: 0.97rem;
  line-height: 1.55;
  color: var(--rc-text-soft);
}

/* ── Comparison period banner ── */
.rc-period-bar {
  display: flex;
  align-items: center;
  gap: 10px;
  padding: 10px 14px;
  background: linear-gradient(90deg, rgba(59,130,246,0.10) 0%, rgba(59,130,246,0.04) 100%);
  border-left: 3px solid #3b82f6;
  border-radius: 0 6px 6px 0;
  margin: 14px 0 6px 0;
  font-size: 0.93rem;
  color: #1e3a8a;
  flex-wrap: wrap;
}

.rc-period-bar code {
  background: rgba(255,255,255,0.72);
  padding: 1px 5px;
  border-radius: 4px;
  font-size: 0.88em;
}

.rc-period-arrow {
  font-weight: 900;
  color: #2563eb;
}

/* ── Summary pill row ── */
.rc-summary-row {
  display: flex;
  gap: 10px;
  margin: 14px 0;
  flex-wrap: wrap;
}

.rc-summary-pill {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  padding: 6px 14px;
  border-radius: 999px;
  font-size: 0.85rem;
  font-weight: 760;
  white-space: nowrap;
}

.rc-summary-pill.high  { background: var(--rc-high-bg);  color: var(--rc-high); border: 1px solid var(--rc-high-bdr); }
.rc-summary-pill.med   { background: var(--rc-med-bg);   color: var(--rc-med);  border: 1px solid var(--rc-med-bdr); }
.rc-summary-pill.low   { background: var(--rc-low-bg);   color: var(--rc-low);  border: 1px solid var(--rc-low-bdr); }
.rc-summary-pill.ok    { background: rgba(148,163,184,0.12); color: #475569; border: 1px solid rgba(148,163,184,0.28); }

/* ── Section headers (HIGH / MEDIUM / LOW) ── */
.rc-section-header {
  display: flex;
  align-items: center;
  gap: 10px;
  margin: 22px 0 10px 0;
  font-size: 1.02rem;
  font-weight: 800;
  letter-spacing: 0.01em;
}

.rc-section-badge {
  display: inline-flex;
  align-items: center;
  padding: 3px 12px;
  border-radius: 999px;
  font-size: 0.78rem;
  font-weight: 800;
  letter-spacing: 0.06em;
  text-transform: uppercase;
}

.rc-section-badge.high  { background: var(--rc-high);  color: #fff; }
.rc-section-badge.med   { background: var(--rc-med);   color: #fff; }
.rc-section-badge.low   { background: var(--rc-low);   color: #fff; }

/* ── Finding cards ── */
.rc-card {
  position: relative;
  padding: 14px 16px 14px 18px;
  margin-bottom: 10px;
  border-radius: 8px;
  background: var(--rc-panel);
  box-shadow: var(--rc-shadow-sm);
  border-left: 4px solid transparent;
  transition: box-shadow 0.14s ease;
}

.rc-card:hover { box-shadow: var(--rc-shadow-md); }
.rc-card.high  { border-left-color: var(--rc-high); background: linear-gradient(90deg, var(--rc-high-bg) 0%, rgba(255,255,255,0.92) 22%); }
.rc-card.med   { border-left-color: var(--rc-med);  background: linear-gradient(90deg, var(--rc-med-bg)  0%, rgba(255,255,255,0.92) 22%); }
.rc-card.low   { border-left-color: var(--rc-low);  background: linear-gradient(90deg, var(--rc-low-bg)  0%, rgba(255,255,255,0.92) 22%); }

.rc-card-persistent-tag {
  display: inline-block;
  margin-left: 8px;
  padding: 1px 8px;
  border-radius: 999px;
  font-size: 0.68rem;
  font-weight: 800;
  text-transform: uppercase;
  letter-spacing: 0.05em;
  background: rgba(220,38,38,0.12);
  color: var(--rc-high);
  border: 1px solid rgba(220,38,38,0.28);
  vertical-align: middle;
}

.rc-card-headline {
  font-size: 0.97rem;
  font-weight: 780;
  color: var(--rc-text);
  margin-bottom: 8px;
  line-height: 1.35;
}

.rc-delta-row {
  display: flex;
  align-items: center;
  gap: 18px;
  margin-bottom: 10px;
  flex-wrap: wrap;
}

.rc-delta-item {
  display: flex;
  flex-direction: column;
  gap: 1px;
}

.rc-delta-label {
  font-size: 0.72rem;
  text-transform: uppercase;
  letter-spacing: 0.05em;
  color: var(--rc-text-dim);
  font-weight: 760;
}

.rc-delta-value {
  font-size: 0.92rem;
  font-weight: 720;
  color: var(--rc-text);
}

.rc-delta-value.change { color: #2563eb; font-weight: 800; }

/* ── Sub-sections inside cards ── */
.rc-card-section-title {
  font-size: 0.78rem;
  text-transform: uppercase;
  letter-spacing: 0.05em;
  font-weight: 760;
  color: var(--rc-text-dim);
  margin: 10px 0 4px 0;
}

.rc-card-body {
  font-size: 0.9rem;
  line-height: 1.6;
  color: var(--rc-text-soft);
}

.rc-card-body ul {
  margin: 4px 0 0 0;
  padding-left: 18px;
}

.rc-card-body ul li { margin-bottom: 3px; }

.rc-card-diag {
  margin-top: 8px;
  padding: 8px 10px;
  background: rgba(15,23,42,0.04);
  border-radius: 5px;
  font-size: 0.80rem;
  font-family: 'JetBrains Mono', 'Fira Code', monospace;
  color: #334155;
  white-space: pre-wrap;
  word-break: break-all;
  border: 1px solid rgba(15,23,42,0.08);
}

/* ── Category chips ── */
.rc-category-chip {
  display: inline-block;
  padding: 1px 9px;
  border-radius: 4px;
  font-size: 0.72rem;
  font-weight: 700;
  background: rgba(59,130,246,0.08);
  color: #2563eb;
  margin-bottom: 6px;
}

/* ── Empty state ── */
.rc-empty {
  padding: 40px 20px;
  text-align: center;
  border: 1px dashed rgba(15,23,42,0.14);
  border-radius: 8px;
  background: rgba(248,250,252,0.88);
  color: var(--rc-text-soft);
}

.rc-empty-title {
  font-size: 1.05rem;
  font-weight: 780;
  color: var(--rc-text);
  margin-bottom: 8px;
}

/* ── AI Executive Briefing card ── */
.rc-ai-card {
  position: relative;
  overflow: hidden;
  padding: 18px 20px 18px 20px;
  margin: 16px 0 4px 0;
  border-radius: 8px;
  background:
    linear-gradient(135deg, rgba(11,18,32,0.97) 0%, rgba(17,26,45,0.95) 60%, rgba(15,23,42,0.93) 100%);
  box-shadow: 0 12px 30px rgba(2,6,23,0.16);
  border: 1px solid rgba(59,130,246,0.16);
}

.rc-ai-card:before {
  content: "";
  position: absolute;
  inset: 0;
  background:
    linear-gradient(90deg, rgba(59,130,246,0.06) 0, rgba(59,130,246,0.06) 1px, transparent 1px, transparent 40px),
    linear-gradient(180deg, rgba(59,130,246,0.04) 0, rgba(59,130,246,0.04) 1px, transparent 1px, transparent 40px);
  opacity: 0.3;
  pointer-events: none;
}

.rc-ai-card:after {
  content: "";
  position: absolute;
  inset: auto 0 0 0;
  height: 2px;
  background: linear-gradient(90deg, transparent 0%, rgba(59,130,246,0.50) 22%, rgba(14,165,233,0.55) 58%, transparent 100%);
}

.rc-ai-card-header {
  position: relative;
  z-index: 1;
  display: flex;
  align-items: center;
  gap: 10px;
  margin-bottom: 12px;
}

.rc-ai-label {
  display: inline-flex;
  align-items: center;
  gap: 5px;
  padding: 3px 10px;
  border-radius: 999px;
  font-size: 0.70rem;
  font-weight: 800;
  letter-spacing: 0.08em;
  text-transform: uppercase;
  background: rgba(59,130,246,0.18);
  color: #93c5fd;
  border: 1px solid rgba(59,130,246,0.28);
}

.rc-ai-card-title {
  font-size: 0.95rem;
  font-weight: 780;
  color: rgba(226,232,240,0.92);
  letter-spacing: -0.01em;
}

.rc-ai-body {
  position: relative;
  z-index: 1;
  font-size: 0.96rem;
  line-height: 1.7;
  color: rgba(226,232,240,0.85);
}

.rc-ai-error {
  position: relative;
  z-index: 1;
  font-size: 0.88rem;
  color: rgba(148,163,184,0.70);
  font-style: italic;
}

/* ── No previous snapshot notice ── */
.rc-no-prev {
  padding: 24px 20px;
  border-left: 3px solid #94a3b8;
  border-radius: 0 6px 6px 0;
  background: rgba(248,250,252,0.88);
  color: var(--rc-text-soft);
  font-size: 0.95rem;
  line-height: 1.6;
}

@media (max-width: 760px) {
  .rc-page-title { font-size: 1.6rem; }
  .rc-delta-row  { gap: 10px; }
}
</style>
"""

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_RISK_LABEL = {HIGH: "HIGH", MEDIUM: "MEDIUM", LOW: "LOW"}
_RISK_CLASS = {HIGH: "high", MEDIUM: "med", LOW: "low"}
_RISK_ICON  = {HIGH: "🔴", MEDIUM: "🟡", LOW: "🟢"}


def _card_html(finding: Dict[str, Any]) -> str:
    risk = finding.get("risk", LOW)
    css_class = _RISK_CLASS.get(risk, "low")
    persistent = finding.get("persistent", False)

    cat = escape(str(finding.get("category", "")))
    headline = escape(str(finding.get("headline", "")))
    impact = escape(str(finding.get("impact", "")))
    recommendation = escape(str(finding.get("recommendation", "")))
    prev_v = escape(str(finding.get("previous", "—")))
    cur_v = escape(str(finding.get("current", "—")))
    delta = escape(str(finding.get("delta_str", "")))

    diagnostics: List[str] = finding.get("diagnostics") or []

    persistent_tag = (
        '<span class="rc-card-persistent-tag">Risk persists</span>'
        if persistent else ""
    )

    diag_blocks = ""
    if diagnostics:
        for dq in diagnostics:
            diag_blocks += f'<div class="rc-card-diag">{escape(dq)}</div>\n'

    return f"""
<div class="rc-card {css_class}">
  <div class="rc-category-chip">{cat}</div>
  <div class="rc-card-headline">{headline}{persistent_tag}</div>
  <div class="rc-delta-row">
    <div class="rc-delta-item">
      <span class="rc-delta-label">Previous</span>
      <span class="rc-delta-value">{prev_v}</span>
    </div>
    <div class="rc-delta-item">
      <span class="rc-delta-label">Current</span>
      <span class="rc-delta-value">{cur_v}</span>
    </div>
    <div class="rc-delta-item">
      <span class="rc-delta-label">Change</span>
      <span class="rc-delta-value change">{delta}</span>
    </div>
  </div>
  <div class="rc-card-section-title">Application Impact</div>
  <div class="rc-card-body">{impact}</div>
  <div class="rc-card-section-title">Recommendation</div>
  <div class="rc-card-body">{recommendation}</div>
  {"<div class='rc-card-section-title'>Diagnostic Query</div>" + diag_blocks if diag_blocks else ""}
</div>
"""


def _render_section(
    findings: List[Dict[str, Any]], risk: str, use_expander: bool = False
) -> None:
    if not findings:
        return

    label = _RISK_LABEL[risk]
    css_cls = _RISK_CLASS[risk]
    icon = _RISK_ICON[risk]

    st.markdown(
        f"""
<div class="rc-section-header">
  <span class="rc-section-badge {css_cls}">{label}</span>
  <span style="color:var(--rc-text-soft); font-weight:600; font-size:0.95rem;">
    {icon} {len(findings)} finding{'s' if len(findings) != 1 else ''}
  </span>
</div>
""",
        unsafe_allow_html=True,
    )

    for finding in findings:
        html = _card_html(finding)
        if use_expander:
            with st.expander(finding.get("headline", "Finding"), expanded=False):
                st.markdown(html, unsafe_allow_html=True)
        else:
            st.markdown(html, unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# AI Briefing renderer
# ---------------------------------------------------------------------------

def _render_ai_briefing(report: Dict[str, Any]) -> None:
    """Render the AI executive briefing card with a spinner while generating."""

    cache_key = f"_rc_ai_briefing_{report['server_name']}_{report['current_date']}"

    # Use session_state to cache the generated text across reruns
    if cache_key not in st.session_state:
        st.session_state[cache_key] = None

    cached_text = st.session_state[cache_key]
    cached_error = st.session_state.get(f"{cache_key}__err")

    if cached_text is None and cached_error is None:
        with st.spinner("Generating AI executive briefing…"):
            try:
                text = generate_ai_briefing(report)
                st.session_state[cache_key] = text
                cached_text = text
            except Exception as exc:
                err_msg = str(exc)[:280]
                st.session_state[f"{cache_key}__err"] = err_msg
                cached_error = err_msg

    if cached_error:
        st.markdown(
            f"""
<div class="rc-ai-card">
  <div class="rc-ai-card-header">
    <span class="rc-ai-label">✦ AI Briefing</span>
    <span class="rc-ai-card-title">Executive Summary</span>
  </div>
  <div class="rc-ai-error">
    AI briefing could not be generated: {escape(cached_error)}.<br>
    The detailed findings below remain fully available.
  </div>
</div>
""",
            unsafe_allow_html=True,
        )
        return

    if cached_text:
        safe_text = escape(cached_text)
        st.markdown(
            f"""
<div class="rc-ai-card">
  <div class="rc-ai-card-header">
    <span class="rc-ai-label">✦ AI Briefing</span>
    <span class="rc-ai-card-title">Executive Summary</span>
  </div>
  <div class="rc-ai-body">{safe_text}</div>
</div>
""",
            unsafe_allow_html=True,
        )


# ---------------------------------------------------------------------------
# Public render function
# ---------------------------------------------------------------------------

def render_recent_changes_tab(selected_server: str, selected_ingestion_date: str) -> None:
    st.markdown(_CSS, unsafe_allow_html=True)

    st.markdown(
        '<div class="rc-page-title">Recent Changes</div>',
        unsafe_allow_html=True,
    )
    st.markdown(
        '<div class="rc-page-sub">'
        "Bulleted analysis of metrics that changed significantly since the last ingestion run. "
        "Findings are ordered from highest to lowest risk. Persistent risks — present in the "
        "previous snapshot and still present now — are flagged separately."
        "</div>",
        unsafe_allow_html=True,
    )

    report = build_changes_report(selected_server, selected_ingestion_date)

    if not report["has_previous"]:
        st.markdown(
            """
<div class="rc-no-prev">
  <strong>No previous snapshot available for comparison.</strong><br>
  This appears to be the first ingestion run for this server under the selected scope.
  Select an ingestion date that has a preceding run to enable the changes analysis.
</div>
""",
            unsafe_allow_html=True,
        )
        return

    prev_date = report["previous_date"]
    cur_date = report["current_date"]

    st.markdown(
        f"""
<div class="rc-period-bar">
  <span>Comparing</span>
  <code>{prev_date}</code>
  <span class="rc-period-arrow">→</span>
  <code>{cur_date}</code>
  <span style="color:#64748b; font-size:0.88em;">
    (previous ingestion → current ingestion)
  </span>
</div>
""",
        unsafe_allow_html=True,
    )

    counts = report["summary_counts"]
    total = sum(counts.values())

    high_n = counts.get(HIGH, 0)
    med_n = counts.get(MEDIUM, 0)
    low_n = counts.get(LOW, 0)

    if total == 0:
        st.markdown(
            """
<div class="rc-empty">
  <div class="rc-empty-title">No significant changes detected</div>
  All tracked metrics are within expected thresholds relative to the previous ingestion run.
  The server posture appears stable between these two snapshots.
</div>
""",
            unsafe_allow_html=True,
        )
        return

    pills_html = '<div class="rc-summary-row">'
    if high_n:
        pills_html += f'<span class="rc-summary-pill high">🔴 {high_n} High Risk</span>'
    if med_n:
        pills_html += f'<span class="rc-summary-pill med">🟡 {med_n} Medium Risk</span>'
    if low_n:
        pills_html += f'<span class="rc-summary-pill low">🟢 {low_n} Low Risk</span>'
    pills_html += f'<span class="rc-summary-pill ok">{total} total finding{"s" if total != 1 else ""}</span>'
    pills_html += "</div>"
    st.markdown(pills_html, unsafe_allow_html=True)

    # ── AI Executive Briefing ──────────────────────────────────────────────
    _render_ai_briefing(report)

    findings = report["findings"]

    high_findings = [f for f in findings if f["risk"] == HIGH]
    med_findings  = [f for f in findings if f["risk"] == MEDIUM]
    low_findings  = [f for f in findings if f["risk"] == LOW]

    _render_section(high_findings, HIGH, use_expander=False)
    _render_section(med_findings, MEDIUM, use_expander=False)
    _render_section(low_findings, LOW, use_expander=len(low_findings) > 4)
