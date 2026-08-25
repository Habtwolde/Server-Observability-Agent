from __future__ import annotations

import pandas as pd
import streamlit as st
from html import escape

from services.metrics_service import build_server_profile

_CSS = r"""
<style>
:root {
  --ov-line: rgba(15, 23, 42, 0.10);
  --ov-line-soft: rgba(15, 23, 42, 0.06);
  --ov-text: #0f172a;
  --ov-text-soft: #475569;
  --ov-text-dim: #64748b;
  --ov-panel: rgba(255, 255, 255, 0.82);
  --ov-panel-soft: rgba(248, 250, 252, 0.88);
  --ov-accent: #3b82f6;
  --ov-accent-2: #2563eb;
  --ov-ok: #16a34a;
  --ov-warn: #d97706;
  --ov-bad: #dc2626;
  --ov-shadow-sm: 0 1px 2px rgba(15, 23, 42, 0.04);
  --ov-shadow-md: 0 12px 28px rgba(15, 23, 42, 0.07);
}

.overview-page-title {
  margin: 0;
  font-size: 2rem;
  font-weight: 820;
  letter-spacing: -0.03em;
  color: var(--ov-text);
}

.overview-page-subtitle {
  margin: 8px 0 0 0;
  font-size: 0.98rem;
  line-height: 1.55;
  color: var(--ov-text-soft);
}

.overview-shell {
  display: flex;
  flex-direction: column;
  gap: 14px;
}

.overview-hero {
  position: relative;
  overflow: hidden;
  padding: 18px 20px;
  border-radius: 8px;
  background:
    linear-gradient(135deg, rgba(11,18,32,0.98) 0%, rgba(17,26,45,0.96) 60%, rgba(15,23,42,0.94) 100%);
  box-shadow: 0 12px 30px rgba(2, 6, 23, 0.14);
}

.overview-hero:before {
  content: "";
  position: absolute;
  inset: 0;
  background:
    linear-gradient(90deg, rgba(59,130,246,0.07) 0, rgba(59,130,246,0.07) 1px, transparent 1px, transparent 42px),
    linear-gradient(180deg, rgba(59,130,246,0.05) 0, rgba(59,130,246,0.05) 1px, transparent 1px, transparent 42px);
  opacity: 0.28;
  pointer-events: none;
}

.overview-hero:after {
  content: "";
  position: absolute;
  inset: auto 0 0 0;
  height: 2px;
  background: linear-gradient(90deg, transparent 0%, rgba(59,130,246,0.55) 22%, rgba(14,165,233,0.62) 55%, transparent 100%);
}

.overview-hero-row {
  position: relative;
  z-index: 1;
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 16px;
  flex-wrap: wrap;
}

.overview-hero-title {
  margin: 0;
  font-size: 1.8rem;
  line-height: 1.12;
  font-weight: 820;
  color: #f8fbff;
}

.overview-hero-sub {
  margin: 8px 0 0 0;
  color: rgba(226, 232, 240, 0.88);
  line-height: 1.55;
  font-size: 0.96rem;
}


.health-pill {
  display: inline-flex;
  align-items: center;
  gap: 8px;
  padding: 6px 12px;
  border-radius: 999px;
  font-size: 0.8rem;
  font-weight: 760;
  white-space: nowrap;
  color: #e2e8f0;
  background: rgba(255,255,255,0.08);
  border: none;
}

.health-pill.ok {
  background: rgba(22, 163, 74, 0.16);
  color: #dcfce7;
}

.overview-hero-row > div:first-child {
  flex: 1 1 640px;
  min-width: 0;
}

.overview-hero-title {
  margin: 0;
  font-size: 1.8rem;
  line-height: 1.12;
  font-weight: 820;
  color: #f8fbff;
  word-break: break-word;
}

.health-pill.warn {
  background: rgba(217, 119, 6, 0.18);
  color: #fef3c7;
}

.health-pill.bad {
  background: rgba(220, 38, 38, 0.18);
  color: #fee2e2;
}

.overview-meta-grid {
  position: relative;
  z-index: 1;
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
  gap: 10px;
  margin-top: 16px;
}

.overview-meta-card {
  padding: 10px 12px;
  background: rgba(255,255,255,0.10);
  border-radius: 6px;
  border: 1px solid rgba(255,255,255,0.06);
}

@supports (backdrop-filter: blur(3px)) {
  .overview-meta-card {
    backdrop-filter: blur(3px);
    background: rgba(255,255,255,0.06);
  }
}

.overview-meta-label {
  font-size: 0.75rem;
  text-transform: uppercase;
  letter-spacing: 0.05em;
  color: rgba(191, 219, 254, 0.86);
  margin-bottom: 4px;
  font-weight: 760;
}

.overview-meta-value {
  font-size: 0.96rem;
  font-weight: 700;
  color: #f8fbff;
  line-height: 1.35;
}

.overview-insight {
  padding: 14px 16px;
  border-left: 3px solid var(--ov-accent);
  background: linear-gradient(90deg, rgba(59,130,246,0.08) 0%, rgba(59,130,246,0.03) 100%);
}

.overview-insight-title {
  font-size: 1rem;
  font-weight: 780;
  color: var(--ov-text);
  margin-bottom: 4px;
}

.overview-insight-text {
  color: var(--ov-text-soft);
  line-height: 1.55;
}

.kpi-card {
  padding: 14px 14px 12px 14px;
  background: linear-gradient(180deg, rgba(255,255,255,0.94) 0%, rgba(248,250,252,0.88) 100%);
  border-radius: 6px;
  box-shadow: var(--ov-shadow-sm);
  min-height: 112px;
  display: flex;
  flex-direction: column;
  justify-content: space-between;
}

.kpi-card.ok {
  box-shadow: inset 0 2px 0 rgba(22, 163, 74, 0.28), var(--ov-shadow-sm);
}

.kpi-card.warn {
  box-shadow: inset 0 2px 0 rgba(217, 119, 6, 0.28), var(--ov-shadow-sm);
}

.kpi-card.bad {
  box-shadow: inset 0 2px 0 rgba(220, 38, 38, 0.28), var(--ov-shadow-sm);
}

.kpi-label {
  font-size: 0.78rem;
  text-transform: uppercase;
  letter-spacing: 0.05em;
  color: var(--ov-text-dim);
  font-weight: 760;
}

.kpi-value {
  margin-top: 6px;
  font-size: 1.82rem;
  font-weight: 840;
  line-height: 1.06;
  color: var(--ov-text);
}

.kpi-hint {
  margin-top: 6px;
  font-size: 0.82rem;
  color: var(--ov-text-soft);
}

.section-title {
  margin: 18px 0 10px 0;
  font-size: 1.02rem;
  font-weight: 780;
  color: var(--ov-text);
}

.metric-panel {
  padding: 14px 14px 12px 14px;
  background: linear-gradient(180deg, rgba(255,255,255,0.92) 0%, rgba(248,250,252,0.84) 100%);
  border-radius: 6px;
  box-shadow: var(--ov-shadow-sm);
  height: 100%;
  box-sizing: border-box;
}

.metric-block-title {
  font-size: 0.96rem;
  font-weight: 780;
  color: var(--ov-text);
  margin-bottom: 10px;
}

.waits-table {
  width: 100%;
  border-collapse: collapse;
  margin-top: 8px;
}

.waits-table th,
.waits-table td {
  padding: 9px 10px;
  border-top: 1px solid rgba(15, 23, 42, 0.08);
  font-size: 0.88rem;
  vertical-align: middle;
}

.waits-table thead th {
  border-top: none;
  font-weight: 760;
  color: var(--ov-text-dim);
  background: rgba(241, 245, 249, 0.85);
}

.wait-type {
  font-weight: 760;
  color: var(--ov-text);
}

.wait-type.top {
  font-weight: 900;
}

.badge-mini {
  display: inline-block;
  padding: 1px 8px;
  border-radius: 999px;
  font-size: 0.72rem;
  font-weight: 700;
  background: rgba(59, 130, 246, 0.08);
  color: var(--ov-accent-2);
  margin-left: 6px;
}

.bar-wrap {
  height: 10px;
  background: rgba(15, 23, 42, 0.08);
  border-radius: 999px;
  overflow: hidden;
}

.bar-fill {
  height: 100%;
  border-radius: 999px;
  background: linear-gradient(90deg, rgba(59,130,246,0.20) 0%, rgba(59,130,246,0.62) 100%);
}


div[data-testid="stDataFrame"] thead tr th {
  background: rgba(241, 245, 249, 0.88) !important;
}

@media (max-width: 760px) {
  .overview-page-title {
    font-size: 1.6rem;
  }

  .overview-meta-grid {
    grid-template-columns: 1fr;
  }
}
</style>
"""


def _fmt_pct(v):
    return f"{v:.1f}%" if isinstance(v, (int, float)) else "—"


def _fmt_int(v):
    try:
        return f"{int(float(v))}"
    except Exception:
        return "—"


def _fmt_s(v):
    try:
        return f"{int(float(v))}s"
    except Exception:
        return "—"


def _mb_to_gb(v):
    try:
        return f"{float(v) / 1024:.1f} GB"
    except Exception:
        return "—"


def _health(cpu, mem, ple):
    score = 0
    if isinstance(cpu, (int, float)):
        score += 2 if cpu >= 85 else (1 if cpu >= 65 else 0)
    if isinstance(mem, (int, float)):
        score += 2 if mem >= 85 else (1 if mem >= 65 else 0)
    if isinstance(ple, (int, float)):
        score += 2 if ple <= 300 else (1 if ple <= 600 else 0)

    if score >= 4:
        return "Attention", "bad"
    if score >= 2:
        return "Watch", "warn"
    return "Healthy", "ok"


def _kpi_class_for_pct(v, warn_at, bad_at):
    if not isinstance(v, (int, float)):
        return "ok"
    if v >= bad_at:
        return "bad"
    if v >= warn_at:
        return "warn"
    return "ok"


def _kpi_class_for_leq(v, warn_at, bad_at):
    if not isinstance(v, (int, float)):
        return "ok"
    if v <= bad_at:
        return "bad"
    if v <= warn_at:
        return "warn"
    return "ok"


def _kpi_class_for_int_geq(v, warn_at, bad_at):
    try:
        vv = int(float(v))
    except Exception:
        return "ok"
    if vv >= bad_at:
        return "bad"
    if vv >= warn_at:
        return "warn"
    return "ok"


def _kpi_tile_html(label, value, hint, klass="ok"):
    return f"""<div class="kpi-card {klass}">
        <div class="kpi-label">{label}</div>
        <div class="kpi-value">{value}</div>
        <div class="kpi-hint">{hint}</div>
    </div>"""


def _build_exec_insight(cpu_pct, mem_pct, ple_s, io_stats: dict):
    signals = []
    risks = []

    if isinstance(cpu_pct, (int, float)):
        if cpu_pct >= 85:
            risks.append("CPU saturation risk (peak CPU ≥ 85%).")
        elif cpu_pct >= 65:
            signals.append("CPU utilization elevated but not critical.")
        else:
            signals.append("CPU headroom looks healthy.")

    if isinstance(mem_pct, (int, float)):
        if mem_pct >= 85:
            risks.append("High memory utilization (peak memory ≥ 85%).")
        elif mem_pct >= 65:
            signals.append("Memory utilization moderately high.")
        else:
            signals.append("Memory utilization within a safe range.")

    if isinstance(ple_s, (int, float)):
        if ple_s <= 300:
            risks.append("Low PLE (≤ 300s) indicates cache churn / memory pressure.")
        elif ple_s <= 600:
            signals.append("PLE borderline—monitor for cache churn.")
        else:
            signals.append("PLE indicates stable buffer cache behavior.")

    rd = io_stats.get("avg_read_latency_ms")
    wr = io_stats.get("avg_write_latency_ms")
    if isinstance(rd, (int, float)):
        if rd >= 20:
            risks.append("Read latency is elevated (avg read ≥ 20ms).")
        elif rd >= 10:
            signals.append("Read latency moderately high.")
    if isinstance(wr, (int, float)):
        if wr >= 20:
            risks.append("Write latency is elevated (avg write ≥ 20ms).")
        elif wr >= 10:
            signals.append("Write latency moderately high.")

    if not signals and not risks:
        return "Performance Insight", "Metrics available, but not enough signals to summarize confidently."

    parts = []
    if risks:
        parts.append("⚠ " + " ".join(risks))
    if signals:
        parts.append("• " + " ".join(signals))
    return "Performance Insight", " ".join(parts)


def _render_waits_table(waits_df: pd.DataFrame):
    df = waits_df.copy()

    if "wait_pct" in df.columns:
        df["wait_pct"] = pd.to_numeric(df["wait_pct"], errors="coerce").fillna(0.0)
    else:
        df["wait_pct"] = 0.0

    if "avg_wait_s" in df.columns:
        df["avg_wait_ms"] = (pd.to_numeric(df["avg_wait_s"], errors="coerce") * 1000).round(2)
    else:
        df["avg_wait_ms"] = pd.NA

    if "avg_signal_s" in df.columns:
        df["signal_ms"] = (pd.to_numeric(df["avg_signal_s"], errors="coerce") * 1000).round(2)
    else:
        df["signal_ms"] = pd.NA

    df = df.sort_values("wait_pct", ascending=False).head(10).reset_index(drop=True)

    max_pct = float(df["wait_pct"].max()) if len(df) else 0.0
    max_pct = max(max_pct, 1.0)

    rows_html = []
    for i, r in df.iterrows():
        wt = str(r["wait_type"]) if "wait_type" in df.columns else f"WAIT_{i + 1}"
        pct = float(r["wait_pct"]) if pd.notna(r["wait_pct"]) else 0.0
        wms = r["avg_wait_ms"]
        sms = r["signal_ms"]

        width = int(round((pct / max_pct) * 100))
        is_top = i == 0

        wt_class = "wait-type top" if is_top else "wait-type"
        top_badge = "<span class='badge-mini'>Top</span>" if is_top else ""

        wms_txt = f"{float(wms):.2f}" if pd.notna(wms) else "—"
        sms_txt = f"{float(sms):.2f}" if pd.notna(sms) else "—"

        rows_html.append(
            f"""<tr>
                <td><span class="{wt_class}">{wt}</span>{top_badge}</td>
                <td style="width:34%">
                    <div class="bar-wrap"><div class="bar-fill" style="width:{width}%"></div></div>
                </td>
                <td style="text-align:right">{pct:.2f}%</td>
                <td style="text-align:right">{wms_txt}</td>
                <td style="text-align:right">{sms_txt}</td>
            </tr>"""
        )

    table = f"""
<div style="overflow-x:auto;">
<table class="waits-table">
  <thead>
    <tr>
      <th>Wait Type</th>
      <th>Contribution</th>
      <th style="text-align:right">Wait %</th>
      <th style="text-align:right">Avg Wait (ms)</th>
      <th style="text-align:right">Signal (ms)</th>
    </tr>
  </thead>
  <tbody>
    {''.join(rows_html)}
  </tbody>
</table>
</div>
"""
    st.markdown(table, unsafe_allow_html=True)


def render_overview(selected_server: str, selected_ingestion_date: str | None):
    st.markdown(_CSS, unsafe_allow_html=True)

    cache = st.session_state.setdefault("_overview_profile_cache", {})
    cache_key = (selected_server, selected_ingestion_date)

    if cache_key in cache:
        profile = cache[cache_key]
    else:
        with st.spinner("Loading server snapshot"):
            profile = build_server_profile(
                selected_server,
                selected_ingestion_date,
            )
        cache[cache_key] = profile

    instance = profile.get("instance") or {}
    util = profile.get("utilization") or {}
    pressure = profile.get("pressure") or {}
    conf = profile.get("configuration") or {}
    workload = profile.get("workload") or {}
    io_stats = profile.get("io_stats") or {}
    waits_df = profile.get("waits_df")
    snapshot = profile.get("snapshot") or selected_ingestion_date or "—"

    sql_banner = instance.get("sql_banner") or "SQL Server"
    edition = instance.get("edition") or "—"
    cpu_count = instance.get("cpu_count")
    ram_mb = instance.get("total_ram_mb")
    os_name = instance.get("os_name") or "—"

    safe_sql_banner = escape(str(sql_banner))
    safe_edition = escape(str(edition))
    safe_server = escape(str(selected_server))
    safe_snapshot = escape(str(snapshot))
    safe_os_name = escape(str(os_name))

    cpu_pct = util.get("max_cpu_pct")
    mem_pct = util.get("max_memory_pct")
    ple_s = util.get("cache_ple_seconds")
    grants_pending = pressure.get("memory_grants_pending")

    health_label, health_class = _health(cpu_pct, mem_pct, ple_s)

    cpu_str = f"{int(cpu_count)} cores" if isinstance(cpu_count, (int, float)) else "—"
    ram_str = f"{int(ram_mb / 1024)} GB RAM" if isinstance(ram_mb, (int, float)) else "—"

    st.markdown('<div class="overview-shell">', unsafe_allow_html=True)
    st.markdown('<div class="overview-page-title">Overview</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="overview-page-subtitle">Snapshot-level server posture, workload pressure, waits, I/O signals, and configuration indicators for the active scope.</div>',
        unsafe_allow_html=True,
    )

    st.markdown(
        f"""
<div class="overview-hero">
  <div class="overview-hero-row">
    <div>
      <div class="overview-hero-title">{safe_sql_banner} • {safe_edition}</div>
      <div class="overview-hero-sub">High-level posture for <code>{safe_server}</code> across the active ingestion snapshot.</div>
    </div>
    <div>
      <span class="health-pill {health_class}">{health_label}</span>
    </div>
  </div>
  <div class="overview-meta-grid">
    <div class="overview-meta-card">
      <div class="overview-meta-label">Server</div>
      <div class="overview-meta-value"><code>{safe_server}</code></div>
    </div>
    <div class="overview-meta-card">
      <div class="overview-meta-label">Snapshot</div>
      <div class="overview-meta-value"><code>{safe_snapshot}</code></div>
    </div>
    <div class="overview-meta-card">
      <div class="overview-meta-label">CPU</div>
      <div class="overview-meta-value">{cpu_str}</div>
    </div>
    <div class="overview-meta-card">
      <div class="overview-meta-label">Memory</div>
      <div class="overview-meta-value">{ram_str}</div>
    </div>
    <div class="overview-meta-card">
      <div class="overview-meta-label">Operating System</div>
      <div class="overview-meta-value">{safe_os_name}</div>
    </div>
    <div class="overview-meta-card">
      <div class="overview-meta-label">Peak CPU</div>
      <div class="overview-meta-value">{_fmt_pct(cpu_pct)}</div>
    </div>
    <div class="overview-meta-card">
      <div class="overview-meta-label">Peak Memory</div>
      <div class="overview-meta-value">{_fmt_pct(mem_pct)}</div>
    </div>
    <div class="overview-meta-card">
      <div class="overview-meta-label">PLE</div>
      <div class="overview-meta-value">{_fmt_s(ple_s)}</div>
    </div>
  </div>
</div>
        """,
        unsafe_allow_html=True,
    )

    insight_title, insight_text = _build_exec_insight(cpu_pct, mem_pct, ple_s, io_stats)
    st.markdown(
        f"""
<div class="overview-insight">
  <div class="overview-insight-title">{insight_title}</div>
  <div class="overview-insight-text">{insight_text}</div>
</div>
        """,
        unsafe_allow_html=True,
    )

    cpu_class = _kpi_class_for_pct(cpu_pct, warn_at=65, bad_at=85)
    mem_class = _kpi_class_for_pct(mem_pct, warn_at=65, bad_at=85)
    ple_class = _kpi_class_for_leq(ple_s, warn_at=600, bad_at=300)
    grants_class = _kpi_class_for_int_geq(grants_pending, warn_at=1, bad_at=5)

    cpu_hint = "CPU headroom OK" if cpu_class == "ok" else ("Elevated CPU load" if cpu_class == "warn" else "CPU at risk")
    mem_hint = "Stable memory use" if mem_class == "ok" else ("Memory trending high" if mem_class == "warn" else "Memory pressure risk")
    ple_hint = "Healthy cache" if ple_class == "ok" else ("Borderline cache churn" if ple_class == "warn" else "Low PLE")
    gp_hint = "No pressure" if grants_class == "ok" else ("Monitor grants" if grants_class == "warn" else "Grant backlog")

    k1, k2, k3, k4 = st.columns(4)
    with k1:
        st.markdown(_kpi_tile_html("Max CPU", _fmt_pct(cpu_pct), cpu_hint, cpu_class), unsafe_allow_html=True)
    with k2:
        st.markdown(_kpi_tile_html("Max Memory", _fmt_pct(mem_pct), mem_hint, mem_class), unsafe_allow_html=True)
    with k3:
        st.markdown(_kpi_tile_html("PLE", _fmt_s(ple_s), ple_hint, ple_class), unsafe_allow_html=True)
    with k4:
        st.markdown(_kpi_tile_html("Grants Pending", _fmt_int(grants_pending), gp_hint, grants_class), unsafe_allow_html=True)

    st.markdown('<div class="section-title">Performance & Bottlenecks</div>', unsafe_allow_html=True)
    col_a, col_b = st.columns([1.1, 1.0], gap="medium")

    with col_a:
        st.markdown('<div class="metric-panel">', unsafe_allow_html=True)
        st.markdown('<div class="metric-block-title">Workload (Top Queries)</div>', unsafe_allow_html=True)
        w1, w2, w3 = st.columns(3)
        w1.metric("Top Queries", _fmt_int(workload.get("top_query_count")))
        w2.metric(
            "Max Query",
            f"{workload.get('max_duration_s'):.1f}s"
            if isinstance(workload.get("max_duration_s"), (int, float))
            else "—",
        )
        w3.metric("Max Reads", _fmt_int(workload.get("max_logical_reads")))
        st.markdown("</div>", unsafe_allow_html=True)

        st.markdown("<div style='height:12px'></div>", unsafe_allow_html=True)

        st.markdown('<div class="metric-panel">', unsafe_allow_html=True)
        st.markdown('<div class="metric-block-title">Waits Breakdown</div>', unsafe_allow_html=True)
        if isinstance(waits_df, pd.DataFrame) and not waits_df.empty:
            _render_waits_table(waits_df)
        else:
            st.caption("No wait statistics available for this snapshot.")
        st.markdown("</div>", unsafe_allow_html=True)

    with col_b:
        st.markdown('<div class="metric-panel">', unsafe_allow_html=True)
        st.markdown('<div class="metric-block-title">I/O Stats</div>', unsafe_allow_html=True)

        i1, i2 = st.columns(2)
        i1.metric(
            "Avg Read Lat (ms)",
            f"{io_stats.get('avg_read_latency_ms'):.1f}"
            if isinstance(io_stats.get("avg_read_latency_ms"), (int, float))
            else "—",
        )
        i2.metric(
            "Avg Write Lat (ms)",
            f"{io_stats.get('avg_write_latency_ms'):.1f}"
            if isinstance(io_stats.get("avg_write_latency_ms"), (int, float))
            else "—",
        )

        i3, i4 = st.columns(2)
        i3.metric(
            "Drive Max Lat (ms)",
            f"{io_stats.get('drive_max_overall_latency_ms'):.1f}"
            if isinstance(io_stats.get("drive_max_overall_latency_ms"), (int, float))
            else "—",
        )

        total_mb = io_stats.get("total_io_mb")
        if isinstance(total_mb, (int, float)):
            if total_mb >= 1_000_000:
                total_val = f"{total_mb / 1_000_000:.1f} TB"
            elif total_mb >= 1_000:
                total_val = f"{total_mb / 1_000:.1f} GB"
            else:
                total_val = f"{int(total_mb)} MB"
        else:
            total_val = "—"

        i4.metric("Total I/O", total_val)
        st.markdown("</div>", unsafe_allow_html=True)

        st.markdown("<div style='height:12px'></div>", unsafe_allow_html=True)

        st.markdown('<div class="metric-panel">', unsafe_allow_html=True)
        st.markdown('<div class="metric-block-title">Configuration</div>', unsafe_allow_html=True)
        c1, c2, c3 = st.columns(3)
        c1.metric("MaxDOP", _fmt_int(conf.get("maxdop")))
        c2.metric("Cost Th.", _fmt_int(conf.get("cost_threshold")))
        c3.metric("Max Mem", _mb_to_gb(conf.get("max_server_memory_mb")))
        st.markdown("</div>", unsafe_allow_html=True)

    st.markdown("</div>", unsafe_allow_html=True)
