from __future__ import annotations

from typing import Optional

import pandas as pd
import streamlit as st

from services.expensive_queries_service import (
    list_expensive_query_types,
    fetch_latest_expensive_queries,
    pick_query_text_column,
    pick_sort_metric_column,
    build_query_dropdown_items,
    analyze_expensive_query_with_llm,
    answer_expensive_query_followup_with_llm,
    QueryTypeOption,
)

_TAB_CSS = """
<style>
:root {
  --eq-border: rgba(15, 23, 42, 0.10);
  --eq-bg: rgba(255, 255, 255, 0.88);
  --eq-muted: rgba(15, 23, 42, 0.66);
  --eq-soft: rgba(15, 23, 42, 0.50);
  --eq-title: #0f172a;
  --eq-accent: #1f6feb;
  --eq-accent-2: #2f81f7;
  --eq-success: #15803d;
  --eq-warning-bg: rgba(245, 158, 11, 0.10);
  --eq-warning-border: rgba(245, 158, 11, 0.18);
  --eq-warning-text: #92400e;
  --eq-info-bg: rgba(59, 130, 246, 0.08);
  --eq-info-border: rgba(59, 130, 246, 0.16);
  --eq-info-text: #1e3a8a;
  --eq-success-bg: rgba(34, 197, 94, 0.08);
  --eq-success-border: rgba(34, 197, 94, 0.16);
  --eq-success-text: #166534;
  --eq-radius-lg: 18px;
  --eq-radius-md: 14px;
  --eq-radius-sm: 12px;
  --eq-shadow-sm: 0 2px 10px rgba(15, 23, 42, 0.05);
  --eq-shadow-md: 0 12px 32px rgba(15, 23, 42, 0.08);
}


.eq-page-title {
  font-size: 2rem;
  font-weight: 780;
  letter-spacing: -0.02em;
  color: var(--eq-title);
  margin: 0 0 0.3rem 0;
}

.eq-page-subtitle {
  font-size: 0.98rem;
  color: var(--eq-muted);
  margin: 0 0 1rem 0;
  line-height: 1.5;
}

.eq-hero {
  border: 1px solid var(--eq-border);
  border-radius: var(--eq-radius-lg);
  background: linear-gradient(180deg, rgba(255,255,255,0.96), rgba(248,250,252,0.92));
  box-shadow: var(--eq-shadow-sm);
  padding: 20px 22px;
  margin-bottom: 16px;
}

.eq-hero-top {
  display: flex;
  justify-content: space-between;
  align-items: flex-start;
  gap: 16px;
  flex-wrap: wrap;
}

.eq-hero-title {
  font-size: 1.28rem;
  font-weight: 760;
  color: var(--eq-title);
  margin: 0 0 6px 0;
}

.eq-hero-sub {
  font-size: 0.94rem;
  color: var(--eq-muted);
  line-height: 1.55;
  margin: 0;
}

.eq-pill {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  border-radius: 999px;
  padding: 6px 12px;
  font-size: 0.82rem;
  font-weight: 680;
  border: 1px solid rgba(0,0,0,0.10);
  white-space: nowrap;
}

.eq-pill-info {
  background: rgba(59, 130, 246, 0.10);
  color: #1d4ed8;
}

.eq-pill-warn {
  background: rgba(245, 158, 11, 0.14);
  color: #92400e;
}

.eq-section {
  border: 1px solid var(--eq-border);
  border-radius: var(--eq-radius-lg);
  background: var(--eq-bg);
  box-shadow: var(--eq-shadow-sm);
  padding: 18px;
  margin-bottom: 16px;
}

.eq-section-title {
  font-size: 1.02rem;
  font-weight: 730;
  color: var(--eq-title);
  margin: 18px 0 10px 0;
}

.eq-meta-grid {
  display: grid;
  grid-template-columns: repeat(4, minmax(160px, 1fr));
  gap: 10px;
}

.eq-meta-card {
  border: 1px solid var(--eq-border);
  border-radius: var(--eq-radius-sm);
  background: rgba(255,255,255,0.82);
  padding: 11px 12px;
}

.eq-meta-label {
  font-size: 0.76rem;
  text-transform: uppercase;
  letter-spacing: 0.04em;
  color: var(--eq-soft);
  margin-bottom: 4px;
}

.eq-meta-value {
  font-size: 0.95rem;
  font-weight: 670;
  color: var(--eq-title);
  line-height: 1.35;
}

.eq-selection-card {
  border: 1px solid var(--eq-border);
  border-radius: var(--eq-radius-md);
  background: linear-gradient(180deg, rgba(255,255,255,0.96), rgba(248,250,252,0.90));
  padding: 16px;
  margin-bottom: 14px;
}

.eq-selection-title {
  font-size: 1rem;
  font-weight: 720;
  color: var(--eq-title);
  margin-bottom: 6px;
}

.eq-selection-sub {
  font-size: 0.9rem;
  color: var(--eq-muted);
  line-height: 1.5;
  margin-bottom: 12px;
}

.eq-ai-card {
  border: 1px solid var(--eq-info-border);
  border-radius: var(--eq-radius-md);
  background: linear-gradient(180deg, rgba(239,246,255,0.82), rgba(255,255,255,0.92));
  padding: 16px;
  margin: 14px 0;
}

.eq-ai-title {
  font-size: 1rem;
  font-weight: 740;
  color: var(--eq-title);
  margin-bottom: 5px;
}

.eq-ai-sub {
  font-size: 0.9rem;
  color: var(--eq-muted);
  line-height: 1.5;
  margin-bottom: 12px;
}

@media (max-width: 1100px) {
  .eq-meta-grid {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }
}

@media (max-width: 760px) {
  .eq-meta-grid {
    grid-template-columns: 1fr;
  }

  .eq-page-title {
    font-size: 1.6rem;
  }
}
</style>
"""


def _to_number(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def render_expensive_queries_tab(selected_server: str, selected_ingestion_date: str | None) -> None:
    st.markdown(_TAB_CSS, unsafe_allow_html=True)

    st.markdown('<div class="eq-page-title">Most Expensive Queries</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="eq-page-subtitle">Inspect high-cost query patterns, review row-level diagnostics, and export or validate the captured evidence for a selected statement.</div>',
        unsafe_allow_html=True,
    )

    if not selected_server:
        st.info("Select a server to view expensive queries.")
        return

    options = list_expensive_query_types(selected_server)
    if not options:
        st.warning("No 'Top …' expensive query sheets were found for this server in the bronze table.")
        return

    st.markdown(
        f"""
<div class="eq-hero">
  <div class="eq-hero-top">
    <div>
      <div class="eq-hero-title">Expensive query analysis workspace</div>
      <p class="eq-hero-sub">
        Select a query bucket, isolate a single expensive statement, and inspect its observed metrics and SQL text from the captured diagnostics.
      </p>
    </div>
    <div>
      <span class="eq-pill eq-pill-info">Server: {selected_server}</span>
    </div>
  </div>
</div>
        """,
        unsafe_allow_html=True,
    )

    st.markdown('<div class="eq-section-title">Selection controls</div>', unsafe_allow_html=True)

    label_to_opt = {o.label: o for o in options}

    c1, c2 = st.columns([1.1, 1.4], gap="large")
    with c1:
        type_label = st.selectbox(
            "Query Type",
            [o.label for o in options],
            key=f"exp_q_type::{selected_server}",
        )
    opt: QueryTypeOption = label_to_opt[type_label]

    with c2:
        query_search = st.text_input(
            "Filter query list",
            placeholder="Filter by text snippet in dropdown label…",
        )

    with st.spinner("Loading expensive queries from Delta…"):
        df, snap = fetch_latest_expensive_queries(
            selected_server,
            opt.sheet_name,
            selected_ingestion_date,
        )

    if df.empty:
        st.warning(f"No rows found for sheet '{opt.sheet_name}' (latest snapshot: {snap or 'unknown'}).")
        return

    metric_col = pick_sort_metric_column(df, opt.kind)
    if metric_col and metric_col in df.columns:
        df = df.copy()
        df["_sort_metric"] = _to_number(df[metric_col])
        df = df.sort_values(by="_sort_metric", ascending=False, na_position="last").drop(columns=["_sort_metric"])
    else:
        metric_col = None

    query_col = pick_query_text_column(df)
    if not query_col:
        st.error(
            "I couldn't find a query text column in this sheet. Expected something like 'Short Query Text' or 'Query Text'."
        )
        st.caption(f"Detected columns: {', '.join(list(df.columns)[:40])}{' …' if len(df.columns) > 40 else ''}")
        return

    items = build_query_dropdown_items(df, query_col=query_col, limit=200)
    if not items:
        st.warning("No query text rows were found for this sheet.")
        return

    filtered_items = [x for x in items if query_search.lower() in x.lower()] if query_search else items
    if not filtered_items:
        st.warning("No query dropdown items matched your filter text.")
        return

    sel_item = st.selectbox(
        "Select Query",
        filtered_items,
        key=f"exp_q_query::{selected_server}::{opt.sheet_name}",
    )

    try:
        sel_idx = int(sel_item.split("—", 1)[0].strip()) - 1
    except Exception:
        sel_idx = 0

    sel_idx = max(0, min(sel_idx, len(df) - 1))
    row = df.iloc[sel_idx].to_dict()
    query_text = str(df.iloc[sel_idx][query_col]) if query_col in df.columns else ""

    sort_value = row.get(metric_col) if metric_col else None
    sort_value_str = str(sort_value) if sort_value is not None else "—"

    st.markdown('<div class="eq-section-title">Selected query summary</div>', unsafe_allow_html=True)
    st.markdown(
        f"""
<div class="eq-selection-card">
  <div class="eq-selection-title">{opt.label}</div>
  <div class="eq-selection-sub">
    Reviewing row <code>{sel_idx + 1}</code> from <code>{opt.sheet_name}</code>. Use the diagnostics below to validate whether the row selection and sort metric align with your diagnostic investigation.
  </div>
  <div class="eq-meta-grid">
    <div class="eq-meta-card">
      <div class="eq-meta-label">Server</div>
      <div class="eq-meta-value"><code>{selected_server}</code></div>
    </div>
    <div class="eq-meta-card">
      <div class="eq-meta-label">Snapshot</div>
      <div class="eq-meta-value"><code>{snap or 'unknown'}</code></div>
    </div>
    <div class="eq-meta-card">
      <div class="eq-meta-label">Sheet</div>
      <div class="eq-meta-value">{opt.sheet_name}</div>
    </div>
    <div class="eq-meta-card">
      <div class="eq-meta-label">Sort Metric</div>
      <div class="eq-meta-value">{metric_col or 'Not detected'}</div>
    </div>
    <div class="eq-meta-card">
      <div class="eq-meta-label">Metric Value</div>
      <div class="eq-meta-value">{sort_value_str}</div>
    </div>
    <div class="eq-meta-card">
      <div class="eq-meta-label">Query Text Column</div>
      <div class="eq-meta-value">{query_col}</div>
    </div>
    <div class="eq-meta-card">
      <div class="eq-meta-label">Selection Index</div>
      <div class="eq-meta-value">{sel_idx + 1}</div>
    </div>
    <div class="eq-meta-card">
      <div class="eq-meta-label">Diagnostics State</div>
      <div class="eq-meta-value">Loaded</div>
    </div>
  </div>
</div>
        """,
        unsafe_allow_html=True,
    )

    important_cols = [
        c for c in [
            "Database Name",
            query_col,
            "Execution Count",
            "Total Worker Time",
            "Avg Worker Time",
            "Total Logical Reads",
            "Avg Logical Reads",
            "Avg Elapsed Time",
            "Total Elapsed Time",
            "Has Missing Index",
        ]
        if c in df.columns
    ]

    st.markdown('<div style="height:10px"></div>', unsafe_allow_html=True)

    if important_cols:
        st.dataframe(pd.DataFrame([row])[important_cols], use_container_width=True, hide_index=True)
    else:
        st.dataframe(pd.DataFrame([row]), use_container_width=True, hide_index=True)

    if query_text and query_text.strip().lower() != "nan":
        with st.expander("View selected SQL text", expanded=False):
            st.code(query_text, language="sql")

    with st.expander("Show full Top list for this Query Type", expanded=False):
        hide_cols = {"sheet_name"}
        display_df = df[[c for c in df.columns if c not in hide_cols]].head(50)
        st.dataframe(display_df, use_container_width=True, hide_index=True)


    analysis_key = (
        f"exp_q_analysis::{selected_server}::{selected_ingestion_date or 'latest'}::"
        f"{opt.sheet_name}::{sel_idx}"
    )
    followup_key = f"exp_q_followup::{analysis_key}"

    st.markdown('<div class="eq-section-title">Agent query analysis</div>', unsafe_allow_html=True)
    st.markdown(
        """
<div class="eq-ai-card">
  <div class="eq-ai-title">Analyze this query</div>
  <div class="eq-ai-sub">
    Generate an evidence-grounded tuning readout for the selected expensive-query row, including likely drivers, a best-fit optimized query rewrite, SQL Server 2022+/Azure SQL rationale, recommended actions, and follow-up diagnostics.
  </div>
</div>
        """,
        unsafe_allow_html=True,
    )

    ai_col, clear_col, _ = st.columns([1.35, 0.95, 2.7], gap="small")
    with ai_col:
        analyze_clicked = st.button(
            "Analyze this query",
            key=f"exp_q_analyze_btn::{analysis_key}",
            use_container_width=True,
        )
    with clear_col:
        clear_clicked = st.button(
            "Clear analysis",
            key=f"exp_q_clear_btn::{analysis_key}",
            use_container_width=True,
            disabled=analysis_key not in st.session_state,
        )

    if clear_clicked:
        st.session_state.pop(analysis_key, None)
        st.session_state.pop(followup_key, None)
        st.rerun()

    if analyze_clicked:
        with st.spinner("Analyzing selected query with SQL Server Observability Agent…"):
            try:
                st.session_state[analysis_key] = analyze_expensive_query_with_llm(
                    server_name=selected_server,
                    ingestion_date=selected_ingestion_date,
                    snapshot=snap,
                    query_type_label=opt.label,
                    sheet_name=opt.sheet_name,
                    query_kind=opt.kind,
                    query_text=query_text,
                    row=row,
                    metric_col=metric_col,
                    sort_value=sort_value,
                    row_index=sel_idx,
                    total_rows=len(df),
                )
                st.session_state.pop(followup_key, None)
            except Exception as e:
                st.session_state[analysis_key] = f"Agent query analysis failed: {e}"

    analysis_md = st.session_state.get(analysis_key)
    if analysis_md:
        st.markdown(analysis_md)

        with st.form(key=f"exp_q_followup_form::{analysis_key}", clear_on_submit=True):
            followup_question = st.text_input(
                "Ask a follow-up about this query",
                placeholder="Example: What should I check first in the actual execution plan?",
            )
            followup_submitted = st.form_submit_button("Ask follow-up")

        if followup_submitted and followup_question.strip():
            with st.spinner("Answering follow-up…"):
                try:
                    st.session_state[followup_key] = answer_expensive_query_followup_with_llm(
                        server_name=selected_server,
                        ingestion_date=selected_ingestion_date,
                        snapshot=snap,
                        query_type_label=opt.label,
                        sheet_name=opt.sheet_name,
                        query_kind=opt.kind,
                        query_text=query_text,
                        row=row,
                        metric_col=metric_col,
                        sort_value=sort_value,
                        row_index=sel_idx,
                        total_rows=len(df),
                        prior_analysis=str(analysis_md),
                        question=followup_question.strip(),
                    )
                except Exception as e:
                    st.session_state[followup_key] = f"Agent follow-up failed: {e}"

        followup_md = st.session_state.get(followup_key)
        if followup_md:
            st.markdown("#### Follow-up answer")
            st.markdown(followup_md)
    else:
        st.info("Click **Analyze this query** to generate a tuning-oriented Agent review with an optimized-query recommendation for the selected row.")
