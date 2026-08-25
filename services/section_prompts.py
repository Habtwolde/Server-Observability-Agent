"""
Per-section LLM prompt registry for the SQL Server Health Assessment report.

Prompt methodology:
  Glenn Berry  — DMV-evidence-first; specific metric thresholds; wait-stats
                interpretation based on sys.dm_os_wait_stats categories;
                PLE/buffer-cache pressure analysis; index-health methodology.
  Brent Ozar  — sp_Blitz findings format (Priority / Finding / Details /
                HowToStopIt); sp_BlitzCache query-analysis patterns (Reads,
                CPU, Duration, Executions, Spills); sp_BlitzFirst triage;
                priority-first, evidence-anchored action items.
  Ola Hallengren — maintenance safety: validate backup/integrity posture,
                statistics freshness, and index-maintenance suitability before
                broad query/index remediation.
  Microsoft   — Query Store and execution-plan validation; controlled
                before/after measurement and no operator-level claims without
                plan evidence.

Each section:
  - receives only the evidence keys it needs (minimises token waste)
  - returns a JSON object with exactly the narrative_keys it owns
  - can be overridden by the user in the Streamlit UI before generation
"""
from __future__ import annotations

import json
from typing import Any, Dict, List, Optional


# ---------------------------------------------------------------------------
# Evidence selectors — pass only what each section needs
# ---------------------------------------------------------------------------
def _pick(evidence: Dict[str, Any], *keys: str) -> Dict[str, Any]:
    return {k: evidence[k] for k in keys if k in evidence}


def _compact_waits(waits: List[Dict]) -> List[Dict]:
    return [
        {
            "wait_type": w.get("wait_type"),
            "pct": w.get("pct"),
            "interpretation": w.get("interpretation"),
        }
        for w in (waits or [])[:8]
    ]


def _compact_hotspots(hotspots: List[Dict]) -> List[Dict]:
    return [
        {
            "object_name": h.get("object_name"),
            "database_name": h.get("database_name"),
            "metric_name": h.get("metric_name"),
            "metric_value": h.get("metric_value"),
            "query_hash": h.get("query_hash"),
            "plan_hash": h.get("plan_hash"),
            "metrics": h.get("metrics") if isinstance(h.get("metrics"), dict) else {},
        }
        for h in (hotspots or [])[:8]
    ]


def _compact_query_analysis(query_analysis: Dict[str, Any]) -> Dict[str, Any]:
    """Trim deterministic query-analysis evidence for LLM section prompts."""
    qa = query_analysis or {}
    compact_hotspots: List[Dict[str, Any]] = []
    for item in (qa.get("hotspots") or [])[:8]:
        cls = item.get("classification") or {}
        diag = item.get("diagnosis") or {}
        recs = item.get("recommendations") or []
        compact_hotspots.append(
            {
                "rank": item.get("rank"),
                "database_name": item.get("database_name"),
                "object_name": item.get("object_name"),
                "query_hash": item.get("query_hash"),
                "plan_hash": item.get("plan_hash"),
                "metrics": item.get("metrics") or {},
                "classification": {
                    "primary_dimension": cls.get("primary_dimension"),
                    "patterns": (cls.get("patterns") or [])[:8],
                    "severity": cls.get("severity"),
                    "confidence": cls.get("confidence"),
                },
                "diagnosis": {
                    "evidence": (diag.get("evidence") or [])[:6],
                    "likely_causes": (diag.get("likely_causes") or [])[:4],
                    "what_not_to_assume": (diag.get("what_not_to_assume") or [])[:2],
                },
                "top_recommendations": [
                    {
                        "owner": r.get("owner"),
                        "methodology": r.get("methodology"),
                        "action": r.get("action"),
                        "validation": r.get("validation"),
                    }
                    for r in recs[:3]
                ],
            }
        )
    return {
        "summary": qa.get("summary") or {},
        "hotspots": compact_hotspots,
        "methodology_matrix": (qa.get("methodology_matrix") or [])[:4],
    }


def _compact_windows(win: Dict) -> Dict:
    return {
        "alerts_total": win.get("alerts_total", 0),
        "alerts_error": win.get("alerts_error", 0),
        "alerts_warning": win.get("alerts_warning", 0),
        "top_providers": (win.get("top_providers") or [])[:3],
        "top_event_ids": (win.get("top_event_ids") or [])[:3],
        "timeline_cues": (win.get("timeline_cues") or [])[:4],
        "scope_mode": win.get("scope_mode"),
    }


def _select_evidence(section_key: str, evidence: Dict[str, Any]) -> Dict[str, Any]:
    """Return a trimmed evidence dict for the given section."""
    base = {
        "server_name": evidence.get("server_name"),
        "snapshot": evidence.get("snapshot"),
        "snapshot_display": evidence.get("snapshot_display"),
    }
    w = evidence.get("windows_events") or {}
    waits = evidence.get("waits") or []
    hotspots = evidence.get("hotspots") or []
    query_analysis = evidence.get("query_analysis") or {}

    selectors: Dict[str, Dict[str, Any]] = {
        "introduction": {
            **base,
            "instance": evidence.get("instance") or {},
            "notes": evidence.get("notes") or [],
            "windows_events_count": int(w.get("alerts_total") or 0),
        },
        "executive_summary": {
            **base,
            "utilization": evidence.get("utilization") or {},
            "configuration": evidence.get("configuration") or {},
            "database_settings": evidence.get("database_settings") or {},
            "waits": _compact_waits(waits),
            "hotspots": _compact_hotspots(hotspots),
            "query_analysis": _compact_query_analysis(query_analysis),
            "windows_events": _compact_windows(w),
            "backup": evidence.get("backup") or {},
        },
        "environment_overview": {
            **base,
            "instance": evidence.get("instance") or {},
            "configuration": evidence.get("configuration") or {},
            "database_settings": evidence.get("database_settings") or {},
            "tempdb": evidence.get("tempdb") or {},
        },
        "performance_characteristics": {
            **base,
            "utilization": evidence.get("utilization") or {},
            "waits": _compact_waits(waits),
            "windows_events": _compact_windows(w),
            "tempdb": evidence.get("tempdb") or {},
        },
        "query_hotspots": {
            **base,
            "hotspots": _compact_hotspots(hotspots),
            "query_analysis": _compact_query_analysis(query_analysis),
            "waits": _compact_waits(waits[:3]),
        },
        "key_findings": {
            **base,
            "utilization": evidence.get("utilization") or {},
            "configuration": evidence.get("configuration") or {},
            "database_settings": evidence.get("database_settings") or {},
            "waits": _compact_waits(waits),
            "hotspots": _compact_hotspots(hotspots),
            "query_analysis": _compact_query_analysis(query_analysis),
            "windows_events": _compact_windows(w),
            "backup": evidence.get("backup") or {},
            "tempdb": evidence.get("tempdb") or {},
        },
        "action_plan": {
            **base,
            "waits": _compact_waits(waits[:4]),
            "hotspots": _compact_hotspots(hotspots[:4]),
            "query_analysis": _compact_query_analysis(query_analysis),
            "configuration": evidence.get("configuration") or {},
            "database_settings": evidence.get("database_settings") or {},
        },
        "developer_action_plan": {
            **base,
            "hotspots": _compact_hotspots(hotspots),
            "query_analysis": _compact_query_analysis(query_analysis),
            "waits": _compact_waits(waits[:5]),
            "configuration": evidence.get("configuration") or {},
        },
        "dba_action_plan": {
            **base,
            "configuration": evidence.get("configuration") or {},
            "database_settings": evidence.get("database_settings") or {},
            "backup": evidence.get("backup") or {},
            "tempdb": evidence.get("tempdb") or {},
            "instance": evidence.get("instance") or {},
            "query_analysis": _compact_query_analysis(query_analysis),
        },
        "resource_optimization": {
            **base,
            "utilization": evidence.get("utilization") or {},
            "instance": evidence.get("instance") or {},
            "configuration": evidence.get("configuration") or {},
        },
        "kpis": {
            **base,
            "waits": _compact_waits(waits[:3]),
            "hotspots": _compact_hotspots(hotspots[:3]),
            "query_analysis": _compact_query_analysis(query_analysis),
            "windows_events": _compact_windows(w),
            "utilization": evidence.get("utilization") or {},
        },
        "conclusion": {
            **base,
            "utilization": evidence.get("utilization") or {},
            "waits": _compact_waits(waits[:3]),
            "hotspots": _compact_hotspots(hotspots[:3]),
            "query_analysis": _compact_query_analysis(query_analysis),
            "configuration": evidence.get("configuration") or {},
            "database_settings": evidence.get("database_settings") or {},
            "windows_events": _compact_windows(w),
        },
        "appendix_references": {
            **base,
            "instance": evidence.get("instance") or {},
        },
        "appendix_followups": {
            **base,
            "waits": _compact_waits(waits),
            "hotspots": _compact_hotspots(hotspots),
            "query_analysis": _compact_query_analysis(query_analysis),
            "configuration": evidence.get("configuration") or {},
            "utilization": evidence.get("utilization") or {},
            "windows_events": _compact_windows(w),
            "tempdb": evidence.get("tempdb") or {},
        },
    }
    return selectors.get(section_key, base)


# ---------------------------------------------------------------------------
# Section definitions
# ---------------------------------------------------------------------------
SECTION_DEFINITIONS: List[Dict[str, Any]] = [

    # -----------------------------------------------------------------------
    # 1. Introduction and Scope
    # -----------------------------------------------------------------------
    {
        "key": "introduction",
        "display_name": "1. Introduction and Scope",
        "narrative_keys": ["introduction_paragraph"],
        "output_schema": {
            "introduction_paragraph": "string — 2-3 sentences that state what server is being assessed, the snapshot date, the nature of DMV-based evidence (point-in-time vs cumulative), and explicit scope limitations."
        },
        "system_prompt": """\
You are a senior SQL Server consultant writing the Introduction section of a formal health assessment.

Methodology: Glenn Berry (point-in-time DMV snapshot interpretation) + Brent Ozar (sp_BlitzFirst triage framing).

Rules:
- Explicitly state that sys.dm_os_wait_stats and sys.dm_exec_query_stats data is cumulative from the last SQL Server restart unless counters were reset; delta sampling across representative load windows is required to confirm trends.
- State the server name, snapshot date, and the scope of evidence (waits, configuration, hotspot queries, Windows events).
- Be factual and concise — 2 to 3 sentences maximum for introduction_paragraph.
- Do NOT fabricate any server names, dates, or metrics not present in the provided evidence.
- Use professional evidence-bound language where evidence is partial: "the snapshot evidence indicates", "this requires validation through delta sampling", or "the evidence is consistent with". Do not use weak hedging such as "may be", "maybe", "seems", "appears to be", or "possibly".

Return ONLY valid JSON matching the output_schema. No prose outside the JSON block.
""",
    },

    # -----------------------------------------------------------------------
    # 2. Executive Summary
    # -----------------------------------------------------------------------
    {
        "key": "executive_summary",
        "display_name": "2. Executive Summary",
        "narrative_keys": ["executive_overall_health", "executive_findings", "immediate_actions"],
        "output_schema": {
            "executive_overall_health": "string — one clear health-status sentence using Glenn Berry PLE thresholds (< 300 s = critical, < 3600 s for large servers = concerning) and CPU saturation signals.",
            "executive_findings": ["string — each is a top-priority finding in Brent Ozar sp_Blitz style: lead with the risk category then the specific evidence signal. 4-6 items."],
            "immediate_actions": ["string — numbered 0-7-day actions, most impactful first. Brent Ozar approach: fix correctness/integrity before performance. 4-6 items."]
        },
        "system_prompt": """\
You are a senior SQL Server consultant writing the Executive Summary section of a formal health assessment.

Methodology:
  Glenn Berry — use specific metric thresholds:
    * PLE (Page Life Expectancy) < 300 s: memory pressure is critical.
    * PLE < 3600 s on servers with > 64 GB RAM: investigate.
    * CPU utilisation > 80 % peak sustained: near-saturation risk.
    * Memory grants pending > 0: active workload memory starvation.
  Brent Ozar — sp_Blitz executive framing:
    * Lead with the most dangerous finding (integrity/recoverability first).
    * Prioritise: (1) Correctness/Integrity → (2) Performance → (3) Operational Maturity → (4) Cost.
    * immediate_actions must be actionable TODAY, not aspirational.

Rules:
- executive_overall_health: one sentence. State whether the server shows acute resource stress and where the primary risk lies.
- executive_findings: 4-6 bullets, each anchored to a specific evidence metric (wait type name, config value, database count, PLE value, etc.).
- immediate_actions: 4-6 numbered items. Order: integrity checks first, then acute performance signals, then configuration drift.
- Do NOT restate raw numbers without interpreting them.
- Do NOT fabricate metrics not present in evidence.

Return ONLY valid JSON matching the output_schema. No prose outside the JSON block.
""",
    },

    # -----------------------------------------------------------------------
    # 3. Environment Overview
    # -----------------------------------------------------------------------
    {
        "key": "environment_overview",
        "display_name": "3. Environment Overview",
        "narrative_keys": ["environment_note"],
        "output_schema": {
            "environment_note": "string — 2-3 sentences interpreting the SQL Server version/edition, hardware configuration, and any configuration drift or MAXDOP/CTFP concerns using Glenn Berry and Brent Ozar sp_Blitz hardening checks."
        },
        "system_prompt": """\
You are a senior SQL Server consultant writing the Environment Overview narrative note.

Methodology:
  Glenn Berry — version-specific feature awareness:
    * SQL Server 2019+ supports CXCONSUMER wait type (benign); distinguish from CXPACKET.
    * SQL Server 2016+ has Query Store; note if it is absent from configuration evidence.
    * Logical CPU count vs NUMA node alignment affects optimal MAXDOP.
  Brent Ozar — sp_Blitz configuration checks:
    * MAXDOP: For OLTP, Glenn Berry recommends MAXDOP = number of physical cores per NUMA node, capped at 8. Brent Ozar often recommends starting at 4 for most workloads and tuning from there.
    * Cost Threshold for Parallelism: default 5 is almost always too low; 50-100+ is common for modern servers.
    * Max Server Memory: must leave OS + agent headroom; only call it unset when observed value is 2147483647 MB. If any other value is present, write "review required" rather than "not set".
    * Optimize for Ad-hoc Workloads: enabling reduces plan-cache bloat from single-use plans (sp_Blitz Priority 70).
    * Remote Admin Connections (DAC): should be enabled for emergency access.

Rules:
- Interpret the observed values against the recommended baselines from the evidence.
- If MAXDOP or CTFP diverges from best-practice ranges, name the divergence.
- Note any version-specific limitations on diagnostic evidence availability.
- Do NOT fabricate configuration values not present in evidence.

Return ONLY valid JSON matching the output_schema. No prose outside the JSON block.
""",
    },

    # -----------------------------------------------------------------------
    # 4. Observed Performance Characteristics
    # -----------------------------------------------------------------------
    {
        "key": "performance_characteristics",
        "display_name": "4. Observed Performance Characteristics",
        "narrative_keys": [
            "performance_framing",
            "performance_notes",
            "performance_table_discussion_paragraph",
            "windows_events_summary_paragraph",
            "windows_events_risk_paragraph",
            "windows_metrics_discussion_paragraph",
            "windows_metrics_correlation_paragraph",
            "windows_events_recommendations",
        ],
        "output_schema": {
            "performance_framing": "string — Glenn Berry-style opening: state whether raw CPU/memory indicate acute exhaustion; set up wait-stats as the primary diagnostic lens.",
            "performance_notes": [
                "string — one bullet per notable wait type: name the category (I/O, parallelism, network, lock, latch, memory), cite the specific wait type, and give the Glenn Berry / Brent Ozar interpretation. 3-5 bullets."
            ],
            "performance_table_discussion_paragraph": "string — 2 sentences synthesising the top-3 waits and what they imply about the workload pattern.",
            "windows_events_summary_paragraph": "string — factual summary of Windows events row counts (total, errors, warnings) and scope mode.",
            "windows_events_risk_paragraph": "string — interpret the top provider and event ID as a pattern; assess operational risk.",
            "windows_metrics_discussion_paragraph": "string — discuss provider/event-ID concentration as signal vs noise.",
            "windows_metrics_correlation_paragraph": "string — direct instruction on how to correlate Windows event timeline buckets with SQL wait spikes.",
            "windows_events_recommendations": ["string — 2-4 specific follow-up actions for Windows events."]
        },
        "system_prompt": """\
You are a senior SQL Server consultant writing the Observed Performance Characteristics section.

Methodology:
  Glenn Berry — wait statistics interpretation via sys.dm_os_wait_stats:
    * CXPACKET / CXCONSUMER: parallelism overhead; check MAXDOP and CTFP; note CXCONSUMER is benign in SQL 2016+.
    * PAGEIOLATCH_SH / _EX: storage I/O bottleneck; review read-ahead, index scans, key lookups.
    * LCK_M_*: locking / blocking; review transaction scope, read-committed snapshot isolation (RCSI).
    * PAGELATCH_UP / PAGELATCH_EX: in-memory page latch; TempDB contention is common cause — check file count vs CPU count.
    * RESOURCE_SEMAPHORE: memory grant starvation; queries spilling to TempDB; review MAX_GRANT_PERCENT or index selectivity.
    * SOS_SCHEDULER_YIELD: CPU pressure, non-yielding schedulers; check for tight loops in queries.
    * ASYNC_NETWORK_IO: client-side slow consumption; review result set sizes and row-by-row patterns.
    * WRITELOG: log I/O latency; check log file placement and VLF count.
  Brent Ozar — sp_BlitzFirst wait interpretation:
    * "The server is waiting on ___ the most" framing.
    * Distinguish between task-level and session-level waits.
    * Validate delta sampling recommendation — cumulative waits require controlled delta review before final root-cause conclusions.

  For Windows events:
    * Correlate provider + event ID concentration with SQL-side anomaly windows.
    * A high error count from a specific provider during a narrow time window is a structural issue, not noise.

Rules:
- Name specific wait types from evidence — do not generalise as "various waits".
- For each notable wait type, state the root-cause category (I/O, parallelism, lock, latch, memory, CPU) and the recommended first diagnostic step.
- If CXPACKET appears without CXCONSUMER context, note the SQL version consideration.
- PLE context: if utilization evidence shows PLE < 300, call it critical explicitly.
- Do NOT fabricate wait types not present in evidence.

Return ONLY valid JSON matching the output_schema. No prose outside the JSON block.
""",
    },

    # -----------------------------------------------------------------------
    # 5. Query and Stored Procedure Hotspots
    # -----------------------------------------------------------------------
    {
        "key": "query_hotspots",
        "display_name": "5. Query and Stored Procedure Hotspots",
        "narrative_keys": ["hotspots_framing", "tuning_workflow"],
        "output_schema": {
            "hotspots_framing": "string — evidence-first framing that names the deterministic primary pressure dimension and top classified hotspot(s). Mention Brent Ozar/sp_BlitzCache triage, Glenn Berry/Dr DMV metric interpretation, Ola Hallengren maintenance guardrails, and Microsoft Query Store/execution-plan validation. 3-5 sentences.",
            "tuning_workflow": [
                "string — each step is one numbered action. 6-8 steps covering: baseline from Query Store/DMVs, capture actual plan and IO/TIME, sort by reads/CPU/duration/executions, validate index/statistics candidates, check memory grants/spills and parameter sensitivity only when evidence exists, use Ola-style safe maintenance guardrails, and validate before/after metrics."
            ]
        },
        "system_prompt": """\
You are a senior SQL Server consultant writing the Query and Stored Procedure Hotspots section.

Use the deterministic query_analysis object as the primary evidence source. It already classifies each hotspot by metric dimension, patterns, severity, confidence, evidence strings, likely causes, and methodology-specific recommendations. Explain the evidence; do not invent new findings.

Methodology to apply:
  Brent Ozar — sp_BlitzCache analysis patterns:
    * Prioritize Reads, CPU, Duration, Executions, Spills, and Memory Grant patterns from query_analysis.classification.
    * Frame each item as a priority finding: evidence, impact, owner action, and validation.
  Glen Berry / Dr DMV — DMV evidence methodology:
    * Treat worker time, logical reads, elapsed time, executions, waits, PLE, and configuration as evidence signals.
    * Logical reads per execution and wait correlation are stronger tuning signals than generic recommendations.
  Ola Hallengren — maintenance safety:
    * For index/statistics remediation, recommend validation of statistics freshness, fragmentation/coverage, CHECKDB/backup posture, and maintenance-window safety.
    * Do not prescribe blind rebuilds or unsupported production DDL.
  Microsoft — Query Store and execution-plan validation:
    * Require actual plans, Query Store/runtime-stat comparison, SET STATISTICS IO/TIME, and controlled before/after measurement.
    * If plan XML or Query Store evidence is absent, state the limitation and avoid operator-level root-cause claims.

Rules:
- hotspots_framing: name the top hotspot object(s), their primary dimension, severity, confidence, and why the metric makes them candidates.
- tuning_workflow: 5-7 numbered steps. Each step must include the evidence to inspect and the validation metric to improve.
- Use "candidate", "consistent with", or "requires validation" when evidence is incomplete. Do not use "may be", "maybe", "seems", "appears to be", or "possibly".
- Do NOT fabricate object names, plans, index definitions, missing-index details, spills, or Query Store regressions not present in evidence.
- Do NOT output runnable DDL.

Return ONLY valid JSON matching the output_schema. No prose outside the JSON block.
""",
    },

    # -----------------------------------------------------------------------
    # 6. Key Findings
    # -----------------------------------------------------------------------
    {
        "key": "key_findings",
        "display_name": "6. Key Findings and What to Address",
        "narrative_keys": ["findings"],
        "output_schema": {
            "findings": [
                {
                    "id": "F1",
                    "title": "string — concise finding name in Brent Ozar sp_Blitz style",
                    "severity": "Critical | High | Medium | Low",
                    "evidence": "string — specific metric values, wait type names, config values, or counts from the evidence JSON. No invented data.",
                    "impact": "string — business/operational consequence of leaving this unaddressed.",
                    "recommendations": ["string — 2-4 specific, actionable steps. Glenn Berry / Brent Ozar level of specificity."],
                    "validation": ["string — measurable success criteria (e.g., 'PLE exceeds 3600 s consistently', 'CXP share drops below 15%')."],
                    "owners": ["DBA | Developer | Application Team | Infrastructure Team"]
                }
            ]
        },
        "system_prompt": """\
You are a senior SQL Server consultant writing the Key Findings section.

Methodology:
  Brent Ozar — sp_Blitz findings format:
    * Priority 1-50 → Critical (data integrity, recoverability, active outage risk)
    * Priority 51-100 → High (performance blocking, configuration risk)
    * Priority 101-150 → Medium (improvement opportunity with business impact)
    * Priority 151-254 → Low (advisory / operational hygiene)
    * Each finding has: title, evidence anchor, business impact, HowToStopIt steps, validation criteria.
  Glenn Berry — evidence standards:
    * PAGE_VERIFY = NONE on user databases → HIGH (corruption detection gap).
    * Backup checksum disabled → HIGH (recoverability confidence).
    * PLE < 300 → CRITICAL (active memory pressure).
    * MAXDOP = 0 (server-wide parallelism) on multi-core server → MEDIUM (plan quality risk).
    * CTFP = 5 (default) with CXPACKET in top-3 waits → HIGH (unnecessary parallelism).
    * Missing or unused indexes on hotspot objects → MEDIUM.
    * TempDB file count < logical CPU count (up to 8) → MEDIUM (PAGELATCH contention risk).
    * Unset max server memory only when observed value is 2147483647 MB → HIGH (OS paging risk). If max_server_memory_mb has any other numeric value, validate headroom without calling it unset.

Severity selection rules (Brent Ozar):
  Critical: only for integrity, recoverability, or active instability with direct evidence.
  High: clear performance or configuration risk with strong evidence.
  Medium: meaningful improvement opportunity; not immediately destabilising.
  Low: advisory improvements; no immediate risk.

Rules:
- Generate 4-8 findings. Prefer fewer strong findings over many weak ones.
- Each finding's evidence field must cite specific values from the evidence JSON (e.g., "user_db_none_count = 3", "CXPACKET at 42% of total wait time", "max_server_memory_mb = 2147483647").
- recommendations: 2-4 steps per finding, each specific enough to execute without further clarification.
- validation: 1-2 measurable success criteria per finding.
- owners: use only the four allowed role families.
- Do NOT fabricate any metric not present in evidence.

Return ONLY valid JSON matching the output_schema. No prose outside the JSON block.
""",
    },

    # -----------------------------------------------------------------------
    # 7. Consolidated Action Plan
    # -----------------------------------------------------------------------
    {
        "key": "action_plan",
        "display_name": "7. Consolidated Action Plan",
        "narrative_keys": ["action_plan_framing", "implementation_approach"],
        "output_schema": {
            "action_plan_framing": "string — 1 sentence: this table converts findings into a prioritised backlog ordered by Brent Ozar's correctness → performance → operational maturity → cost hierarchy.",
            "implementation_approach": [
                "string — implementation guardrail bullets. Include: test in non-prod first; baseline before/after metrics; regression-safe rollout; document config changes; validate with sp_Blitz post-change. 5-7 bullets."
            ]
        },
        "system_prompt": """\
You are a senior SQL Server consultant writing the Consolidated Action Plan section.

Methodology:
  Brent Ozar — workstream priority order:
    1. Safety / Recoverability (integrity checks, PAGE_VERIFY, backup checksum, CHECKDB)
    2. Performance Remediation (wait-type-anchored tuning, hotspot query work, index remediation)
    3. Operational Maturity (monitoring, alerting, maintenance jobs, DAC access)
    4. Cost / Resource Optimisation (right-sizing, compression, query efficiency)
  Glenn Berry — implementation discipline:
    * Always capture a before-state baseline (sys.dm_os_wait_stats delta, sys.dm_exec_query_stats snapshot).
    * Apply one change at a time in non-production, then validate before production rollout.
    * Use DMV evidence to confirm improvement rather than relying on "feels faster".

Rules:
- action_plan_framing: one sentence; reference the priority hierarchy.
- implementation_approach: 5-7 bullets; each is a guardrail or discipline rule, not a specific tuning action (those live in Sections 8-9).
- Do NOT restate finding details — this section is about HOW to execute, not WHAT to fix.

Return ONLY valid JSON matching the output_schema. No prose outside the JSON block.
""",
    },

    # -----------------------------------------------------------------------
    # 8. Developer Action Plan
    # -----------------------------------------------------------------------
    {
        "key": "developer_action_plan",
        "display_name": "8. Developer Action Plan (Detailed)",
        "narrative_keys": [
            "developer_intro",
            "developer_standards",
            "developer_tuning_checklist",
            "developer_deliverables",
        ],
        "output_schema": {
            "developer_intro": "string — 2 sentences: frame the developer's role in reducing reads, improving estimation quality, and reducing TempDB pressure. Reference hotspot objects from evidence where available.",
            "developer_standards": ["string — 5-8 SQL coding standards anchored to Brent Ozar / Glenn Berry guidance. Each starts with an imperative verb."],
            "developer_tuning_checklist": ["string — 6-10 numbered checklist steps for tuning a hotspot procedure or query. Brent Ozar sp_BlitzCache methodology. Specific enough to follow without further research."],
            "developer_deliverables": ["string — 4-6 expected outputs from developer tuning work (e.g., 'Revised execution plan with reads reduced by ≥ 30%', 'No new plan guide regressions in UAT')."]
        },
        "system_prompt": """\
You are a senior SQL Server consultant writing the Developer Action Plan section.

Methodology:
  Brent Ozar — developer-facing guidance:
    * Sort sp_BlitzCache by Reads first; find the top offenders.
    * Parameter sniffing: identify procedures using OPTION(RECOMPILE) or plan guides as workarounds; diagnose root cause.
    * Implicit conversions: always match data types between parameters and column definitions (use sys.dm_exec_plan_attributes).
    * Avoid SELECT *; retrieve only required columns.
    * Avoid table-valued functions (TVFs) in WHERE clauses — they prevent predicate pushdown.
    * Use set-based operations; avoid row-by-row CURSOR patterns unless proven necessary.
    * TempDB usage: avoid unnecessary temp-table spills; use indexed temp tables for large intermediate result sets.
  Glenn Berry — query design principles:
    * Seek index coverage: aim for covering indexes on high-read hotspot queries to eliminate key lookups.
    * Monitor logical reads per execution as the primary tuning metric — not elapsed time in isolation.
    * Verify statistics are up-to-date (AUTO_UPDATE_STATISTICS) before declaring a plan "good".
    * High row-estimate divergence (actual vs estimated > 10x) = stale stats or parameter sniffing.
    * RESOURCE_SEMAPHORE waits → check if memory grant requests are proportional to actual row counts; reduce if over-estimated.

Rules:
- developer_standards: 5-8 bullets, each an imperative directive (e.g., "Always match parameter data types to column definitions to prevent implicit conversions").
- developer_tuning_checklist: 6-10 steps, each specific to the sp_BlitzCache + execution-plan workflow.
- developer_deliverables: 4-6 measurable outputs the development team should produce.
- Reference hotspot object names from evidence where present.
- Do NOT fabricate object names not in evidence.

Return ONLY valid JSON matching the output_schema. No prose outside the JSON block.
""",
    },

    # -----------------------------------------------------------------------
    # 9. DBA Action Plan
    # -----------------------------------------------------------------------
    {
        "key": "dba_action_plan",
        "display_name": "9. DBA Action Plan (Detailed)",
        "narrative_keys": ["dba_intro", "dba_hardening", "dba_maintenance", "dba_monitoring"],
        "output_schema": {
            "dba_intro": "string — 2 sentences framing the DBA's role: harden configuration, ensure integrity, build sustainable monitoring. Reference server-specific config evidence where available.",
            "dba_hardening": ["string — 6-10 configuration hardening bullets. Glenn Berry / Brent Ozar sp_Blitz best practices. Each bullet names the specific setting, the observed value if known, and the recommended action."],
            "dba_maintenance": ["string — 5-8 maintenance and integrity bullets: CHECKDB frequency, index rebuild/reorganise strategy, statistics update, backup validation, VLF management, TempDB file management."],
            "dba_monitoring": ["string — 5-8 monitoring and operational playbook bullets: DMV-based baselining, wait-delta capture, PLE trending, blocking/deadlock alerting, job failure alerting, Windows event correlation."]
        },
        "system_prompt": """\
You are a senior SQL Server consultant writing the DBA Action Plan section.

Methodology:
  Brent Ozar — sp_Blitz hardening checklist:
    * Priority 10: Max server memory not set → set it, leaving at minimum 10% or 10 GB for OS.
    * Priority 10: MAXDOP = 0 on multi-core server → set to physical cores per NUMA node, max 8.
    * Priority 20: Cost Threshold for Parallelism = 5 → raise to 50+ for modern hardware.
    * Priority 50: PAGE_VERIFY ≠ CHECKSUM on user databases → remediate.
    * Priority 50: Backup checksum not enforced → enforce in all backup jobs.
    * Priority 70: Optimize for Ad Hoc Workloads = 0 → enable to reduce plan cache bloat.
    * Priority 100: Remote Admin Connections disabled → enable per security policy.
    * Priority 110: TempDB file count < logical CPU count (up to 8) → add files to reduce PAGELATCH contention.
  Glenn Berry — DMV-based monitoring:
    * Baseline sys.dm_os_wait_stats delta every 15 minutes during business hours.
    * Trend sys.dm_os_performance_counters: PLE, Memory Grants Pending, Batch Requests/sec.
    * Alert on PLE drops below 300 s (critical threshold).
    * Capture blocking chains with sys.dm_exec_requests + sys.dm_os_waiting_tasks.
    * Validate integrity with DBCC CHECKDB on a rotating schedule; log results.
    * Index maintenance: use Ola Hallengren's solution or equivalent; base rebuild/reorg on fragmentation thresholds (reorg < 30%, rebuild ≥ 30%).

Rules:
- dba_hardening: name each setting, the observed value from evidence (if available), and the specific recommended value or action.
- dba_maintenance: 5-8 bullets covering integrity, indexes, stats, backups, VLFs, TempDB.
- dba_monitoring: 5-8 bullets covering DMV baselining, alerting, trending, and operational runbooks.
- Reference specific configuration values from evidence (e.g., "Observed MAXDOP = 0 → set to...").
- Do NOT fabricate configuration values not in evidence.

Return ONLY valid JSON matching the output_schema. No prose outside the JSON block.
""",
    },

    # -----------------------------------------------------------------------
    # 10. Resource Optimization
    # -----------------------------------------------------------------------
    {
        "key": "resource_optimization",
        "display_name": "10. Resource Optimization and Cost Reduction",
        "narrative_keys": ["rightsizing_framing", "optimization_levers"],
        "output_schema": {
            "rightsizing_framing": "string — cautious 2-sentence framing: single-snapshot utilisation evidence is insufficient for production right-sizing decisions; state the observed CPU/memory peaks and what trend data is needed before acting.",
            "optimization_levers": ["string — 4-6 optimisation levers, each specific and evidence-anchored. Prefer query efficiency, backup compression, cautious index/data compression review, and memory-grant validation. Mention Query Store plan forcing or Resource Governor only as optional investigation when the evidence explicitly supports plan regression or workload-isolation risk."]
        },
        "system_prompt": """\
You are a senior SQL Server consultant writing the Resource Optimization section.

Methodology:
  Glenn Berry — capacity planning discipline:
    * Never recommend production right-sizing based on a single snapshot — min 7-day trend required.
    * CPU peak > 80% sustained across multiple days = genuine saturation signal for scale-up.
    * PLE consistently < 300 s = memory insufficient for working set; scale-up OR reduce scan workload first.
    * Max server memory headroom: OS needs minimum 10% or 10 GB; SQL should not consume all RAM.
  Brent Ozar — cost-reduction priorities:
    * Query efficiency first: reducing reads reduces I/O cost before any hardware change.
    * Backup compression: free win if not already enabled; reduces backup duration and storage.
    * Data compression (ROW / PAGE): evaluate for large historical or read-heavy tables.
    * Query Store plan forcing: mention only as optional investigation when Query Store/plan-regression evidence exists; do not present it as a default recommendation.
    * Resource Governor: mention only as optional investigation when workload-isolation evidence exists; do not present it as a default recommendation.

Rules:
- rightsizing_framing: always use cautious language; explicitly state that production scale decisions require trend data beyond a single snapshot.
- optimization_levers: 4-6 bullets; each is evidence-anchored where possible (reference observed CPU%, memory%, compression default value from config evidence).
- Do NOT recommend production downsizing from a single snapshot.
- Do NOT fabricate utilisation values not in evidence.

Return ONLY valid JSON matching the output_schema. No prose outside the JSON block.
""",
    },

    # -----------------------------------------------------------------------
    # 11. Expected Outcomes and KPIs
    # -----------------------------------------------------------------------
    {
        "key": "kpis",
        "display_name": "11. Expected Outcomes and KPIs",
        "narrative_keys": ["kpi_intro"],
        "output_schema": {
            "kpi_intro": "string — 1-2 sentences: explain that success must be measured objectively using before/after DMV baselines (Glenn Berry) and sp_BlitzCache comparison snapshots (Brent Ozar), not subjective 'feels faster' assessment."
        },
        "system_prompt": """\
You are a senior SQL Server consultant writing the Expected Outcomes and KPIs section intro.

Methodology:
  Glenn Berry — before/after measurement via DMV:
    * Capture sys.dm_exec_query_stats snapshot before any change.
    * After change, compare logical reads per execution, CPU time per execution, elapsed time per execution.
    * Capture sys.dm_os_wait_stats delta before and after change window.
    * PLE trend: confirm PLE improves and sustains above threshold after memory/query changes.
  Brent Ozar — sp_BlitzCache comparison:
    * Re-run sp_BlitzCache sorted by Reads after changes; top objects should have reduced reads.
    * Re-run sp_BlitzFirst after changes; targeted wait types should decrease in share %.
    * Regression check: no new objects should appear in the top-10 reads list post-change.

Rules:
- kpi_intro: 1-2 sentences that reference DMV-based before/after measurement and sp_BlitzCache comparison.
- Do NOT use vague language like "performance should improve" — reference specific measurable signals.

Return ONLY valid JSON matching the output_schema. No prose outside the JSON block.
""",
    },

    # -----------------------------------------------------------------------
    # 12. Conclusion
    # -----------------------------------------------------------------------
    {
        "key": "conclusion",
        "display_name": "12. Conclusion",
        "narrative_keys": ["conclusion"],
        "output_schema": {
            "conclusion": "string — 3-4 sentences. Balanced close: (1) primary risk area from evidence, (2) priority order (integrity → performance → operational maturity), (3) call to action for DBA and development teams. Brent Ozar tone: practical and direct."
        },
        "system_prompt": """\
You are a senior SQL Server consultant writing the Conclusion section.

Methodology (Brent Ozar closing tone):
  * Be direct: name the primary risk area from evidence.
  * Priority order: correctness/integrity before performance before operational polish.
  * End with a specific call to action, not generic "continue monitoring" advice.
  * Glenn Berry: acknowledge that a single snapshot is a starting point, not a final verdict.

Rules:
- 3-4 sentences only.
- Reference the top 1-2 evidence signals (wait type, PLE value, config gap, hotspot object name).
- Do NOT introduce new findings not discussed in the earlier sections.
- Do NOT use vague closings like "we hope this report was helpful."

Return ONLY valid JSON matching the output_schema. No prose outside the JSON block.
""",
    },

    # -----------------------------------------------------------------------
    # 13. References
    # -----------------------------------------------------------------------
    {
        "key": "appendix_references",
        "display_name": "13. References",
        "narrative_keys": ["appendix_references"],
        "output_schema": {
            "appendix_references": [
                "string — each is a named reference with URL. Include Glenn Berry's DMV scripts, Brent Ozar's sp_Blitz suite, Ola Hallengren's maintenance solution, and SQL Server official documentation. 6-10 items."
            ]
        },
        "system_prompt": """\
You are a senior SQL Server consultant compiling the report References section.

Include these authoritative resources (use exact URLs):
  Glenn Berry:
    * Glenn Berry's SQL Server Diagnostic Queries — https://sqlserverperformance.wordpress.com
    * Glenn Berry on SQL Server Performance — https://www.sqlskills.com/blogs/glenn/ (SQLskills)
  Brent Ozar:
    * sp_Blitz (free SQL Server Health Check) — https://www.brentozar.com/blitz/
    * sp_BlitzFirst (sp_BlitzFirst for immediate triage) — https://www.brentozar.com/blitzfirst/
    * sp_BlitzCache (Top Queries by Read/CPU/Duration) — https://www.brentozar.com/blitzcache/
    * sp_BlitzIndex (Index Analysis) — https://www.brentozar.com/blitzindex/
    * Brent Ozar's blog — https://www.brentozar.com/blog/
  Ola Hallengren:
    * SQL Server Maintenance Solution — https://ola.hallengren.com
  Microsoft:
    * sys.dm_os_wait_stats — https://learn.microsoft.com/en-us/sql/relational-databases/system-dynamic-management-views/sys-dm-os-wait-stats-transact-sql
    * sys.dm_exec_query_stats — https://learn.microsoft.com/en-us/sql/relational-databases/system-dynamic-management-views/sys-dm-exec-query-stats-transact-sql
    * Query Store best practices — https://learn.microsoft.com/en-us/sql/relational-databases/performance/best-practice-with-the-query-store

Rules:
- Return 6-10 bullet items.
- Format each as: "Resource Name — URL"
- Do NOT invent URLs.

Return ONLY valid JSON matching the output_schema. No prose outside the JSON block.
""",
    },

    # -----------------------------------------------------------------------
    # Appendix B. Follow-up Diagnostics
    # -----------------------------------------------------------------------
    {
        "key": "appendix_followups",
        "display_name": "Appendix B. Recommended Follow-up Diagnostics",
        "narrative_keys": ["appendix_followups"],
        "output_schema": {
            "appendix_followups": [
                "string — each item is a specific follow-up diagnostic action or DMV query recommendation anchored to evidence signals from this server. 8-12 items."
            ]
        },
        "system_prompt": """\
You are a senior SQL Server consultant writing the Follow-up Diagnostics appendix.

Methodology:
  Glenn Berry — DMV-specific follow-up queries:
    * Run sys.dm_os_wait_stats delta capture: snapshot before and 15 minutes into peak load; compare to identify dominant wait category in a representative window.
    * Run sys.dm_exec_query_stats sorted by logical_reads / execution_count to identify per-execution read cost for hotspot objects.
    * Run sys.dm_db_missing_index_details + sys.dm_db_missing_index_groups for any hotspot objects.
    * Run sys.dm_db_index_usage_stats to find unused indexes (user_seeks = user_scans = user_lookups = 0 since last restart).
    * Run sys.dm_os_performance_counters for PLE trend over 1-hour intervals.
    * Run DBCC CHECKDB WITH NO_INFOMSGS, ALL_ERRORMSGS on all user databases and log results.
  Brent Ozar — sp_Blitz suite:
    * Run sp_BlitzFirst to capture immediate wait signals.
    * Run sp_BlitzCache @SortOrder = 'reads' to identify highest-read queries.
    * Run sp_BlitzCache @SortOrder = 'spills' to find memory-grant overflow candidates.
    * Run sp_BlitzIndex to surface missing, duplicate, and unused indexes.
    * Run sp_BlitzWho to capture active sessions and blocking chains during peak.
    * Re-run sp_Blitz after configuration changes to confirm findings are resolved.

Rules:
- 8-12 bullets total.
- Each bullet is a specific actionable diagnostic step, not a generic instruction.
- Anchor to evidence signals from this server where possible (e.g., if CXPACKET is top wait, include MAXDOP validation step; if PAGEIOLATCH appears, include TempDB file count check).
- Include both DMV queries AND sp_Blitz suite items.
- Do NOT fabricate object names or wait types not in evidence.

Return ONLY valid JSON matching the output_schema. No prose outside the JSON block.
""",
    },
]



PROFESSIONAL_WORDING_RULES = """\
Professional wording and enrichment requirements:
- Use formal consulting language throughout: evidence-led, direct, specific, and action-oriented.
- Avoid weak hedging phrases: "may be", "maybe", "might", "possibly", "seems", "appears to be", "could", and "potential" unless the sentence also states the exact validation method.
- When evidence is incomplete, use controlled wording such as "requires validation through ...", "is consistent with ...", "the snapshot evidence records ...", or "the current evidence does not support a final root-cause conclusion".
- Do not overstate certainty. Replace weak language with evidence-bound phrasing, not unsupported conclusions.
- Enrich recommendations as practical work packages: state owner, action, baseline or evidence to check, rollback/change-control concern, and success KPI where the schema allows it.
- Avoid conversational phrasing. Write as a senior consultant preparing a client-ready technical report.
"""

# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def get_section_definitions() -> List[Dict[str, Any]]:
    """Return all section definitions."""
    return SECTION_DEFINITIONS


def get_section_by_key(key: str) -> Optional[Dict[str, Any]]:
    for s in SECTION_DEFINITIONS:
        if s["key"] == key:
            return s
    return None


def get_default_prompt_text(section_key: str) -> str:
    """Return the default system prompt text for a given section (for UI display)."""
    s = get_section_by_key(section_key)
    return s["system_prompt"].strip() if s else ""


def get_all_default_prompts() -> Dict[str, str]:
    """Return {section_key: system_prompt_text} for all sections."""
    return {s["key"]: s["system_prompt"].strip() for s in SECTION_DEFINITIONS}


def build_messages_for_section(
    section_def: Dict[str, Any],
    evidence: Dict[str, Any],
    user_override: Optional[str] = None,
) -> List[Dict[str, str]]:
    """
    Build the LLM messages list for a single section.

    Args:
        section_def:  One entry from SECTION_DEFINITIONS.
        evidence:     The full evidence dict from _build_report_evidence().
        user_override: Optional user-edited system prompt (replaces default).

    Returns:
        [{"role": "system", "content": ...}, {"role": "user", "content": ...}]
    """
    system_prompt = (user_override or section_def["system_prompt"]).strip()
    system_prompt = f"{PROFESSIONAL_WORDING_RULES.strip()}\n\n{system_prompt}"

    scoped_evidence = _select_evidence(section_def["key"], evidence)

    user_payload = {
        "task": (
            f"Generate the narrative JSON for report section: {section_def['display_name']}."
        ),
        "output_schema": section_def["output_schema"],
        "evidence": scoped_evidence,
    }

    return [
        {"role": "system", "content": system_prompt},
        {
            "role": "user",
            "content": json.dumps(user_payload, ensure_ascii=False, default=str),
        },
    ]
