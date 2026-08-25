"""Grounded LLM explanations for deterministic Agent findings."""
from __future__ import annotations

import json
from typing import Any

import pandas as pd

from services.agent_findings_service import (
    load_fleet_health,
    load_latest_run,
    load_server_health,
    load_top_findings,
)
from services.llm_service import chat_completion


def _json_safe(value: Any) -> Any:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except Exception:
            pass
    if isinstance(value, (str, int, float, bool, list, dict)):
        return value
    return str(value)


def _records(df: pd.DataFrame, limit: int | None = None) -> list[dict[str, Any]]:
    if df is None or df.empty:
        return []
    source = df.head(limit) if limit else df
    records: list[dict[str, Any]] = []
    for raw in source.to_dict(orient="records"):
        record = {str(key): _json_safe(value) for key, value in raw.items()}
        evidence = record.get("evidence_json")
        if isinstance(evidence, str):
            try:
                record["evidence"] = json.loads(evidence)
            except json.JSONDecodeError:
                record["evidence"] = evidence
            record.pop("evidence_json", None)
        records.append(record)
    return records


def build_grounded_messages(
    *,
    question: str,
    scope: str,
    run_records: list[dict[str, Any]],
    health_records: list[dict[str, Any]],
    finding_records: list[dict[str, Any]],
) -> list[dict[str, str]]:
    evidence_payload = {
        "scope": scope,
        "latest_pipeline_run": run_records,
        "health_summary": health_records,
        "priority_findings": finding_records,
    }

    system_message = """
You are the SQL Server Observability Agent for a DBA operations team.

Use only the supplied deterministic findings and health data. Never invent a
metric, event, cause, affected database, timestamp, or Microsoft recommendation.
If the data is incomplete, historical, or test-only, say so prominently.

Prioritize CRITICAL before HIGH, then MEDIUM and LOW. Explain why each issue
requires attention, cite the concrete evidence in plain language, and preserve
the recommended action and Microsoft reference supplied with the finding.
Keep the response concise and operational. Do not repeat healthy information
unless the question asks for it. Do not claim that remediation was completed.
""".strip()

    user_message = f"""
Administrator question:
{question.strip()}

Authoritative Agent evidence:
{json.dumps(evidence_payload, ensure_ascii=False, indent=2)}
""".strip()

    return [
        {"role": "system", "content": system_message},
        {"role": "user", "content": user_message},
    ]


def answer_agent_question(
    question: str,
    server_name: str | None = None,
    top_n: int = 5,
) -> str:
    clean_question = str(question or "").strip()
    if not clean_question:
        raise ValueError("Enter a question for the Agent.")

    run_records = _records(load_latest_run(), limit=1)
    if server_name:
        scope = server_name.strip().upper()
        health_records = _records(load_server_health(scope), limit=1)
        finding_records = _records(load_top_findings(scope, top_n=top_n), limit=top_n)
    else:
        scope = "ALL SERVERS"
        health_records = _records(load_fleet_health(), limit=45)
        # A fleet response uses the most severe findings across the environment.
        finding_records = _records(load_top_findings(None, top_n=top_n), limit=25)

    messages = build_grounded_messages(
        question=clean_question,
        scope=scope,
        run_records=run_records,
        health_records=health_records,
        finding_records=finding_records,
    )
    return chat_completion(messages, temperature=0.1, max_tokens=1800)


def generate_server_briefing(server_name: str, top_n: int = 5) -> str:
    server = str(server_name or "").strip().upper()
    if not server:
        raise ValueError("Select a server before generating a briefing.")
    return answer_agent_question(
        "Summarize only the priority issues that require DBA attention. For each, "
        "state the issue, evidence, likely cause, immediate action, and supplied "
        "Microsoft reference. End with a short ordered action plan.",
        server_name=server,
        top_n=top_n,
    )

