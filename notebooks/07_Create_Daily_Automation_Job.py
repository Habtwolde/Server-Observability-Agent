from __future__ import annotations

import json
import re
from typing import Any

import requests
from databricks.sdk import WorkspaceClient


# Run this notebook once after notebooks 01-06 exist in the workspace folder.
# It creates or updates one paused production Workflow. Review it, configure the
# SMTP secrets, then unpause it from Jobs & Pipelines.

dbutils.widgets.text(
    "notebooks_root",
    "/Users/eaglesanalytica@gmail.com/Streamlit_Databricks_log_performance/Server-Observability-Agent/notebooks",
    "Workspace notebooks folder",
)
dbutils.widgets.text(
    "job_name",
    "prod-server-observability-agent-daily",
    "Workflow name",
)
dbutils.widgets.text(
    "recipient_email",
    "habtwolde5@gmail.com",
    "Priority and failure notification recipient",
)
dbutils.widgets.dropdown(
    "top_issues_per_server",
    "5",
    ["3", "5"],
    "Priority issues per server",
)
dbutils.widgets.dropdown(
    "schedule_state",
    "PAUSED",
    ["PAUSED", "UNPAUSED"],
    "Initial schedule state",
)


def validate_email(value: str) -> str:
    clean = value.strip()
    if not re.fullmatch(r"[^\s@]+@[^\s@]+\.[^\s@]+", clean):
        raise ValueError(f"Invalid recipient email address: {clean!r}")
    return clean


NOTEBOOKS_ROOT = dbutils.widgets.get("notebooks_root").strip().rstrip("/")
JOB_NAME = dbutils.widgets.get("job_name").strip()
RECIPIENT_EMAIL = validate_email(dbutils.widgets.get("recipient_email"))
TOP_ISSUES_PER_SERVER = int(dbutils.widgets.get("top_issues_per_server"))
SCHEDULE_STATE = dbutils.widgets.get("schedule_state").strip().upper()

if not NOTEBOOKS_ROOT.startswith("/Users/"):
    raise ValueError("notebooks_root must be an absolute /Users/... workspace path.")
if not JOB_NAME:
    raise ValueError("job_name cannot be blank.")
if SCHEDULE_STATE not in {"PAUSED", "UNPAUSED"}:
    raise ValueError(f"Invalid schedule state: {SCHEDULE_STATE}")


NOTEBOOK_NAMES = [
    "01_Register_and_Validate_Daily_Run",
    "02_Ingest_SQL_Diagnostics_Workbooks",
    "03_Ingest_Windows_Events",
    "04_Build_Analysis_Ready_Tables",
    "05_Evaluate_Server_Health_Rules",
    "06_Send_Priority_Email",
]


client = WorkspaceClient()
session = requests.Session()
session.headers.update({"Content-Type": "application/json", **client.config.authenticate()})
host = client.config.host.rstrip("/")


def api_request(
    method: str,
    path: str,
    *,
    params: dict[str, Any] | None = None,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    response = session.request(
        method,
        f"{host}{path}",
        params=params,
        json=payload,
        timeout=60,
    )
    if not response.ok:
        raise RuntimeError(
            f"Databricks API {method} {path} failed ({response.status_code}): "
            f"{response.text[:2000]}"
        )
    if not response.content:
        return {}
    return response.json()


# Fail early if a notebook is missing or was uploaded with a different name.
missing_paths: list[str] = []
for notebook_name in NOTEBOOK_NAMES:
    notebook_path = f"{NOTEBOOKS_ROOT}/{notebook_name}"
    response = session.get(
        f"{host}/api/2.0/workspace/get-status",
        params={"path": notebook_path},
        timeout=30,
    )
    if response.status_code == 404:
        missing_paths.append(notebook_path)
    elif not response.ok:
        raise RuntimeError(
            f"Could not validate workspace path {notebook_path}: "
            f"{response.status_code} {response.text[:1000]}"
        )

if missing_paths:
    raise RuntimeError(
        "Upload or rename these required notebooks before creating the Workflow:\n- "
        + "\n- ".join(missing_paths)
    )


def notebook_task(
    *,
    task_key: str,
    notebook_name: str,
    base_parameters: dict[str, str],
    depends_on: str | None = None,
) -> dict[str, Any]:
    task: dict[str, Any] = {
        "task_key": task_key,
        "notebook_task": {
            "notebook_path": f"{NOTEBOOKS_ROOT}/{notebook_name}",
            "source": "WORKSPACE",
            "base_parameters": base_parameters,
        },
        "environment_key": "default",
        "timeout_seconds": 3600,
        "max_retries": 1,
        "min_retry_interval_millis": 120000,
        "retry_on_timeout": True,
    }
    if depends_on:
        task["depends_on"] = [{"task_key": depends_on}]
    return task


run_id_reference = "{{tasks.register_validate.values.run_id}}"
tasks = [
    notebook_task(
        task_key="register_validate",
        notebook_name="01_Register_and_Validate_Daily_Run",
        base_parameters={
            "run_date": "",
            "bootstrap_registry": "false",
            "fail_on_incomplete": "true",
            "run_trigger": "SCHEDULED_JOB",
        },
    ),
    notebook_task(
        task_key="ingest_sql_diagnostics",
        notebook_name="02_Ingest_SQL_Diagnostics_Workbooks",
        depends_on="register_validate",
        base_parameters={
            "run_id": run_id_reference,
            "allow_incomplete_run": "false",
            "fail_on_workbook_error": "true",
            "force_reprocess": "false",
        },
    ),
    notebook_task(
        task_key="ingest_windows_events",
        notebook_name="03_Ingest_Windows_Events",
        depends_on="ingest_sql_diagnostics",
        base_parameters={
            "run_id": run_id_reference,
            "allow_incomplete_run": "false",
            "force_reprocess": "false",
        },
    ),
    notebook_task(
        task_key="build_analysis_tables",
        notebook_name="04_Build_Analysis_Ready_Tables",
        depends_on="ingest_windows_events",
        base_parameters={
            "run_id": run_id_reference,
            "allow_incomplete_run": "false",
        },
    ),
    notebook_task(
        task_key="evaluate_health_rules",
        notebook_name="05_Evaluate_Server_Health_Rules",
        depends_on="build_analysis_tables",
        base_parameters={
            "run_id": run_id_reference,
            "allow_incomplete_run": "false",
            "top_issues_per_server": str(TOP_ISSUES_PER_SERVER),
        },
    ),
    notebook_task(
        task_key="send_priority_email",
        notebook_name="06_Send_Priority_Email",
        depends_on="evaluate_health_rules",
        base_parameters={
            "run_id": run_id_reference,
            "recipient_email": RECIPIENT_EMAIL,
            "top_issues_per_server": str(TOP_ISSUES_PER_SERVER),
            "minimum_severity": "HIGH",
            "allow_test_run": "false",
            "force_resend": "false",
            "smtp_host": "smtp.gmail.com",
            "smtp_port": "587",
            "secret_scope": "server-observability-agent",
            "smtp_user_secret_key": "smtp-user",
            "smtp_password_secret_key": "smtp-app-password",
        },
    ),
]

job_settings: dict[str, Any] = {
    "name": JOB_NAME,
    "description": (
        "Daily SQL Server diagnostics and Windows Events ingestion, deterministic "
        "health-rule evaluation, and priority-only DBA email briefing."
    ),
    "max_concurrent_runs": 1,
    "timeout_seconds": 7200,
    "queue": {"enabled": True},
    "schedule": {
        "quartz_cron_expression": "0 15 11 * * ?",
        "timezone_id": "America/New_York",
        "pause_status": SCHEDULE_STATE,
    },
    "email_notifications": {
        "on_failure": [RECIPIENT_EMAIL],
    },
    "notification_settings": {
        "no_alert_for_skipped_runs": True,
        "no_alert_for_canceled_runs": True,
    },
    "tags": {
        "application": "server-observability-agent",
        "environment": "production",
        "owner": "dba-operations",
    },
    "environments": [
        {
            "environment_key": "default",
            "spec": {
                "environment_version": "2",
                "dependencies": ["openpyxl==3.1.5"],
            },
        }
    ],
    "tasks": tasks,
}

existing = api_request(
    "GET",
    "/api/2.1/jobs/list",
    params={"name": JOB_NAME, "limit": 25},
).get("jobs", [])

exact_matches = [job for job in existing if str(job.get("settings", {}).get("name")) == JOB_NAME]
if len(exact_matches) > 1:
    raise RuntimeError(
        f"More than one Workflow is named {JOB_NAME!r}. Resolve duplicates before continuing."
    )

if exact_matches:
    job_id = int(exact_matches[0]["job_id"])
    api_request(
        "POST",
        "/api/2.1/jobs/reset",
        payload={"job_id": job_id, "new_settings": job_settings},
    )
    operation = "UPDATED"
else:
    created = api_request("POST", "/api/2.1/jobs/create", payload=job_settings)
    job_id = int(created["job_id"])
    operation = "CREATED"

result = {
    "operation": operation,
    "job_id": job_id,
    "job_name": JOB_NAME,
    "schedule": "Daily at 11:15 America/New_York",
    "schedule_state": SCHEDULE_STATE,
    "recipient": RECIPIENT_EMAIL,
    "task_count": len(tasks),
    "workspace_url": f"{host}/jobs/{job_id}",
}

display(spark.createDataFrame([(key, str(value)) for key, value in result.items()], ["check", "value"]))
print(json.dumps(result, indent=2))
