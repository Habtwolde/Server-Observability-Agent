# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# 08_Create_Daily_Automation_Job
#
# Initial deployment notebook only.
#
# Run this notebook once to create/update the fixed production Workflow.
# Subscriber additions/removals do NOT require rerunning this notebook because
# task 07 reads the active subscription table dynamically on every job run.

from __future__ import annotations

from databricks.sdk import WorkspaceClient

workspace_client = WorkspaceClient()


# -----------------------------------------------------------------------------
# 1. Deployment parameters
# -----------------------------------------------------------------------------

current_notebook_path = (
    dbutils.notebook.entry_point
    .getDbutils()
    .notebook()
    .getContext()
    .notebookPath()
    .get()
)

DEFAULT_NOTEBOOKS_ROOT = current_notebook_path.rsplit("/", 1)[0]


dbutils.widgets.text(
    "job_name",
    "prod-server-observability-agent-daily",
    "Production Workflow name",
)

dbutils.widgets.text(
    "warehouse_id",
    "47bde9279fec4222",
    "SQL warehouse used for Databricks Alerts",
)

dbutils.widgets.text(
    "alert_name",
    "prod-server-observability-agent-priority",
    "Base subscriber alert name",
)

dbutils.widgets.dropdown(
    "top_issues_per_server",
    "5",
    ["3", "5"],
    "Priority findings retained per server",
)

dbutils.widgets.dropdown(
    "schedule_state",
    "PAUSED",
    ["PAUSED", "UNPAUSED"],
    "Daily Workflow schedule state",
)

JOB_NAME = dbutils.widgets.get("job_name").strip()
WAREHOUSE_ID = dbutils.widgets.get("warehouse_id").strip()
ALERT_NAME = dbutils.widgets.get("alert_name").strip()
TOP_ISSUES_PER_SERVER = int(
    dbutils.widgets.get("top_issues_per_server").strip()
)
SCHEDULE_STATE = dbutils.widgets.get("schedule_state").strip().upper()
NOTEBOOKS_ROOT = DEFAULT_NOTEBOOKS_ROOT

if not JOB_NAME:
    raise ValueError("job_name cannot be blank.")
if not WAREHOUSE_ID:
    raise ValueError("warehouse_id cannot be blank.")
if not ALERT_NAME:
    raise ValueError("alert_name cannot be blank.")
if TOP_ISSUES_PER_SERVER not in {3, 5}:
    raise ValueError("top_issues_per_server must be 3 or 5.")
if SCHEDULE_STATE not in {"PAUSED", "UNPAUSED"}:
    raise ValueError(f"Invalid schedule_state: {SCHEDULE_STATE}")

print("SQL Server Observability Agent Workflow deployment")
print(f"Workflow: {JOB_NAME}")
print(f"Notebook folder: {NOTEBOOKS_ROOT}")
print(f"Warehouse: {WAREHOUSE_ID}")
print(f"Schedule state: {SCHEDULE_STATE}")


# -----------------------------------------------------------------------------
# 2. Fixed production task chain
# -----------------------------------------------------------------------------

RUN_ID_REFERENCE = "{{tasks.validate_daily_run.values.run_id}}"

workflow_tasks = [
    {
        "task_key": "validate_daily_run",
        "run_if": "ALL_SUCCESS",
        "notebook_task": {
            "notebook_path": (
                f"{NOTEBOOKS_ROOT}/01A_Validate_Available_Daily_Run"
            ),
            "source": "WORKSPACE",
            "base_parameters": {
                "run_date": "",
                "bootstrap_registry": "false",
                "execution_mode": "PRODUCTION",
            },
        },
    },
    {
        "task_key": "ingest_sql_diagnostics",
        "depends_on": [{"task_key": "validate_daily_run"}],
        "run_if": "ALL_SUCCESS",
        "notebook_task": {
            "notebook_path": (
                f"{NOTEBOOKS_ROOT}/02_Ingest_SQL_Diagnostics_Workbooks"
            ),
            "source": "WORKSPACE",
            "base_parameters": {
                "run_id": RUN_ID_REFERENCE,
                "allow_incomplete_run": "false",
                "force_reprocess": "false",
                "fail_on_workbook_error": "true",
            },
        },
    },
    {
        "task_key": "ingest_windows_events",
        "depends_on": [{"task_key": "ingest_sql_diagnostics"}],
        "run_if": "ALL_SUCCESS",
        "notebook_task": {
            "notebook_path": (
                f"{NOTEBOOKS_ROOT}/03_Ingest_Windows_Events"
            ),
            "source": "WORKSPACE",
            "base_parameters": {
                "run_id": RUN_ID_REFERENCE,
                "allow_incomplete_run": "false",
                "force_reprocess": "false",
            },
        },
    },
    {
        "task_key": "build_analysis_tables",
        "depends_on": [{"task_key": "ingest_windows_events"}],
        "run_if": "ALL_SUCCESS",
        "notebook_task": {
            "notebook_path": (
                f"{NOTEBOOKS_ROOT}/04_Build_Analysis_Ready_Tables"
            ),
            "source": "WORKSPACE",
            "base_parameters": {
                "run_id": RUN_ID_REFERENCE,
                "allow_incomplete_run": "false",
            },
        },
    },
    {
        "task_key": "evaluate_health_rules",
        "depends_on": [{"task_key": "build_analysis_tables"}],
        "run_if": "ALL_SUCCESS",
        "notebook_task": {
            "notebook_path": (
                f"{NOTEBOOKS_ROOT}/05_Evaluate_Server_Health_Rules"
            ),
            "source": "WORKSPACE",
            "base_parameters": {
                "run_id": RUN_ID_REFERENCE,
                "allow_incomplete_run": "false",
                "top_issues_per_server": str(TOP_ISSUES_PER_SERVER),
            },
        },
    },
    {
        "task_key": "prepare_databricks_alert",
        "depends_on": [{"task_key": "evaluate_health_rules"}],
        "run_if": "ALL_SUCCESS",
        "notebook_task": {
            "notebook_path": (
                f"{NOTEBOOKS_ROOT}/06_Prepare_Databricks_Alert"
            ),
            "source": "WORKSPACE",
            "base_parameters": {
                "run_id": RUN_ID_REFERENCE,
                "top_issues_per_server": str(TOP_ISSUES_PER_SERVER),
                "minimum_severity": "HIGH",
                "allow_test_email_delivery": "false",
            },
        },
    },
    {
        "task_key": "dispatch_subscriber_notifications",
        "depends_on": [{"task_key": "prepare_databricks_alert"}],
        "run_if": "ALL_SUCCESS",
        "notebook_task": {
            "notebook_path": (
                f"{NOTEBOOKS_ROOT}/07_Dispatch_Subscriber_Notifications"
            ),
            "source": "WORKSPACE",
            "base_parameters": {
                "run_id": RUN_ID_REFERENCE,
                "warehouse_id": WAREHOUSE_ID,
                "alert_name": ALERT_NAME,
                "dispatch_timeout_minutes": "20",
            },
        },
    },
]

workflow_spec = {
    "name": JOB_NAME,
    "description": (
        "Daily SQL Server Observability Agent. Processes available validated "
        "registered server inputs, evaluates health findings, then dynamically "
        "dispatches subscriber-specific Databricks email alerts."
    ),
    "schedule": {
        "quartz_cron_expression": "0 15 11 * * ?",
        "timezone_id": "America/New_York",
        "pause_status": SCHEDULE_STATE,
    },
    "max_concurrent_runs": 1,
    "queue": {"enabled": True},
    "tasks": workflow_tasks,
}

print("")
print("Fixed production task chain:")
for task in workflow_tasks:
    dependencies = [
        item["task_key"]
        for item in task.get("depends_on", [])
    ]
    print(f"- {task['task_key']} <- {dependencies or ['START']}")


# -----------------------------------------------------------------------------
# 3. Create or update the saved production Workflow
# -----------------------------------------------------------------------------

jobs_response = workspace_client.api_client.do(
    "GET",
    "/api/2.2/jobs/list",
    query={
        "name": JOB_NAME,
        "limit": 100,
    },
)

matching_jobs = jobs_response.get("jobs", []) or []

if len(matching_jobs) > 1:
    raise RuntimeError(
        f"Multiple Workflows named {JOB_NAME!r} exist. Resolve duplicates first."
    )

if matching_jobs:
    job_id = int(matching_jobs[0]["job_id"])

    workspace_client.api_client.do(
        "POST",
        "/api/2.2/jobs/reset",
        body={
            "job_id": job_id,
            "new_settings": workflow_spec,
        },
    )

    print(f"Updated existing Workflow: {JOB_NAME} [job_id={job_id}]")

else:
    created = workspace_client.api_client.do(
        "POST",
        "/api/2.2/jobs/create",
        body=workflow_spec,
    )

    job_id = created.get("job_id")
    if not job_id:
        raise RuntimeError("Databricks returned no job_id after Workflow creation.")

    print(f"Created Workflow: {JOB_NAME} [job_id={job_id}]")


# -----------------------------------------------------------------------------
# 4. Verify the saved Workflow
# -----------------------------------------------------------------------------

job_detail = workspace_client.api_client.do(
    "GET",
    "/api/2.2/jobs/get",
    query={"job_id": job_id},
)

settings = job_detail.get("settings") or {}
saved_tasks = settings.get("tasks") or []
saved_schedule = settings.get("schedule") or {}

expected_task_keys = [task["task_key"] for task in workflow_tasks]
saved_task_keys = [task.get("task_key") for task in saved_tasks]

if saved_task_keys != expected_task_keys:
    raise RuntimeError(
        "Saved Workflow task chain does not match the expected fixed chain. "
        f"Expected={expected_task_keys}; saved={saved_task_keys}"
    )

if saved_schedule.get("timezone_id") != "America/New_York":
    raise RuntimeError("Saved Workflow has the wrong timezone.")

if saved_schedule.get("pause_status") != SCHEDULE_STATE:
    raise RuntimeError("Saved Workflow schedule state does not match the request.")

print("")
print("Production Workflow verified")
print(f"Job ID: {job_id}")
print(f"Task count: {len(saved_tasks)}")
print("Schedule: 11:15 America/New_York")
print(f"Schedule state: {saved_schedule.get('pause_status')}")
print("")
print(
    "Subscriber changes now require NO Workflow rebuild. "
    "The final dispatch task reads the active subscription table on every run."
)