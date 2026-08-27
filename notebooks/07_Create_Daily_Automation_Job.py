# Databricks notebook source
from __future__ import annotations

import json
from databricks.sdk import WorkspaceClient
workspace_client = WorkspaceClient()
from pyspark.sql import functions as F

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
    "notebooks_root",
    DEFAULT_NOTEBOOKS_ROOT,
    "Workspace notebooks folder",
)

dbutils.widgets.text(
    "job_name",
    "prod-server-observability-agent-daily",
    "Workflow name",
)

dbutils.widgets.text(
    "alert_name",
    "prod-server-observability-agent-priority",
    "Databricks SQL Alert name",
)

dbutils.widgets.text(
    "warehouse_id",
    "47bde9279fec4222",
    "SQL warehouse used by the Databricks Alert",
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
    "Initial Workflow schedule state",
)

dbutils.widgets.dropdown(
    "execution_mode",
    "PRODUCTION",
    ["PRODUCTION", "TEST"],
    "Workflow execution mode",
)

dbutils.widgets.text(
    "test_run_date",
    "",
    "TEST run date (YYYY-MM-DD)",
)

NOTEBOOKS_ROOT = DEFAULT_NOTEBOOKS_ROOT

JOB_NAME = dbutils.widgets.get("job_name").strip()

ALERT_NAME = dbutils.widgets.get("alert_name").strip()

WAREHOUSE_ID = (
    dbutils.widgets.get("warehouse_id")
    .strip()
)

CATALOG = "ent_log_analytics"
SCHEMA = "observability"

ALERT_SUBSCRIPTIONS_TABLE = (
    f"`{CATALOG}`.`{SCHEMA}`.`agent_alert_subscriptions`"
)

ROUTED_ALERT_VIEW = (
    f"`{CATALOG}`.`{SCHEMA}`.`v_agent_databricks_alert_routes`"
)

TOP_ISSUES_PER_SERVER = int(
    dbutils.widgets.get("top_issues_per_server")
)

SCHEDULE_STATE = (
    dbutils.widgets.get("schedule_state")
    .strip()
    .upper()
)

EXECUTION_MODE = (
    dbutils.widgets.get("execution_mode")
    .strip()
    .upper()
)

if EXECUTION_MODE not in {"PRODUCTION", "TEST"}:
    raise ValueError(
        f"Invalid execution_mode: {EXECUTION_MODE}"
    )

ALLOW_INCOMPLETE_PARAMETER = (
    "true"
    if EXECUTION_MODE == "TEST"
    else "false"
)

TEST_RUN_DATE = (
    dbutils.widgets.get("test_run_date")
    .strip()
)

if EXECUTION_MODE == "TEST" and not TEST_RUN_DATE:
    raise ValueError(
        "test_run_date is required when "
        "execution_mode=TEST."
    )

WORKFLOW_RUN_DATE = (
    TEST_RUN_DATE
    if EXECUTION_MODE == "TEST"
    else ""
)

if not NOTEBOOKS_ROOT.startswith("/"):
    raise ValueError(
        "notebooks_root must be an absolute "
        "Databricks workspace path."
    )

if not JOB_NAME:
    raise ValueError("job_name cannot be blank.")

if not ALERT_NAME:
    raise ValueError("alert_name cannot be blank.")

if not WAREHOUSE_ID:
    raise ValueError("warehouse_id cannot be blank.")

if TOP_ISSUES_PER_SERVER not in {3, 5}:
    raise ValueError(
        "top_issues_per_server must be 3 or 5."
    )

if SCHEDULE_STATE not in {
    "PAUSED",
    "UNPAUSED",
}:
    raise ValueError(
        f"Invalid schedule state: {SCHEDULE_STATE}"
    )

print("Databricks native notification deployment")
print(f"Workflow: {JOB_NAME}")
print(f"Alert: {ALERT_NAME}")
print(f"Warehouse ID: {WAREHOUSE_ID}")
print(f"Execution mode: {EXECUTION_MODE}")
print(f"Workspace notebooks folder: {NOTEBOOKS_ROOT}")
print(
    "Workflow run date: "
    f"{WORKFLOW_RUN_DATE or 'CURRENT BUSINESS DATE'}"
)

print(
    f"Top issues per server: "
    f"{TOP_ISSUES_PER_SERVER}"
)
print(f"Schedule state: {SCHEDULE_STATE}")

# COMMAND ----------

# COMMAND ----------

# -----------------------------------------------------------------------------
# 2. Load active subscriber routing
# -----------------------------------------------------------------------------

active_subscriptions_df = spark.sql(
    f"""
    SELECT
        subscription_id,
        lower(trim(subscriber_email)) AS subscriber_email,
        upper(trim(canonical_server_name)) AS canonical_server_name,
        notification_destination_id,
        notes,
        updated_ts
    FROM {ALERT_SUBSCRIPTIONS_TABLE}
    WHERE is_active = true
      AND subscriber_email IS NOT NULL
      AND trim(subscriber_email) <> ''
      AND canonical_server_name IS NOT NULL
      AND trim(canonical_server_name) <> ''
    ORDER BY
        subscriber_email,
        canonical_server_name
    """
)


ACTIVE_ROUTE_COUNT = active_subscriptions_df.count()

DISTINCT_SUBSCRIBER_COUNT = (
    active_subscriptions_df
    .select("subscriber_email")
    .distinct()
    .count()
)

MISSING_DESTINATION_COUNT = (
    active_subscriptions_df
    .where(
        F.col("notification_destination_id").isNull()
        | (F.trim(F.col("notification_destination_id")) == "")
    )
    .select("subscriber_email")
    .distinct()
    .count()
)


if ACTIVE_ROUTE_COUNT == 0:
    raise RuntimeError(
        "No active notification subscriptions are configured. "
        "Add at least one subscriber through the Streamlit app first."
    )


print("")
print("Active notification routing loaded")
print(f"Active server-recipient routes: {ACTIVE_ROUTE_COUNT}")
print(f"Distinct subscribers: {DISTINCT_SUBSCRIBER_COUNT}")
print(
    "Subscribers without notification destination: "
    f"{MISSING_DESTINATION_COUNT}"
)


display(
    active_subscriptions_df
)

# COMMAND ----------

# COMMAND ----------

# -----------------------------------------------------------------------------
# 3. Inspect existing Databricks notification destinations
# -----------------------------------------------------------------------------
notification_destinations = []
page_token = None

while True:
    query = {
        "page_size": 100,
    }

    if page_token:
        query["page_token"] = page_token

    response = workspace_client.api_client.do(
        "GET",
        "/api/2.0/notification-destinations",
        query=query,
    )

    notification_destinations.extend(
        response.get("results", [])
    )

    page_token = response.get("next_page_token")

    if not page_token:
        break


email_destinations = [
    destination
    for destination in notification_destinations
    if str(
        destination.get("destination_type", "")
    ).upper() == "EMAIL"
]


print("")
print("Databricks notification destinations inspected")
print(
    f"Total destinations visible: "
    f"{len(notification_destinations)}"
)
print(
    f"Email destinations visible: "
    f"{len(email_destinations)}"
)

if email_destinations:
    print("")
    print("Existing email destinations:")

    for destination in email_destinations:
        print(
            f"- {destination.get('display_name')} "
            f"[{destination.get('id')}]"
        )
else:
    print("No existing email notification destinations were found.")

# COMMAND ----------

# COMMAND ----------

# -----------------------------------------------------------------------------
# 4. Prepare subscriber email destinations
# -----------------------------------------------------------------------------

subscriber_rows = (
    active_subscriptions_df
    .select("subscriber_email")
    .distinct()
    .orderBy("subscriber_email")
    .collect()
)

subscriber_emails = [
    row["subscriber_email"]
    for row in subscriber_rows
]


destination_specs = []

for email in subscriber_emails:
    destination_specs.append(
        {
            "display_name": (
                "SQL Observability Agent - "
                f"{email}"
            ),
            "config": {
                "email": {
                    "addresses": [email]
                }
            },
        }
    )


print("")
print("Notification destinations to provision:")
print(
    json.dumps(
        destination_specs,
        indent=2,
    )
)

# COMMAND ----------

# COMMAND ----------

# -----------------------------------------------------------------------------
# 5. Create or resolve subscriber notification destinations
# -----------------------------------------------------------------------------

existing_by_name = {
    destination.get("display_name"): destination
    for destination in notification_destinations
    if destination.get("display_name")
}

resolved_destinations = {}


for spec in destination_specs:
    display_name = spec["display_name"]

    email = (
        spec["config"]["email"]["addresses"][0]
        .strip()
        .lower()
    )

    existing = existing_by_name.get(display_name)

    if existing:
        destination_type = str(
            existing.get("destination_type", "")
        ).upper()

        if destination_type != "EMAIL":
            raise RuntimeError(
                f"Existing notification destination "
                f"{display_name!r} is not EMAIL."
            )

        destination_id = existing.get("id")

        if not destination_id:
            raise RuntimeError(
                f"Existing destination {display_name!r} "
                "does not have an ID."
            )

        print(
            f"Using existing email destination: "
            f"{display_name} [{destination_id}]"
        )

    else:
        try:
            created = workspace_client.api_client.do(
                "POST",
                "/api/2.0/notification-destinations",
                body=spec,
            )
        except Exception as exc:
            raise RuntimeError(
                "Could not create Databricks email "
                f"notification destination for {email}. "
                "Creating notification destinations requires "
                "workspace-admin permission."
            ) from exc

        destination_id = created.get("id")

        if not destination_id:
            raise RuntimeError(
                f"Databricks created no destination ID "
                f"for {email}."
            )

        print(
            f"Created email destination: "
            f"{display_name} [{destination_id}]"
        )

    resolved_destinations[email] = destination_id


print("")
print("Subscriber destinations resolved:")
print(
    json.dumps(
        resolved_destinations,
        indent=2,
    )
)

# COMMAND ----------

# COMMAND ----------

# -----------------------------------------------------------------------------
# 6. Persist notification destination IDs to subscription routing
# -----------------------------------------------------------------------------

destination_rows = [
    (email, destination_id)
    for email, destination_id
    in resolved_destinations.items()
]

destination_map_df = spark.createDataFrame(
    destination_rows,
    [
        "subscriber_email",
        "notification_destination_id",
    ],
)

destination_map_df.createOrReplaceTempView(
    "_agent_notification_destinations"
)


spark.sql(
    f"""
    MERGE INTO {ALERT_SUBSCRIPTIONS_TABLE} AS target

    USING _agent_notification_destinations AS source

       ON lower(trim(target.subscriber_email))
          = source.subscriber_email

    WHEN MATCHED
         AND target.is_active = true
    THEN UPDATE SET

        target.notification_destination_id =
            source.notification_destination_id,

        target.updated_ts =
            current_timestamp()
    """
)


spark.catalog.dropTempView(
    "_agent_notification_destinations"
)


updated_subscriptions_df = spark.sql(
    f"""
    SELECT
        subscriber_email,
        canonical_server_name,
        notification_destination_id,
        is_active,
        updated_ts
    FROM {ALERT_SUBSCRIPTIONS_TABLE}
    WHERE is_active = true
    ORDER BY
        subscriber_email,
        canonical_server_name
    """
)


remaining_missing_destinations = (
    updated_subscriptions_df
    .where(
        F.col("notification_destination_id").isNull()
        | (
            F.trim(
                F.col("notification_destination_id")
            ) == ""
        )
    )
    .count()
)


print("")
print("Notification destination IDs persisted")
print(
    f"Active routes still missing a destination: "
    f"{remaining_missing_destinations}"
)

display(updated_subscriptions_df)

# COMMAND ----------

# COMMAND ----------

# -----------------------------------------------------------------------------
# 7. Inspect existing Databricks SQL Alerts
# -----------------------------------------------------------------------------

existing_alerts = []
page_token = None

while True:
    query = {
        "page_size": 100,
    }

    if page_token:
        query["page_token"] = page_token

    response = workspace_client.api_client.do(
        "GET",
        "/api/2.0/alerts",
        query=query,
    )

    page_alerts = (
        response.get("alerts")
        or response.get("results")
        or []
    )

    existing_alerts.extend(page_alerts)

    page_token = response.get("next_page_token")

    if not page_token:
        break


agent_alerts = [
    alert
    for alert in existing_alerts
    if str(
        alert.get("display_name", "")
    ).startswith(ALERT_NAME)
]


print("")
print("Databricks SQL Alerts inspected")
print(f"Total alerts visible: {len(existing_alerts)}")
print(
    f"Existing Observability Agent alerts: "
    f"{len(agent_alerts)}"
)


if agent_alerts:
    print("")
    print("Existing Agent alerts:")

    for alert in agent_alerts:
        schedule = alert.get("schedule") or {}

        print(
            f"- {alert.get('display_name')} "
            f"[{alert.get('id')}] "
            f"schedule={schedule.get('pause_status')}"
        )
else:
    print(
        "No existing SQL Observability Agent alerts were found."
    )

# COMMAND ----------

# COMMAND ----------

# -----------------------------------------------------------------------------
# 8. Create or update paused subscriber-specific SQL Alerts
# -----------------------------------------------------------------------------

created_alerts = {}

existing_agent_alerts_by_name = {
    alert.get("display_name"): alert
    for alert in agent_alerts
    if alert.get("display_name")
}


for email, destination_id in resolved_destinations.items():

    alert_display_name = (
        f"{ALERT_NAME} - {email}"
    )

    # Protect the SQL literal.
    email_sql = email.replace("'", "''")

    alert_query = f"""
    SELECT
        1 AS alert_trigger,
        snapshot_date,
        canonical_server_name,
        health_status,
        health_score,
        critical_issue_count,
        high_issue_count,
        priority_issue_count,
        priority_briefing

    FROM {ROUTED_ALERT_VIEW}

    WHERE lower(trim(subscriber_email)) =
          '{email_sql}'

    ORDER BY
        critical_issue_count DESC,
        high_issue_count DESC,
        canonical_server_name
    """.strip()


    # One authoritative definition used for both
    # CREATE and UPDATE.
    alert_spec = {
        "display_name": alert_display_name,

        "query_text": alert_query,

        "warehouse_id": WAREHOUSE_ID,

        "evaluation": {
            "source": {
                "name": "alert_trigger",
                "display": "alert_trigger",
            },

            "comparison_operator": "EQUAL",

            "threshold": {
                "value": {
                    "double_value": 1.0
                }
            },

            "empty_result_state": "OK",

            "notification": {
                "subscriptions": [
                    {
                        "destination_id":
                            destination_id
                    }
                ],

                "retrigger_seconds": 1,
                "notify_on_ok": False,
            },
        },

        # The Workflow evaluates the alert.
        # Its own schedule stays disabled.
        "schedule": {
            "quartz_cron_schedule":
                "0 30 11 * * ?",

            "timezone_id":
                "America/New_York",

            "pause_status":
                "PAUSED",
        },

        "custom_summary": (
            "SQL Server Observability Agent - "
            "Priority Findings"
        ),

        "custom_description": (
            "<h2>SQL Server Observability Agent</h2>"
            "<p>Priority findings for your subscribed "
            "SQL Servers are shown below.</p>"
            "{{QUERY_RESULT_TABLE}}"
        ),
    }


    existing_alert = (
        existing_agent_alerts_by_name.get(
            alert_display_name
        )
    )


    if existing_alert:

        alert_id = existing_alert.get("id")

        if not alert_id:
            raise RuntimeError(
                f"Existing alert {alert_display_name!r} "
                "does not have an ID."
            )

        workspace_client.api_client.do(
            "PATCH",
            f"/api/2.0/alerts/{alert_id}",
            query={
                "update_mask": (
                    "display_name,"
                    "query_text,"
                    "warehouse_id,"
                    "evaluation,"
                    "schedule,"
                    "custom_summary,"
                    "custom_description"
                )
            },
            body=alert_spec,
        )

        print(
            "Updated existing subscriber alert: "
            f"{alert_display_name} [{alert_id}]"
        )


    else:

        created = workspace_client.api_client.do(
            "POST",
            "/api/2.0/alerts",
            body=alert_spec,
        )

        alert_id = created.get("id")

        if not alert_id:
            raise RuntimeError(
                "Databricks did not return an alert ID "
                f"for {email}."
            )

        print(
            "Created PAUSED subscriber alert: "
            f"{alert_display_name} [{alert_id}]"
        )


    created_alerts[email] = {
        "alert_id": alert_id,
        "destination_id": destination_id,
        "display_name": alert_display_name,
    }


print("")
print("Subscriber SQL Alerts resolved:")
print(
    json.dumps(
        created_alerts,
        indent=2,
    )
)

# COMMAND ----------

# COMMAND ----------

# -----------------------------------------------------------------------------
# 9. Verify subscriber SQL Alert configuration
# -----------------------------------------------------------------------------

verified_alerts = {}


for email, alert_info in created_alerts.items():

    alert_id = alert_info["alert_id"]

    alert_detail = workspace_client.api_client.do(
        "GET",
        f"/api/2.0/alerts/{alert_id}",
    )

    verified_alerts[email] = alert_detail

    schedule = alert_detail.get("schedule") or {}
    evaluation = alert_detail.get("evaluation") or {}
    notification = evaluation.get("notification") or {}

    subscriptions = (
        notification.get("subscriptions") or []
    )

    destination_ids = [
        item.get("destination_id")
        for item in subscriptions
        if item.get("destination_id")
    ]

    print("")
    print(f"Subscriber: {email}")
    print(f"Alert ID: {alert_id}")
    print(
        f"Display name: "
        f"{alert_detail.get('display_name')}"
    )
    print(
        f"Warehouse ID: "
        f"{alert_detail.get('warehouse_id')}"
    )
    print(
        f"Schedule state: "
        f"{schedule.get('pause_status')}"
    )
    print(
        f"Notification destinations: "
        f"{destination_ids}"
    )


    if schedule.get("pause_status") != "PAUSED":
        raise RuntimeError(
            f"Safety violation: alert for {email} "
            "is not PAUSED."
        )

    expected_destination = (
        alert_info["destination_id"]
    )

    if expected_destination not in destination_ids:
        raise RuntimeError(
            f"Alert for {email} is not connected "
            "to the expected notification destination."
        )


print("")
print(
    "Subscriber SQL Alert configuration "
    "verified successfully."
)

# COMMAND ----------

# COMMAND ----------

# -----------------------------------------------------------------------------
# 9. Verify subscriber SQL Alert configuration
# -----------------------------------------------------------------------------

verified_alerts = {}


for email, alert_info in created_alerts.items():

    alert_id = alert_info["alert_id"]

    alert_detail = workspace_client.api_client.do(
        "GET",
        f"/api/2.0/alerts/{alert_id}",
    )

    verified_alerts[email] = alert_detail

    schedule = alert_detail.get("schedule") or {}
    evaluation = alert_detail.get("evaluation") or {}
    notification = evaluation.get("notification") or {}

    subscriptions = (
        notification.get("subscriptions") or []
    )

    destination_ids = [
        item.get("destination_id")
        for item in subscriptions
        if item.get("destination_id")
    ]

    print("")
    print(f"Subscriber: {email}")
    print(f"Alert ID: {alert_id}")
    print(
        f"Display name: "
        f"{alert_detail.get('display_name')}"
    )
    print(
        f"Warehouse ID: "
        f"{alert_detail.get('warehouse_id')}"
    )
    print(
        f"Schedule state: "
        f"{schedule.get('pause_status')}"
    )
    print(
        f"Notification destinations: "
        f"{destination_ids}"
    )


    if schedule.get("pause_status") != "PAUSED":
        raise RuntimeError(
            f"Safety violation: alert for {email} "
            "is not PAUSED."
        )

    expected_destination = (
        alert_info["destination_id"]
    )

    if expected_destination not in destination_ids:
        raise RuntimeError(
            f"Alert for {email} is not connected "
            "to the expected notification destination."
        )


print("")
print(
    "Subscriber SQL Alert configuration "
    "verified successfully."
)

# COMMAND ----------

# COMMAND ----------

# -----------------------------------------------------------------------------
# 10. Inspect existing daily Workflow
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


print("")
print("Daily Workflow inspection complete")
print(f"Workflow name: {JOB_NAME}")
print(f"Matching jobs found: {len(matching_jobs)}")


if matching_jobs:
    for job in matching_jobs:
        print(
            f"- {job.get('settings', {}).get('name')} "
            f"[job_id={job.get('job_id')}]"
        )
else:
    print("No existing daily Workflow was found.")

# COMMAND ----------

# COMMAND ----------

# -----------------------------------------------------------------------------
# 11. Prepare the PAUSED daily Workflow definition
# -----------------------------------------------------------------------------

if len(matching_jobs) > 1:
    raise RuntimeError(
        f"Multiple Workflows named {JOB_NAME!r} exist. "
        "Resolve the duplicates before deployment."
    )

EXISTING_JOB_ID = (
    int(matching_jobs[0]["job_id"])
    if matching_jobs
    else None
)

print(
    "Workflow deployment mode: "
    + (
        f"UPDATE existing job {EXISTING_JOB_ID}"
        if EXISTING_JOB_ID
        else "CREATE new job"
    )
)

# Safety guard during development.
if SCHEDULE_STATE != "PAUSED":
    raise RuntimeError(
        "Development safety guard: schedule_state must remain PAUSED."
    )


RUN_ID_REFERENCE = (
    "{{tasks.validate_daily_run.values.run_id}}"
)


workflow_tasks = [
    {
        "task_key": "validate_daily_run",
        "run_if": "ALL_SUCCESS",
        "notebook_task": {
            "notebook_path": (
                f"{NOTEBOOKS_ROOT}/"
                "01_Register_and_Validate_Daily_Run"
            ),
            "source": "WORKSPACE",
            "base_parameters": {
                "run_date": WORKFLOW_RUN_DATE,
                "bootstrap_registry": "false",
                "fail_on_incomplete": "true",
                "run_trigger": "SCHEDULED",
                "execution_mode": EXECUTION_MODE,
            },
        },
    },

    {
        "task_key": "ingest_sql_diagnostics",
        "depends_on": [
            {"task_key": "validate_daily_run"}
        ],
        "run_if": "ALL_SUCCESS",
        "notebook_task": {
            "notebook_path": (
                f"{NOTEBOOKS_ROOT}/"
                "02_Ingest_SQL_Diagnostics_Workbooks"
            ),
            "source": "WORKSPACE",
            "base_parameters": {
                "run_id": RUN_ID_REFERENCE,
                "allow_incomplete_run": ALLOW_INCOMPLETE_PARAMETER,
                "force_reprocess": "false",
                "fail_on_workbook_error": "true",
            },
        },
    },

    {
        "task_key": "ingest_windows_events",
        "depends_on": [
            {"task_key": "ingest_sql_diagnostics"}
        ],
        "run_if": "ALL_SUCCESS",
        "notebook_task": {
            "notebook_path": (
                f"{NOTEBOOKS_ROOT}/"
                "03_Ingest_Windows_Events"
            ),
            "source": "WORKSPACE",
            "base_parameters": {
                "run_id": RUN_ID_REFERENCE,
                "allow_incomplete_run": ALLOW_INCOMPLETE_PARAMETER,
                "force_reprocess": "false",
            },
        },
    },

    {
        "task_key": "build_analysis_tables",
        "depends_on": [
            {"task_key": "ingest_windows_events"}
        ],
        "run_if": "ALL_SUCCESS",
        "notebook_task": {
            "notebook_path": (
                f"{NOTEBOOKS_ROOT}/"
                "04_Build_Analysis_Ready_Tables"
            ),
            "source": "WORKSPACE",
            "base_parameters": {
                "run_id": RUN_ID_REFERENCE,
                "allow_incomplete_run": ALLOW_INCOMPLETE_PARAMETER,
            },
        },
    },

    {
        "task_key": "evaluate_health_rules",
        "depends_on": [
            {"task_key": "build_analysis_tables"}
        ],
        "run_if": "ALL_SUCCESS",
        "notebook_task": {
            "notebook_path": (
                f"{NOTEBOOKS_ROOT}/"
                "05_Evaluate_Server_Health_Rules"
            ),
            "source": "WORKSPACE",
            "base_parameters": {
                "run_id": RUN_ID_REFERENCE,
                "allow_incomplete_run": ALLOW_INCOMPLETE_PARAMETER,
                "top_issues_per_server": (
                    str(TOP_ISSUES_PER_SERVER)
                ),
            },
        },
    },

    {
        "task_key": "prepare_databricks_alert",
        "depends_on": [
            {"task_key": "evaluate_health_rules"}
        ],
        "run_if": "ALL_SUCCESS",
        "notebook_task": {
            "notebook_path": (
                f"{NOTEBOOKS_ROOT}/"
                "06_Prepare_Databricks_Alert"
            ),
            "source": "WORKSPACE",
            "base_parameters": {
                "run_id": RUN_ID_REFERENCE,
                "top_issues_per_server": (
                    str(TOP_ISSUES_PER_SERVER)
                ),
                "minimum_severity": "HIGH",
            },
        },
    },
]


# One alert-evaluation task per distinct subscriber.
for index, (email, alert_info) in enumerate(
    sorted(created_alerts.items()),
    start=1,
):
    workflow_tasks.append(
        {
            "task_key": (
                f"notify_subscriber_{index:03d}"
            ),

            "depends_on": [
                {
                    "task_key":
                        "prepare_databricks_alert"
                }
            ],

            "run_if": "ALL_SUCCESS",

            "alert_task": {
                "alert_id": str(
                    alert_info["alert_id"]
                ),

                "warehouse_id": WAREHOUSE_ID,

                "subscribers": [
                    {
                        "destination_id":
                            alert_info[
                                "destination_id"
                            ]
                    }
                ],
            },
        }
    )


workflow_spec = {
    "name": JOB_NAME,

    "description": (
        "Daily SQL Server Observability Agent "
        "ingestion, health evaluation, and "
        "subscriber-specific Databricks alerts."
    ),

    "schedule": {
        "quartz_cron_expression":
            "0 15 11 * * ?",

        "timezone_id":
            "America/New_York",

        "pause_status":
            "PAUSED",
    },

    "max_concurrent_runs": 1,

    "queue": {
        "enabled": True,
    },

    "tasks": workflow_tasks,
}


print("")
print("Daily Workflow definition prepared")
print(f"Workflow: {JOB_NAME}")
print(
    "Schedule: 11:15 America/New_York"
)
print(
    f"Schedule state: "
    f"{workflow_spec['schedule']['pause_status']}"
)
print(
    f"Notebook tasks: 6"
)
print(
    f"Subscriber alert tasks: "
    f"{len(workflow_tasks) - 6}"
)
print(
    f"Total tasks: {len(workflow_tasks)}"
)

print("")
print("Task dependency chain:")

for task in workflow_tasks:
    dependencies = [
        item["task_key"]
        for item in task.get(
            "depends_on", []
        )
    ]

    print(
        f"- {task['task_key']} "
        f"<- {dependencies or ['START']}"
    )

# COMMAND ----------

# COMMAND ----------

# -----------------------------------------------------------------------------
# 12. Create or update the PAUSED daily Workflow
# -----------------------------------------------------------------------------

if (
    workflow_spec.get("schedule", {})
    .get("pause_status")
    != "PAUSED"
):
    raise RuntimeError(
        "Safety violation: Workflow must be PAUSED "
        "during deployment testing."
    )


if EXISTING_JOB_ID:

    workspace_client.api_client.do(
        "POST",
        "/api/2.2/jobs/reset",
        body={
            "job_id": EXISTING_JOB_ID,
            "new_settings": workflow_spec,
        },
    )

    CREATED_JOB_ID = EXISTING_JOB_ID

    print("")
    print("Daily Workflow updated successfully")

else:

    create_job_response = (
        workspace_client.api_client.do(
            "POST",
            "/api/2.2/jobs/create",
            body=workflow_spec,
        )
    )

    CREATED_JOB_ID = create_job_response.get(
        "job_id"
    )

    if not CREATED_JOB_ID:
        raise RuntimeError(
            "Databricks did not return a job_id "
            "after Workflow creation."
        )

    print("")
    print("Daily Workflow created successfully")


print(f"Job ID: {CREATED_JOB_ID}")
print(f"Workflow: {JOB_NAME}")
print(
    "Schedule state: "
    f"{workflow_spec['schedule']['pause_status']}"
)
print(
    f"Total tasks: "
    f"{len(workflow_tasks)}"
)

# COMMAND ----------

# COMMAND ----------

# -----------------------------------------------------------------------------
# 13. Verify the created daily Workflow
# -----------------------------------------------------------------------------

job_detail = workspace_client.api_client.do(
    "GET",
    "/api/2.2/jobs/get",
    query={
        "job_id": CREATED_JOB_ID,
    },
)

job_settings = job_detail.get("settings") or {}
job_schedule = job_settings.get("schedule") or {}
job_tasks = job_settings.get("tasks") or []

print("")
print("Created Workflow verification")
print(f"Job ID: {CREATED_JOB_ID}")
print(f"Workflow name: {job_settings.get('name')}")
print(f"Task count: {len(job_tasks)}")
print(
    "Schedule: "
    f"{job_schedule.get('quartz_cron_expression')}"
)
print(
    "Timezone: "
    f"{job_schedule.get('timezone_id')}"
)
print(
    "Schedule state: "
    f"{job_schedule.get('pause_status')}"
)

print("")
print("Configured tasks:")

for task in job_tasks:
    dependencies = [
        dependency.get("task_key")
        for dependency in task.get("depends_on", [])
    ]

    task_type = (
        "ALERT"
        if "alert_task" in task
        else "NOTEBOOK"
        if "notebook_task" in task
        else "OTHER"
    )

    print(
        f"- {task.get('task_key')} "
        f"[{task_type}] "
        f"<- {dependencies or ['START']}"
    )


# -------------------------------------------------------------------------
# Safety and structural validation
# -------------------------------------------------------------------------

if job_settings.get("name") != JOB_NAME:
    raise RuntimeError(
        "Created Workflow name does not match JOB_NAME."
    )

if len(job_tasks) != len(workflow_tasks):
    raise RuntimeError(
        "Created Workflow task count does not match "
        f"the prepared definition: "
        f"{len(job_tasks)} != {len(workflow_tasks)}"
    )

if job_schedule.get("pause_status") != "PAUSED":
    raise RuntimeError(
        "Safety violation: created Workflow is not PAUSED."
    )

if job_schedule.get("timezone_id") != "America/New_York":
    raise RuntimeError(
        "Created Workflow has the wrong timezone."
    )

alert_tasks = [
    task
    for task in job_tasks
    if "alert_task" in task
]

if len(alert_tasks) != len(created_alerts):
    raise RuntimeError(
        "Subscriber alert task count does not match "
        "the configured subscriber count."
    )


# -------------------------------------------------------------------------
# Verify TEST / PRODUCTION task parameters
# -------------------------------------------------------------------------

tasks_by_key = {
    task["task_key"]: task
    for task in job_tasks
}

validate_params = (
    tasks_by_key["validate_daily_run"]
    ["notebook_task"]
    .get("base_parameters", {})
)

if validate_params.get("run_date") != WORKFLOW_RUN_DATE:
    raise RuntimeError(
        "validate_daily_run run_date was not "
        "persisted correctly. "
        f"Actual={validate_params.get('run_date')!r}, "
        f"expected={WORKFLOW_RUN_DATE!r}."
    )

if validate_params.get("execution_mode") != EXECUTION_MODE:
    raise RuntimeError(
        "validate_daily_run execution_mode was not "
        "persisted correctly."
    )


test_aware_tasks = [
    "ingest_sql_diagnostics",
    "ingest_windows_events",
    "build_analysis_tables",
    "evaluate_health_rules",
]

for task_key in test_aware_tasks:

    task_params = (
        tasks_by_key[task_key]
        ["notebook_task"]
        .get("base_parameters", {})
    )

    actual_value = task_params.get(
        "allow_incomplete_run"
    )

    if actual_value != ALLOW_INCOMPLETE_PARAMETER:
        raise RuntimeError(
            f"{task_key} has allow_incomplete_run="
            f"{actual_value!r}; expected "
            f"{ALLOW_INCOMPLETE_PARAMETER!r}."
        )


print("")
print("Workflow execution parameters verified")
print(f"Execution mode: {EXECUTION_MODE}")
print(
    "Workflow run date: "
    f"{validate_params.get('run_date') or 'CURRENT BUSINESS DATE'}"
)
print(
    "allow_incomplete_run: "
    f"{ALLOW_INCOMPLETE_PARAMETER}"
)


print("")
print(
    "Created daily Workflow verified successfully."
)