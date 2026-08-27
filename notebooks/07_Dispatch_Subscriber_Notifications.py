# Databricks notebook source
# 07_Dispatch_Subscriber_Notifications
#
# Final runtime task for the SQL Server Observability Agent.
#
# This notebook intentionally reads the CURRENT active subscription table on
# every daily run. A client can therefore add or remove subscribers in the
# Streamlit app without rebuilding the production Workflow.

from __future__ import annotations

import json
import re
import time
from datetime import datetime, timezone

from databricks.sdk import WorkspaceClient
from pyspark.sql import functions as F

workspace_client = WorkspaceClient()


# -----------------------------------------------------------------------------
# 1. Runtime parameters
# -----------------------------------------------------------------------------

dbutils.widgets.text(
    "run_id",
    "",
    "Run ID prepared by the upstream Agent tasks",
)

dbutils.widgets.text(
    "warehouse_id",
    "47bde9279fec4222",
    "SQL warehouse used to evaluate subscriber alerts",
)

dbutils.widgets.text(
    "alert_name",
    "prod-server-observability-agent-priority",
    "Base Databricks SQL Alert name",
)

dbutils.widgets.text(
    "dispatch_timeout_minutes",
    "20",
    "Maximum wait for the one-time subscriber alert run",
)

CATALOG = "ent_log_analytics"
SCHEMA = "observability"

RUN_ID = dbutils.widgets.get("run_id").strip()
WAREHOUSE_ID = dbutils.widgets.get("warehouse_id").strip()
ALERT_NAME = dbutils.widgets.get("alert_name").strip()
DISPATCH_TIMEOUT_MINUTES = int(
    dbutils.widgets.get("dispatch_timeout_minutes").strip()
)

if not RUN_ID:
    raise ValueError("run_id is required.")
if not WAREHOUSE_ID:
    raise ValueError("warehouse_id is required.")
if not ALERT_NAME:
    raise ValueError("alert_name is required.")
if not 1 <= DISPATCH_TIMEOUT_MINUTES <= 120:
    raise ValueError("dispatch_timeout_minutes must be between 1 and 120.")

ALERT_SUBSCRIPTIONS_TABLE = (
    f"`{CATALOG}`.`{SCHEMA}`.`agent_alert_subscriptions`"
)
ROUTED_ALERT_VIEW = (
    f"`{CATALOG}`.`{SCHEMA}`.`v_agent_databricks_alert_routes`"
)
INVENTORY_TABLE = (
    f"`{CATALOG}`.`{SCHEMA}`.`agent_server_daily_inventory`"
)
RUNS_TABLE = f"`{CATALOG}`.`{SCHEMA}`.`agent_ingestion_runs`"

print("Dynamic subscriber notification dispatch")
print(f"Run ID: {RUN_ID}")
print(f"Warehouse ID: {WAREHOUSE_ID}")


# -----------------------------------------------------------------------------
# 2. Production run gate
# -----------------------------------------------------------------------------

run_rows = spark.sql(
    f"""
    SELECT run_status
    FROM {RUNS_TABLE}
    WHERE run_id = '{RUN_ID}'
    ORDER BY updated_ts DESC
    LIMIT 1
    """
).collect()

if not run_rows:
    raise RuntimeError(f"Run {RUN_ID} does not exist.")

RUN_STATUS = str(run_rows[0]["run_status"])

if RUN_STATUS != "HEALTH_RULES_EVALUATED":
    raise RuntimeError(
        f"Run {RUN_ID} is not ready for production notification dispatch. "
        f"Current status: {RUN_STATUS}."
    )


# -----------------------------------------------------------------------------
# 3. Read CURRENT active subscriptions
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
    ORDER BY subscriber_email, canonical_server_name
    """
)

ACTIVE_ROUTE_COUNT = active_subscriptions_df.count()

if ACTIVE_ROUTE_COUNT == 0:
    print("No active notification subscriptions. Nothing to dispatch.")
    try:
        dbutils.jobs.taskValues.set(key="dispatch_status", value="NO_SUBSCRIBERS")
        dbutils.jobs.taskValues.set(key="subscriber_count", value=0)
    except Exception:
        pass
    dbutils.notebook.exit("NO_SUBSCRIBERS")

subscriber_emails = [
    row["subscriber_email"]
    for row in (
        active_subscriptions_df
        .select("subscriber_email")
        .distinct()
        .orderBy("subscriber_email")
        .collect()
    )
]

print(f"Active server-recipient routes: {ACTIVE_ROUTE_COUNT}")
print(f"Active subscribers: {len(subscriber_emails)}")


# -----------------------------------------------------------------------------
# 4. Create or reuse Databricks email notification destinations
# -----------------------------------------------------------------------------

notification_destinations = []
page_token = None

while True:
    query = {"page_size": 100}
    if page_token:
        query["page_token"] = page_token

    response = workspace_client.api_client.do(
        "GET",
        "/api/2.0/notification-destinations",
        query=query,
    )

    notification_destinations.extend(response.get("results", []) or [])
    page_token = response.get("next_page_token")
    if not page_token:
        break

existing_destinations_by_name = {
    item.get("display_name"): item
    for item in notification_destinations
    if item.get("display_name")
}

resolved_destinations: dict[str, str] = {}

for email in subscriber_emails:
    display_name = f"SQL Observability Agent - {email}"
    existing = existing_destinations_by_name.get(display_name)

    if existing:
        destination_id = existing.get("id")
        destination_type = str(existing.get("destination_type", "")).upper()

        if destination_type != "EMAIL" or not destination_id:
            raise RuntimeError(
                f"Existing notification destination {display_name!r} is invalid."
            )

        print(f"Using destination: {display_name} [{destination_id}]")

    else:
        spec = {
            "display_name": display_name,
            "config": {
                "email": {
                    "addresses": [email]
                }
            },
        }

        try:
            created = workspace_client.api_client.do(
                "POST",
                "/api/2.0/notification-destinations",
                body=spec,
            )
        except Exception as exc:
            raise RuntimeError(
                "Could not automatically create the Databricks email "
                f"notification destination for {email}. The identity running "
                "the production Workflow must be allowed to manage workspace "
                "notification destinations."
            ) from exc

        destination_id = created.get("id")
        if not destination_id:
            raise RuntimeError(
                f"Databricks returned no destination ID for {email}."
            )

        print(f"Created destination: {display_name} [{destination_id}]")

    resolved_destinations[email] = str(destination_id)


# Persist destination IDs so the Streamlit subscription table remains auditable.
destination_rows = [
    (email, destination_id)
    for email, destination_id in resolved_destinations.items()
]

destination_df = spark.createDataFrame(
    destination_rows,
    ["subscriber_email", "notification_destination_id"],
)
destination_df.createOrReplaceTempView("_agent_dynamic_destinations")

spark.sql(
    f"""
    MERGE INTO {ALERT_SUBSCRIPTIONS_TABLE} AS target
    USING _agent_dynamic_destinations AS source
       ON lower(trim(target.subscriber_email)) = source.subscriber_email
    WHEN MATCHED AND target.is_active = true
    THEN UPDATE SET
        target.notification_destination_id = source.notification_destination_id,
        target.updated_ts = current_timestamp()
    """
)

spark.catalog.dropTempView("_agent_dynamic_destinations")


# -----------------------------------------------------------------------------
# 5. Inspect existing subscriber SQL Alerts
# -----------------------------------------------------------------------------

existing_alerts = []
page_token = None

while True:
    query = {"page_size": 100}
    if page_token:
        query["page_token"] = page_token

    response = workspace_client.api_client.do(
        "GET",
        "/api/2.0/alerts",
        query=query,
    )

    existing_alerts.extend(
        response.get("alerts")
        or response.get("results")
        or []
    )

    page_token = response.get("next_page_token")
    if not page_token:
        break

existing_agent_alerts_by_name = {
    alert.get("display_name"): alert
    for alert in existing_alerts
    if str(alert.get("display_name", "")).startswith(ALERT_NAME)
}


# -----------------------------------------------------------------------------
# 6. Create/update one PAUSED alert per CURRENT subscriber
# -----------------------------------------------------------------------------

resolved_alerts: dict[str, dict[str, str]] = {}

for email in subscriber_emails:
    destination_id = resolved_destinations[email]
    alert_display_name = f"{ALERT_NAME} - {email}"
    email_sql = email.replace("'", "''")

    # Important production filter:
    # a subscriber is emailed only for subscribed servers that have BOTH a
    # valid SQL workbook and Windows Events evidence in the same Agent run.
    alert_query = f"""
    SELECT
        1 AS alert_trigger,
        routes.snapshot_date,
        routes.canonical_server_name,

        CASE
            WHEN upper(routes.health_status) = 'CRITICAL'
                THEN concat('🔴 ', routes.health_status)
            WHEN upper(routes.health_status) = 'HIGH'
                THEN concat('🟠 ', routes.health_status)
            WHEN upper(routes.health_status) = 'DATA_INCOMPLETE'
                THEN concat('🟡 ', routes.health_status)
            WHEN upper(routes.health_status) IN ('HEALTHY', 'OK')
                THEN concat('🟢 ', routes.health_status)
            ELSE routes.health_status
        END AS health_status_display,

        CASE
            WHEN routes.critical_issue_count > 0
                THEN concat('🔴 ', CAST(routes.critical_issue_count AS STRING))
            ELSE CAST(routes.critical_issue_count AS STRING)
        END AS critical_issue_count_display,

        CASE
            WHEN routes.high_issue_count > 0
                THEN concat('🟠 ', CAST(routes.high_issue_count AS STRING))
            ELSE CAST(routes.high_issue_count AS STRING)
        END AS high_issue_count_display,

        CAST(routes.priority_issue_count AS STRING)
            AS priority_issue_count_display,

        replace(
            replace(
                replace(
                    routes.priority_briefing,
                    '\\n',
                    '<br>'
                ),
                '[CRITICAL]',
                '🔴 [CRITICAL]'
            ),
            '[HIGH]',
            '🟠 [HIGH]'
        ) AS priority_briefing_display

    FROM {ROUTED_ALERT_VIEW} AS routes

    INNER JOIN {INVENTORY_TABLE} AS inventory
        ON inventory.run_id = routes.run_id
       AND inventory.canonical_server_name = routes.canonical_server_name

    WHERE routes.run_id = '{RUN_ID}'
      AND lower(trim(routes.subscriber_email)) = '{email_sql}'
      AND inventory.inventory_status = 'COMPLETE'

    ORDER BY
        routes.critical_issue_count DESC,
        routes.high_issue_count DESC,
        routes.canonical_server_name
    """.strip()

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
                    {"destination_id": destination_id}
                ],
                "retrigger_seconds": 1,
                "notify_on_ok": False,
            },
        },
        # The underlying alert never runs on its own. The daily Workflow
        # dispatch task evaluates it after the Agent analysis is complete.
        "schedule": {
            "quartz_cron_schedule": "0 30 11 * * ?",
            "timezone_id": "America/New_York",
            "pause_status": "PAUSED",
        },
        "custom_summary": (
            "SQL Server Observability Agent - Priority Findings"
        ),
        "custom_description": (
            "<h2>SQL Server Observability Agent</h2>"
            "<p>Priority findings requiring attention are shown below.</p>"
            "{{#QUERY_RESULT_ROWS}}"
            "<hr>"
            "<h3>{{canonical_server_name}}</h3>"
            "<p>"
            "<b>Snapshot date:</b> {{snapshot_date}}<br>"
            "<b>Health status:</b> {{health_status_display}}<br>"
            "<b>Critical issues:</b> {{critical_issue_count_display}}<br>"
            "<b>High issues:</b> {{high_issue_count_display}}<br>"
            "<b>Priority findings:</b> {{priority_issue_count_display}}"
            "</p>"
            "<h4>Priority briefing</h4>"
            "<div>{{priority_briefing_display}}</div>"
            "{{/QUERY_RESULT_ROWS}}"
        ),
    }

    existing = existing_agent_alerts_by_name.get(alert_display_name)

    if existing:
        alert_id = existing.get("id")
        if not alert_id:
            raise RuntimeError(
                f"Existing alert {alert_display_name!r} has no ID."
            )

        workspace_client.api_client.do(
            "PATCH",
            f"/api/2.0/alerts/{alert_id}",
            query={
                "update_mask": (
                    "display_name,query_text,warehouse_id,evaluation,"
                    "schedule,custom_summary,custom_description"
                )
            },
            body=alert_spec,
        )

        print(f"Updated subscriber alert: {alert_display_name} [{alert_id}]")

    else:
        created = workspace_client.api_client.do(
            "POST",
            "/api/2.0/alerts",
            body=alert_spec,
        )
        alert_id = created.get("id")

        if not alert_id:
            raise RuntimeError(
                f"Databricks returned no alert ID for {email}."
            )

        print(f"Created subscriber alert: {alert_display_name} [{alert_id}]")

    resolved_alerts[email] = {
        "alert_id": str(alert_id),
        "destination_id": destination_id,
    }


# -----------------------------------------------------------------------------
# 7. Submit one-time alert-evaluation tasks for CURRENT subscribers
# -----------------------------------------------------------------------------

# The saved production Workflow stays fixed. This one-time child run is built
# from the subscription table at runtime, which is what removes the need to
# rerun the Workflow-creation notebook whenever subscribers change.

def safe_task_key(email: str, index: int) -> str:
    local = re.sub(r"[^A-Za-z0-9_]", "_", email)
    return f"subscriber_{index:03d}_{local}"[:100]


dispatch_tasks = []

for index, email in enumerate(sorted(resolved_alerts), start=1):
    info = resolved_alerts[email]
    dispatch_tasks.append(
        {
            "task_key": safe_task_key(email, index),
            "alert_task": {
                "alert_id": info["alert_id"],
                "warehouse_id": WAREHOUSE_ID,
                "subscribers": [
                    {
                        "destination_id": info["destination_id"]
                    }
                ],
            },
        }
    )

submit_response = workspace_client.api_client.do(
    "POST",
    "/api/2.2/jobs/runs/submit",
    body={
        "run_name": (
            "sql-observability-subscriber-dispatch-"
            + RUN_ID.lower()
        ),
        "tasks": dispatch_tasks,
    },
)

DISPATCH_RUN_ID = submit_response.get("run_id")

if not DISPATCH_RUN_ID:
    raise RuntimeError(
        "Databricks did not return a run_id for subscriber dispatch."
    )

print("")
print(f"Submitted subscriber dispatch run: {DISPATCH_RUN_ID}")
print(f"Alert tasks submitted: {len(dispatch_tasks)}")


# -----------------------------------------------------------------------------
# 8. Wait for subscriber alert evaluation to finish
# -----------------------------------------------------------------------------

terminal_lifecycle_states = {
    "TERMINATED",
    "SKIPPED",
    "INTERNAL_ERROR",
}

deadline = time.time() + DISPATCH_TIMEOUT_MINUTES * 60
last_state = None

while time.time() < deadline:
    run_detail = workspace_client.api_client.do(
        "GET",
        "/api/2.2/jobs/runs/get",
        query={"run_id": DISPATCH_RUN_ID},
    )

    state = run_detail.get("state") or {}
    lifecycle = str(state.get("life_cycle_state") or "")
    result_state = str(state.get("result_state") or "")

    current_state = (lifecycle, result_state)
    if current_state != last_state:
        print(
            "Dispatch run state: "
            f"life_cycle={lifecycle or 'UNKNOWN'}, "
            f"result={result_state or 'PENDING'}"
        )
        last_state = current_state

    if lifecycle in terminal_lifecycle_states:
        if lifecycle == "TERMINATED" and result_state == "SUCCESS":
            break

        state_message = state.get("state_message") or "No detail returned."
        raise RuntimeError(
            "Subscriber alert dispatch failed. "
            f"life_cycle={lifecycle}; result={result_state}; "
            f"detail={state_message}"
        )

    time.sleep(5)
else:
    raise TimeoutError(
        "Timed out waiting for subscriber alert dispatch run "
        f"{DISPATCH_RUN_ID}."
    )


# -----------------------------------------------------------------------------
# 9. Publish runtime summary
# -----------------------------------------------------------------------------

# Count only CURRENT run/server/subscriber routes that have complete SQL +
# Windows evidence. Alerts with no qualifying priority findings simply resolve
# OK and do not send an email.
eligible_route_count = spark.sql(
    f"""
    SELECT COUNT(*) AS route_count
    FROM {ROUTED_ALERT_VIEW} AS routes
    INNER JOIN {INVENTORY_TABLE} AS inventory
        ON inventory.run_id = routes.run_id
       AND inventory.canonical_server_name = routes.canonical_server_name
    WHERE routes.run_id = '{RUN_ID}'
      AND inventory.inventory_status = 'COMPLETE'
    """
).first()["route_count"]

result = {
    "dispatch_status": "SUCCESS",
    "run_id": RUN_ID,
    "subscriber_count": len(subscriber_emails),
    "active_subscription_route_count": ACTIVE_ROUTE_COUNT,
    "eligible_priority_route_count": int(eligible_route_count or 0),
    "dispatch_run_id": int(DISPATCH_RUN_ID),
    "completed_ts": datetime.now(timezone.utc).isoformat(),
}

try:
    for key, value in result.items():
        dbutils.jobs.taskValues.set(key=key, value=value)
except Exception:
    pass

print("")
print("Dynamic subscriber dispatch completed successfully")
print(json.dumps(result, indent=2))
