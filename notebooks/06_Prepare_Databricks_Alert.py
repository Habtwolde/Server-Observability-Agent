# Databricks notebook source
from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from typing import Any

from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from pyspark.sql.window import Window


# -----------------------------------------------------------------------------
# 1. Runtime parameters
# -----------------------------------------------------------------------------

dbutils.widgets.text(
    "run_id",
    "",
    "Run ID (blank = latest run)",
)

dbutils.widgets.dropdown(
    "top_issues_per_server",
    "5",
    ["3", "5"],
    "Priority issues per server",
)

dbutils.widgets.dropdown(
    "minimum_severity",
    "HIGH",
    ["CRITICAL", "HIGH"],
    "Minimum severity included",
)


CATALOG = "ent_log_analytics"
SCHEMA = "observability"

ALERT_PAYLOAD_TABLE = "agent_alert_payload"
NOTIFICATION_LOG_TABLE = "agent_notification_log"


def table_name(name: str) -> str:
    return f"`{CATALOG}`.`{SCHEMA}`.`{name}`"


def widget_int(name: str, minimum: int, maximum: int) -> int:
    raw_value = dbutils.widgets.get(name).strip()

    try:
        value = int(raw_value)
    except ValueError as exc:
        raise ValueError(
            f"Widget {name} must be an integer: {raw_value!r}"
        ) from exc

    if not minimum <= value <= maximum:
        raise ValueError(
            f"Widget {name} must be between {minimum} and {maximum}: {value}"
        )

    return value


RUN_ID_PARAMETER = dbutils.widgets.get("run_id").strip()

TOP_ISSUES_PER_SERVER = widget_int(
    "top_issues_per_server",
    1,
    5,
)

MINIMUM_SEVERITY = (
    dbutils.widgets.get("minimum_severity")
    .strip()
    .upper()
)

SEVERITY_RANK = {
    "LOW": 1,
    "MEDIUM": 2,
    "HIGH": 3,
    "CRITICAL": 4,
}

if MINIMUM_SEVERITY not in {"HIGH", "CRITICAL"}:
    raise ValueError(
        f"Unsupported production alert severity: {MINIMUM_SEVERITY}"
    )


print("Databricks native alert preparation")
print(f"Run ID parameter: {RUN_ID_PARAMETER or 'latest run'}")
print(f"Minimum severity: {MINIMUM_SEVERITY}")
print(f"Top issues per server: {TOP_ISSUES_PER_SERVER}")

# COMMAND ----------

# -----------------------------------------------------------------------------
# 2. Select and gate the Agent run
# -----------------------------------------------------------------------------

runs_df = spark.table(
    f"{CATALOG}.{SCHEMA}.agent_ingestion_runs"
)

if RUN_ID_PARAMETER:
    run_rows = (
        runs_df
        .where(F.col("run_id") == RUN_ID_PARAMETER)
        .orderBy(F.col("updated_ts").desc())
        .limit(1)
        .collect()
    )
else:
    run_rows = (
        runs_df
        .orderBy(
            F.col("run_date").desc(),
            F.col("updated_ts").desc(),
        )
        .limit(1)
        .collect()
    )


if not run_rows:
    raise RuntimeError(
        f"Agent run was not found: "
        f"{RUN_ID_PARAMETER or 'latest run'}"
    )


run_row = run_rows[0]

RUN_ID = str(run_row["run_id"])
RUN_DATE = run_row["run_date"]
RUN_STATUS = str(run_row["run_status"])


PRODUCTION_READY_STATUS = "HEALTH_RULES_EVALUATED"
TEST_READY_STATUS = "TEST_HEALTH_RULES_EVALUATED"

SUPPORTED_RUN_STATUSES = {
    PRODUCTION_READY_STATUS,
    TEST_READY_STATUS,
}


if RUN_STATUS not in SUPPORTED_RUN_STATUSES:
    raise RuntimeError(
        f"Run {RUN_ID} has status {RUN_STATUS}. "
        "Databricks alert preparation requires the health-rule "
        "evaluation to finish first."
    )


IS_TEST_RUN = RUN_STATUS == TEST_READY_STATUS

ALERT_ELIGIBLE = RUN_STATUS == PRODUCTION_READY_STATUS

SUPPRESSION_REASON = (
    "TEST_RUN"
    if IS_TEST_RUN
    else None
)


print("")
print("Selected Agent run")
print(f"Run ID: {RUN_ID}")
print(f"Run date: {RUN_DATE}")
print(f"Run status: {RUN_STATUS}")
print(f"Test run: {IS_TEST_RUN}")
print(f"Production alert eligible: {ALERT_ELIGIBLE}")

if SUPPRESSION_REASON:
    print(
        f"Production notification will be suppressed: "
        f"{SUPPRESSION_REASON}"
    )

# COMMAND ----------

# -----------------------------------------------------------------------------
# 3. Create Databricks native-alert payload table
# -----------------------------------------------------------------------------

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {table_name(ALERT_PAYLOAD_TABLE)} (
        payload_id STRING NOT NULL,
        run_id STRING NOT NULL,
        snapshot_date DATE NOT NULL,
        run_status STRING NOT NULL,

        alert_eligible BOOLEAN NOT NULL,
        suppression_reason STRING,

        minimum_severity STRING NOT NULL,
        top_issues_per_server INT NOT NULL,

        canonical_server_name STRING NOT NULL,
        health_status STRING,
        health_score DOUBLE,

        critical_issue_count INT NOT NULL,
        high_issue_count INT NOT NULL,
        priority_issue_count INT NOT NULL,

        priority_briefing STRING NOT NULL,

        prepared_ts TIMESTAMP NOT NULL
    )
    USING DELTA
    COMMENT 'Run-scoped SQL Server Observability Agent payload for Databricks native alert notifications'
    TBLPROPERTIES (
        'delta.enableChangeDataFeed' = 'true',
        'quality' = 'operations',
        'agent.owner' = 'sql-server-observability-agent'
    )
    """
)


print("")
print("Native alert payload table ready:")
print(
    f"{CATALOG}.{SCHEMA}.{ALERT_PAYLOAD_TABLE}"
)

# COMMAND ----------

# -----------------------------------------------------------------------------
# 4. Select and rank priority findings for the exact Agent run
# -----------------------------------------------------------------------------

minimum_rank = SEVERITY_RANK[MINIMUM_SEVERITY]

severity_value = (
    F.when(F.col("severity") == "CRITICAL", F.lit(4))
    .when(F.col("severity") == "HIGH", F.lit(3))
    .when(F.col("severity") == "MEDIUM", F.lit(2))
    .when(F.col("severity") == "LOW", F.lit(1))
    .otherwise(F.lit(0))
)


ranking_window = Window.partitionBy(
    "canonical_server_name"
).orderBy(
    severity_value.desc(),
    F.col("priority_score").desc(),
    F.col("rule_id").asc(),
    F.coalesce(F.col("entity_name"), F.lit("")).asc(),
)


priority_findings_df = (
    spark.table(
        f"{CATALOG}.{SCHEMA}.agent_findings"
    )
    .where(
        (F.col("run_id") == RUN_ID)
        & (F.col("finding_status") == "OPEN")
        & (severity_value >= F.lit(minimum_rank))
    )
    .withColumn(
        "finding_rank",
        F.row_number().over(ranking_window),
    )
    .where(
        F.col("finding_rank")
        <= F.lit(TOP_ISSUES_PER_SERVER)
    )
    .select(
        "run_id",
        "snapshot_date",
        "canonical_server_name",
        "finding_rank",
        "severity",
        "priority_score",
        "domain",
        "rule_id",
        "rule_title",
        "entity_name",
        "finding_summary",
        "likely_cause",
        "recommended_action",
        "microsoft_reference_url",
        "source_observed_ts",
    )
)


PRIORITY_ISSUE_COUNT = priority_findings_df.count()

AFFECTED_SERVER_COUNT = (
    priority_findings_df
    .select("canonical_server_name")
    .distinct()
    .count()
)


print("")
print("Priority finding selection complete")
print(f"Run ID: {RUN_ID}")
print(f"Minimum severity: {MINIMUM_SEVERITY}")
print(f"Top issues per server: {TOP_ISSUES_PER_SERVER}")
print(f"Priority findings selected: {PRIORITY_ISSUE_COUNT}")
print(f"Affected servers: {AFFECTED_SERVER_COUNT}")


display(
    priority_findings_df.orderBy(
        "canonical_server_name",
        "finding_rank",
    )
)

# COMMAND ----------

# -----------------------------------------------------------------------------
# 5. Build one native-alert briefing row per affected server
# -----------------------------------------------------------------------------

finding_text = F.concat(
    F.lit("#"),
    F.col("finding_rank").cast("string"),
    F.lit(" ["),
    F.col("severity"),
    F.lit("] "),
    F.col("rule_title"),
    F.lit("\nEvidence: "),
    F.col("finding_summary"),
    F.lit("\nLikely cause: "),
    F.col("likely_cause"),
    F.lit("\nImmediate action: "),
    F.col("recommended_action"),
    F.lit("\nMicrosoft reference: "),
    F.coalesce(
        F.col("microsoft_reference_url"),
        F.lit("Not supplied"),
    ),
)


server_findings_df = (
    priority_findings_df
    .withColumn(
        "_finding_text",
        finding_text,
    )
    .groupBy(
        "run_id",
        "snapshot_date",
        "canonical_server_name",
    )
    .agg(
        F.sum(
            F.when(
                F.col("severity") == "CRITICAL",
                F.lit(1),
            ).otherwise(F.lit(0))
        ).cast("int").alias("critical_issue_count"),

        F.sum(
            F.when(
                F.col("severity") == "HIGH",
                F.lit(1),
            ).otherwise(F.lit(0))
        ).cast("int").alias("high_issue_count"),

        F.count("*")
        .cast("int")
        .alias("priority_issue_count"),

        F.sort_array(
            F.collect_list(
                F.struct(
                    F.col("finding_rank"),
                    F.col("_finding_text").alias("finding_text"),
                )
            )
        ).alias("_ranked_findings"),
    )
    .withColumn(
        "priority_briefing",
        F.expr(
            """
            concat_ws(
                '\n\n',
                transform(
                    _ranked_findings,
                    item -> item.finding_text
                )
            )
            """
        ),
    )
    .drop("_ranked_findings")
)


health_df = (
    spark.table(
        f"{CATALOG}.{SCHEMA}.agent_server_health_summary"
    )
    .where(
        F.col("run_id") == RUN_ID
    )
    .select(
        "run_id",
        "canonical_server_name",
        "health_status",
        "health_score",
    )
)


server_payload_df = (
    server_findings_df
    .join(
        health_df,
        on=[
            "run_id",
            "canonical_server_name",
        ],
        how="left",
    )
    .withColumn(
        "run_status",
        F.lit(RUN_STATUS),
    )
    .withColumn(
        "alert_eligible",
        F.lit(ALERT_ELIGIBLE),
    )
    .withColumn(
        "suppression_reason",
        F.lit(SUPPRESSION_REASON).cast("string"),
    )
    .withColumn(
        "minimum_severity",
        F.lit(MINIMUM_SEVERITY),
    )
    .withColumn(
        "top_issues_per_server",
        F.lit(TOP_ISSUES_PER_SERVER).cast("int"),
    )
)


SERVER_PAYLOAD_COUNT = server_payload_df.count()

MAX_FINDINGS_IN_SERVER = (
    server_payload_df
    .agg(
        F.max("priority_issue_count")
        .alias("max_priority_issue_count")
    )
    .first()["max_priority_issue_count"]
    or 0
)


if SERVER_PAYLOAD_COUNT != AFFECTED_SERVER_COUNT:
    raise RuntimeError(
        "Payload row count does not match affected server count: "
        f"{SERVER_PAYLOAD_COUNT} != {AFFECTED_SERVER_COUNT}"
    )


if MAX_FINDINGS_IN_SERVER > TOP_ISSUES_PER_SERVER:
    raise RuntimeError(
        "A server exceeded the configured priority finding limit: "
        f"{MAX_FINDINGS_IN_SERVER} > {TOP_ISSUES_PER_SERVER}"
    )


print("")
print("Per-server alert payload prepared")
print(f"Payload rows: {SERVER_PAYLOAD_COUNT}")
print(f"Expected affected servers: {AFFECTED_SERVER_COUNT}")
print(
    f"Maximum findings in one server: "
    f"{MAX_FINDINGS_IN_SERVER}"
)
print(f"Production alert eligible: {ALERT_ELIGIBLE}")
print(
    f"Suppression reason: "
    f"{SUPPRESSION_REASON or 'None'}"
)


display(
    server_payload_df
    .select(
        "canonical_server_name",
        "health_status",
        "health_score",
        "critical_issue_count",
        "high_issue_count",
        "priority_issue_count",
        "alert_eligible",
        "suppression_reason",
        "priority_briefing",
    )
    .orderBy(
        F.col("critical_issue_count").desc(),
        F.col("high_issue_count").desc(),
        F.col("canonical_server_name"),
    )
)

# COMMAND ----------

# -----------------------------------------------------------------------------
# 6. Persist the run-scoped native-alert payload
# -----------------------------------------------------------------------------

payload_to_write_df = (
    server_payload_df
    .withColumn(
        "payload_id",
        F.sha2(
            F.concat_ws(
                "||",
                F.col("run_id"),
                F.col("canonical_server_name"),
                F.lit("DATABRICKS_NATIVE_ALERT"),
            ),
            256,
        ),
    )
    .withColumn(
        "prepared_ts",
        F.current_timestamp(),
    )
    .select(
        "payload_id",
        "run_id",
        "snapshot_date",
        "run_status",
        "alert_eligible",
        "suppression_reason",
        "minimum_severity",
        "top_issues_per_server",
        "canonical_server_name",
        "health_status",
        "health_score",
        "critical_issue_count",
        "high_issue_count",
        "priority_issue_count",
        "priority_briefing",
        "prepared_ts",
    )
)


payload_to_write_df.createOrReplaceTempView(
    "_agent_native_alert_payload"
)


# Remove any older payload rows for this same run that are no longer
# present in the newly prepared result.
spark.sql(
    f"""
    DELETE FROM {table_name(ALERT_PAYLOAD_TABLE)}
    WHERE run_id = '{RUN_ID}'
      AND payload_id NOT IN (
          SELECT payload_id
          FROM _agent_native_alert_payload
      )
    """
)


# Insert new rows or refresh existing rows for this run/server.
spark.sql(
    f"""
    MERGE INTO {table_name(ALERT_PAYLOAD_TABLE)} AS target
    USING _agent_native_alert_payload AS source
       ON target.payload_id = source.payload_id

    WHEN MATCHED THEN UPDATE SET
        target.run_id = source.run_id,
        target.snapshot_date = source.snapshot_date,
        target.run_status = source.run_status,
        target.alert_eligible = source.alert_eligible,
        target.suppression_reason = source.suppression_reason,
        target.minimum_severity = source.minimum_severity,
        target.top_issues_per_server = source.top_issues_per_server,
        target.canonical_server_name = source.canonical_server_name,
        target.health_status = source.health_status,
        target.health_score = source.health_score,
        target.critical_issue_count = source.critical_issue_count,
        target.high_issue_count = source.high_issue_count,
        target.priority_issue_count = source.priority_issue_count,
        target.priority_briefing = source.priority_briefing,
        target.prepared_ts = source.prepared_ts

    WHEN NOT MATCHED THEN INSERT *
    """
)


spark.catalog.dropTempView(
    "_agent_native_alert_payload"
)


persisted_df = (
    spark.table(
        f"{CATALOG}.{SCHEMA}.{ALERT_PAYLOAD_TABLE}"
    )
    .where(
        F.col("run_id") == RUN_ID
    )
)


PERSISTED_ROW_COUNT = persisted_df.count()

if PERSISTED_ROW_COUNT != SERVER_PAYLOAD_COUNT:
    raise RuntimeError(
        "Persisted alert payload row count does not match "
        f"prepared payload count: "
        f"{PERSISTED_ROW_COUNT} != {SERVER_PAYLOAD_COUNT}"
    )


print("")
print("Native alert payload persisted")
print(f"Run ID: {RUN_ID}")
print(f"Rows persisted: {PERSISTED_ROW_COUNT}")
print(f"Alert eligible: {ALERT_ELIGIBLE}")
print(
    f"Suppression reason: "
    f"{SUPPRESSION_REASON or 'None'}"
)


display(
    persisted_df
    .select(
        "canonical_server_name",
        "health_status",
        "critical_issue_count",
        "high_issue_count",
        "priority_issue_count",
        "alert_eligible",
        "suppression_reason",
        "prepared_ts",
    )
    .orderBy(
        F.col("critical_issue_count").desc(),
        F.col("high_issue_count").desc(),
        F.col("canonical_server_name"),
    )
)

# COMMAND ----------

# -----------------------------------------------------------------------------
# 7. Create the production Databricks Alert consumption view
# -----------------------------------------------------------------------------

ALERT_VIEW = "v_agent_databricks_alert_payload"


spark.sql(
    f"""
    CREATE OR REPLACE VIEW {table_name(ALERT_VIEW)} AS

    WITH latest_run AS (
        SELECT
            run_id,
            run_date,
            run_status
        FROM {table_name('agent_ingestion_runs')}
        ORDER BY
            run_date DESC,
            updated_ts DESC
        LIMIT 1
    )

    SELECT
        payload.run_id,
        payload.snapshot_date,
        payload.canonical_server_name,
        payload.health_status,
        payload.health_score,
        payload.critical_issue_count,
        payload.high_issue_count,
        payload.priority_issue_count,
        payload.priority_briefing,
        payload.prepared_ts

    FROM {table_name(ALERT_PAYLOAD_TABLE)} AS payload

    INNER JOIN latest_run
        ON payload.run_id = latest_run.run_id

    WHERE
        latest_run.run_status = 'HEALTH_RULES_EVALUATED'
        AND payload.alert_eligible = true
        AND payload.priority_issue_count > 0
    """
)


alert_view_df = spark.table(
    f"{CATALOG}.{SCHEMA}.{ALERT_VIEW}"
)

ALERT_VIEW_ROW_COUNT = alert_view_df.count()


print("")
print("Databricks Alert consumption view ready")
print(
    f"View: "
    f"{CATALOG}.{SCHEMA}.{ALERT_VIEW}"
)
print(
    f"Rows currently eligible for notification: "
    f"{ALERT_VIEW_ROW_COUNT}"
)


display(
    alert_view_df.orderBy(
        F.col("critical_issue_count").desc(),
        F.col("high_issue_count").desc(),
        F.col("canonical_server_name"),
    )
)

# COMMAND ----------

# -----------------------------------------------------------------------------
# 8. Final validation and publish Workflow task values
# -----------------------------------------------------------------------------

# Safety contract:
# - test runs must expose zero production-alert rows
# - production runs with priority findings must expose exactly one row
#   per affected server

if IS_TEST_RUN and ALERT_VIEW_ROW_COUNT != 0:
    raise RuntimeError(
        "Safety violation: a TEST run exposed rows to the "
        "production Databricks Alert view."
    )


if (
    ALERT_ELIGIBLE
    and SERVER_PAYLOAD_COUNT > 0
    and ALERT_VIEW_ROW_COUNT != SERVER_PAYLOAD_COUNT
):
    raise RuntimeError(
        "Production alert view row count does not match the "
        "prepared server payload count: "
        f"{ALERT_VIEW_ROW_COUNT} != {SERVER_PAYLOAD_COUNT}"
    )


if IS_TEST_RUN:
    ALERT_STATUS = "SUPPRESSED_TEST_RUN"

elif SERVER_PAYLOAD_COUNT == 0:
    ALERT_STATUS = "NO_PRIORITY_FINDINGS"

elif ALERT_VIEW_ROW_COUNT > 0:
    ALERT_STATUS = "READY_FOR_NOTIFICATION"

else:
    ALERT_STATUS = "NOT_READY"


result = {
    "run_id": RUN_ID,
    "run_status": RUN_STATUS,
    "alert_status": ALERT_STATUS,
    "alert_eligible": ALERT_ELIGIBLE,
    "priority_issue_count": PRIORITY_ISSUE_COUNT,
    "affected_server_count": AFFECTED_SERVER_COUNT,
    "payload_row_count": SERVER_PAYLOAD_COUNT,
    "alert_view_row_count": ALERT_VIEW_ROW_COUNT,
    "suppression_reason": SUPPRESSION_REASON or "",
}


# These values become available to downstream Databricks Workflow tasks.
try:
    for key, value in result.items():
        dbutils.jobs.taskValues.set(
            key=key,
            value=value,
        )
except Exception:
    # Expected when running the notebook interactively
    # rather than as a Workflow task.
    pass


print("")
print("Databricks native alert preparation complete")

for key, value in result.items():
    print(f"{key}: {value}")