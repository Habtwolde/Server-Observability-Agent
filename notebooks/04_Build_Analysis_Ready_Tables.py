# Databricks notebook source
# 04_Build_Analysis_Ready_Tables

from __future__ import annotations

from datetime import datetime, timezone

from pyspark.sql import functions as F
from pyspark.sql.types import MapType, StringType


# -----------------------------------------------------------------------------
# 1. Runtime parameters and run gate
# -----------------------------------------------------------------------------

dbutils.widgets.text("run_id", "", "Run ID (blank = latest run)")
dbutils.widgets.dropdown(
    "allow_incomplete_run",
    "false",
    ["false", "true"],
    "Allow incomplete run for development testing",
)

CATALOG = "ent_log_analytics"
SCHEMA = "observability"


def table_name(name: str) -> str:
    return f"`{CATALOG}`.`{SCHEMA}`.`{name}`"


CONFIG = {
    row["config_key"]: row["config_value"]
    for row in spark.table(f"{CATALOG}.{SCHEMA}.agent_config")
    .select("config_key", "config_value")
    .collect()
}

SOURCE_TIMEZONE = CONFIG.get("source_timezone", "America/New_York")
ALLOW_INCOMPLETE_RUN = (
    dbutils.widgets.get("allow_incomplete_run").strip().lower() == "true"
)
RUN_ID_PARAMETER = dbutils.widgets.get("run_id").strip()

spark.conf.set("spark.sql.session.timeZone", SOURCE_TIMEZONE)

if RUN_ID_PARAMETER:
    RUN_ID = RUN_ID_PARAMETER
else:
    latest_runs = (
        spark.table(f"{CATALOG}.{SCHEMA}.agent_ingestion_runs")
        .orderBy(F.col("run_date").desc(), F.col("updated_ts").desc())
        .select("run_id")
        .limit(1)
        .collect()
    )
    if not latest_runs:
        raise RuntimeError("No Agent ingestion run exists. Run notebooks 01-03 first.")
    RUN_ID = latest_runs[0]["run_id"]

run_rows = (
    spark.table(f"{CATALOG}.{SCHEMA}.agent_ingestion_runs")
    .where(F.col("run_id") == RUN_ID)
    .orderBy(F.col("updated_ts").desc())
    .limit(1)
    .collect()
)

if not run_rows:
    raise RuntimeError(f"Run ID was not found: {RUN_ID}")

RUN_DATE = run_rows[0]["run_date"]
PRIOR_RUN_STATUS = run_rows[0]["run_status"]

PRODUCTION_INPUT_STATUSES = {
    "INPUTS_INGESTED",
    "ANALYSIS_TABLES_READY",
}
TEST_INPUT_STATUSES = {
    "TEST_INPUTS_INGESTED",
    "TEST_ANALYSIS_TABLES_READY",
}

if PRIOR_RUN_STATUS not in PRODUCTION_INPUT_STATUSES and not (
    ALLOW_INCOMPLETE_RUN and PRIOR_RUN_STATUS in TEST_INPUT_STATUSES
):
    raise RuntimeError(
        f"Run {RUN_ID} has status {PRIOR_RUN_STATUS}. "
        "Production Silver processing requires INPUTS_INGESTED. "
        "Use allow_incomplete_run=true only for the controlled sample test."
    )

print(f"Run ID: {RUN_ID}")
print(f"Run date: {RUN_DATE}")
print(f"Prior run status: {PRIOR_RUN_STATUS}")
print(f"Source timezone: {SOURCE_TIMEZONE}")
print(f"Allow incomplete run: {ALLOW_INCOMPLETE_RUN}")


# -----------------------------------------------------------------------------
# 2. Create separate Agent Silver tables
# -----------------------------------------------------------------------------

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {table_name('agent_sql_rows_silver')} (
        run_id STRING NOT NULL,
        source_file_id STRING NOT NULL,
        canonical_server_name STRING NOT NULL,
        snapshot_date DATE NOT NULL,
        sheet_ordinal INT NOT NULL,
        sheet_name STRING NOT NULL,
        source_row_number BIGINT NOT NULL,
        entity_type STRING,
        entity_name STRING,
        observed_ts TIMESTAMP,
        row_values MAP<STRING, STRING> NOT NULL,
        raw_row_json STRING NOT NULL,
        data_quality_status STRING NOT NULL,
        source_file_path STRING NOT NULL,
        silvered_ts TIMESTAMP NOT NULL
    )
    USING DELTA
    TBLPROPERTIES (
        'delta.enableChangeDataFeed' = 'true',
        'quality' = 'silver',
        'agent.owner' = 'sql-server-observability-agent'
    )
    """
)

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {table_name('agent_sql_metric_facts')} (
        metric_id STRING NOT NULL,
        run_id STRING NOT NULL,
        source_file_id STRING NOT NULL,
        canonical_server_name STRING NOT NULL,
        snapshot_date DATE NOT NULL,
        sheet_ordinal INT NOT NULL,
        sheet_name STRING NOT NULL,
        source_row_number BIGINT NOT NULL,
        entity_type STRING,
        entity_name STRING,
        observed_ts TIMESTAMP,
        metric_name STRING NOT NULL,
        normalized_metric_name STRING NOT NULL,
        metric_value_string STRING,
        metric_value_double DOUBLE,
        metric_value_boolean BOOLEAN,
        metric_value_timestamp TIMESTAMP,
        metric_unit STRING,
        raw_row_json STRING NOT NULL,
        silvered_ts TIMESTAMP NOT NULL
    )
    USING DELTA
    TBLPROPERTIES (
        'delta.enableChangeDataFeed' = 'true',
        'quality' = 'silver',
        'agent.owner' = 'sql-server-observability-agent'
    )
    """
)

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {table_name('agent_windows_events_silver')} (
        event_fingerprint STRING NOT NULL,
        run_id STRING,
        source_file_id STRING NOT NULL,
        canonical_server_name STRING NOT NULL,
        source_server_name STRING,
        event_id STRING,
        provider_name STRING,
        log_name STRING,
        container_log STRING,
        level_display_name STRING,
        severity_rank INT NOT NULL,
        severity_class STRING NOT NULL,
        event_time TIMESTAMP NOT NULL,
        event_date DATE NOT NULL,
        message STRING,
        normalized_message STRING,
        occurrence_count BIGINT NOT NULL,
        security_signal BOOLEAN NOT NULL,
        availability_signal BOOLEAN NOT NULL,
        backup_signal BOOLEAN NOT NULL,
        resource_signal BOOLEAN NOT NULL,
        first_seen_ts TIMESTAMP NOT NULL,
        last_seen_ts TIMESTAMP NOT NULL,
        source_file_path STRING NOT NULL,
        ingestion_date DATE NOT NULL,
        silvered_ts TIMESTAMP NOT NULL
    )
    USING DELTA
    TBLPROPERTIES (
        'delta.enableChangeDataFeed' = 'true',
        'quality' = 'silver',
        'agent.owner' = 'sql-server-observability-agent'
    )
    """
)

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {table_name('agent_server_daily_inventory')} (
        run_id STRING NOT NULL,
        snapshot_date DATE NOT NULL,
        canonical_server_name STRING NOT NULL,
        has_sql_workbook BOOLEAN NOT NULL,
        has_windows_events BOOLEAN NOT NULL,
        populated_sheet_count INT NOT NULL,
        no_data_sheet_count INT NOT NULL,
        sql_row_count BIGINT NOT NULL,
        sql_metric_count BIGINT NOT NULL,
        windows_unique_event_count BIGINT NOT NULL,
        windows_source_occurrence_count BIGINT NOT NULL,
        windows_error_count BIGINT NOT NULL,
        windows_warning_count BIGINT NOT NULL,
        earliest_observed_ts TIMESTAMP,
        latest_observed_ts TIMESTAMP,
        inventory_status STRING NOT NULL,
        silvered_ts TIMESTAMP NOT NULL
    )
    USING DELTA
    TBLPROPERTIES (
        'delta.enableChangeDataFeed' = 'true',
        'quality' = 'silver',
        'agent.owner' = 'sql-server-observability-agent'
    )
    """
)


# -----------------------------------------------------------------------------
# 3. Normalize every populated row from all 53 diagnostic worksheets
# -----------------------------------------------------------------------------

bronze_df = (
    spark.table(f"{CATALOG}.{SCHEMA}.agent_sql_diagnostics_bronze")
    .where(F.col("run_id") == RUN_ID)
)

if bronze_df.limit(1).count() == 0:
    raise RuntimeError(f"No SQL diagnostic Bronze rows exist for {RUN_ID}.")

row_map_type = MapType(StringType(), StringType(), True)
silvered_ts = datetime.now(timezone.utc)

rows_df = bronze_df.withColumn(
    "row_values",
    F.from_json(F.col("row_json"), row_map_type),
)

generic_entity_columns = [
    "Database Name",
    "Database",
    "Job Name",
    "Drive",
    "volume_mount_point",
    "fixed_drive_path",
    "WaitType",
    "servicename",
    "Function Name",
    "Memory Clerk Type",
    "client_net_address",
    "name",
]

entity_columns_by_sheet = {
    "4-Configuration Values": ["name"],
    "7-SQL Server Services Info": ["servicename"],
    "8-Last Backup By Database": ["Database"],
    "10-SQL Server Agent Jobs": ["Job Name"],
    "11-SQL Server Agent Alerts": ["name"],
    "26-Database Filenames and Path": ["Database Name", "physical_name"],
    "27-Fixed Drives": ["fixed_drive_path"],
    "28-Volume Info": ["volume_mount_point"],
    "29-Drive Level Latency": ["Drive", "Volume Mount Point"],
    "30-IO Latency by File": ["Database Name", "physical_name"],
    "32-RG Resource Pools": ["name"],
    "33-Database Properties": ["Database Name"],
    "34-Missing Indexes All Databas": ["Database.Schema.Table"],
    "35-VLF Counts": ["Database Name"],
    "36-CPU Usage by Database": ["Database Name"],
    "37-IO Usage By Database": ["Database Name"],
    "38-Total Buffer Usage by Datab": ["Database Name"],
    "39-Version Store Space Usage": ["Database Name"],
    "40-Top Waits": ["WaitType"],
    "41-Connection Counts by IP Add": ["client_net_address", "program_name"],
    "43-Detect Blocking": ["database", "waiter sid"],
    "46-Top Worker Time Queries": ["Database Name", "Short Query Text"],
    "47-PLE by NUMA Node": ["instance_name"],
    "49-Memory Clerk Usage": ["Memory Clerk Type"],
    "50-Ad hoc Queries": ["Database Name", "Short Query Text"],
    "51-Top Logical Reads Queries": ["Database Name", "Short Query Text"],
    "52-Top Avg Elapsed Time Querie": ["Database Name", "Short Query Text"],
    "53-UDF Stats by DB": ["Database Name", "Function Name"],
}

entity_name_expr = F.coalesce(
    *[F.element_at(F.col("row_values"), F.lit(key)) for key in generic_entity_columns]
)

for sheet_name, candidate_columns in entity_columns_by_sheet.items():
    sheet_entity = F.coalesce(
        *[
            F.element_at(F.col("row_values"), F.lit(key))
            for key in candidate_columns
        ]
    )
    entity_name_expr = F.when(
        F.col("sheet_name") == sheet_name,
        sheet_entity,
    ).otherwise(entity_name_expr)

entity_type_expr = (
    F.when(F.col("sheet_name").isin("8-Last Backup By Database", "33-Database Properties", "35-VLF Counts"), F.lit("DATABASE"))
    .when(F.col("sheet_name") == "10-SQL Server Agent Jobs", F.lit("SQL_AGENT_JOB"))
    .when(F.col("sheet_name") == "11-SQL Server Agent Alerts", F.lit("SQL_AGENT_ALERT"))
    .when(F.col("sheet_name") == "4-Configuration Values", F.lit("CONFIGURATION"))
    .when(F.col("sheet_name").isin("27-Fixed Drives", "28-Volume Info", "29-Drive Level Latency"), F.lit("STORAGE_VOLUME"))
    .when(F.col("sheet_name") == "30-IO Latency by File", F.lit("DATABASE_FILE"))
    .when(F.col("sheet_name") == "40-Top Waits", F.lit("WAIT_TYPE"))
    .when(F.col("sheet_name") == "41-Connection Counts by IP Add", F.lit("CONNECTION_SOURCE"))
    .when(F.col("sheet_name") == "43-Detect Blocking", F.lit("BLOCKING_SESSION"))
    .when(F.col("sheet_name") == "49-Memory Clerk Usage", F.lit("MEMORY_CLERK"))
    .when(F.col("sheet_name") == "53-UDF Stats by DB", F.lit("DATABASE_FUNCTION"))
    .when(F.col("sheet_name").rlike("Database|Datab"), F.lit("DATABASE_METRIC"))
    .otherwise(F.lit("SERVER_METRIC"))
)

observed_candidates = [
    "Event Time",
    "System Time",
    "Last Start Date",
    "last_execution_time",
    "Creation Time",
    "last_user_seek",
    "last_startup_time",
    "SQL Server Start Time",
    "statistics_start_time",
    "LogDate",
]

rows_df = (
    rows_df
    .withColumn(
        "_observed_text",
        F.coalesce(
            *[
                F.element_at(F.col("row_values"), F.lit(key))
                for key in observed_candidates
            ]
        ),
    )
    .withColumn(
        "observed_ts",
        F.coalesce(
            F.expr("try_to_timestamp(_observed_text, 'M/d/yyyy h:mm:ss a')"),
            F.expr("try_to_timestamp(_observed_text, 'M/d/yyyy H:mm:ss')"),
            F.expr("try_to_timestamp(_observed_text, 'yyyy-MM-dd HH:mm:ss')"),
            F.expr("try_to_timestamp(_observed_text)"),
        ),
    )
    .withColumn("entity_type", entity_type_expr)
    .withColumn("entity_name", F.substring(F.trim(entity_name_expr), 1, 1000))
    .withColumn(
        "data_quality_status",
        F.when(F.col("row_values").isNull(), F.lit("INVALID_JSON"))
        .when(F.size(F.col("row_values")) == 0, F.lit("EMPTY_MAP"))
        .otherwise(F.lit("VALID")),
    )
    .select(
        "run_id",
        "source_file_id",
        "canonical_server_name",
        "snapshot_date",
        "sheet_ordinal",
        "sheet_name",
        "source_row_number",
        "entity_type",
        "entity_name",
        "observed_ts",
        "row_values",
        F.col("row_json").alias("raw_row_json"),
        "data_quality_status",
        "source_file_path",
        F.lit(silvered_ts).cast("timestamp").alias("silvered_ts"),
    )
)

invalid_json_count = rows_df.where(F.col("data_quality_status") != "VALID").count()
if invalid_json_count:
    raise RuntimeError(
        f"{invalid_json_count} SQL diagnostic rows could not be normalized from JSON."
    )

rows_df.createOrReplaceTempView("_agent_sql_rows_silver_source")

spark.sql(
    f"""
    MERGE INTO {table_name('agent_sql_rows_silver')} AS target
    USING _agent_sql_rows_silver_source AS source
       ON target.source_file_id = source.source_file_id
      AND target.sheet_ordinal = source.sheet_ordinal
      AND target.source_row_number = source.source_row_number
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    WHEN NOT MATCHED BY SOURCE AND target.run_id = '{RUN_ID}' THEN DELETE
    """
)

spark.catalog.dropTempView("_agent_sql_rows_silver_source")


# -----------------------------------------------------------------------------
# 4. Convert every worksheet value into a typed metric fact
# -----------------------------------------------------------------------------

metric_df = (
    rows_df
    .select("*", F.explode(F.map_entries(F.col("row_values"))).alias("_metric"))
    .select(
        "run_id",
        "source_file_id",
        "canonical_server_name",
        "snapshot_date",
        "sheet_ordinal",
        "sheet_name",
        "source_row_number",
        "entity_type",
        "entity_name",
        "observed_ts",
        F.col("_metric.key").alias("metric_name"),
        F.trim(F.col("_metric.value")).alias("metric_value_string"),
        "raw_row_json",
        "silvered_ts",
    )
    .where(
        F.col("metric_value_string").isNotNull()
        & (F.col("metric_value_string") != "")
    )
    .withColumn(
        "normalized_metric_name",
        F.regexp_replace(
            F.lower(F.trim(F.col("metric_name"))),
            r"[^a-z0-9]+",
            "_",
        ),
    )
    .withColumn(
        "metric_value_double",
        F.expr(
            "try_cast(regexp_replace(metric_value_string, '[,%]', '') AS DOUBLE)"
        ),
    )
    .withColumn(
        "metric_value_boolean",
        F.when(
            F.lower(F.col("metric_value_string")).isin("true", "yes", "y", "on"),
            F.lit(True),
        )
        .when(
            F.lower(F.col("metric_value_string")).isin("false", "no", "n", "off"),
            F.lit(False),
        )
        .otherwise(F.lit(None).cast("boolean")),
    )
    .withColumn(
        "metric_value_timestamp",
        F.coalesce(
            F.expr("try_to_timestamp(metric_value_string, 'M/d/yyyy h:mm:ss a')"),
            F.expr("try_to_timestamp(metric_value_string, 'M/d/yyyy H:mm:ss')"),
            F.expr("try_to_timestamp(metric_value_string, 'yyyy-MM-dd HH:mm:ss')"),
            F.when(
                F.col("normalized_metric_name").rlike("date|time"),
                F.expr("try_to_timestamp(metric_value_string)"),
            ),
        ),
    )
    .withColumn(
        "metric_unit",
        F.when(F.lower(F.col("metric_name")).rlike(r"\(mb\)|\bmb\b"), F.lit("MB"))
        .when(F.lower(F.col("metric_name")).rlike(r"\(gb\)|\bgb\b"), F.lit("GB"))
        .when(F.lower(F.col("metric_name")).rlike(r"percent|%"), F.lit("PERCENT"))
        .when(F.lower(F.col("metric_name")).rlike(r"latency.*ms|_ms\b|\(ms\)"), F.lit("MILLISECONDS"))
        .when(F.lower(F.col("metric_name")).rlike(r"_sec\b|seconds|\(sec\)"), F.lit("SECONDS"))
        .when(F.lower(F.col("metric_name")).rlike(r"count"), F.lit("COUNT"))
        .otherwise(F.lit(None).cast("string")),
    )
    .withColumn(
        "metric_id",
        F.sha2(
            F.concat_ws(
                "||",
                F.col("source_file_id"),
                F.col("sheet_ordinal").cast("string"),
                F.col("source_row_number").cast("string"),
                F.col("metric_name"),
            ),
            256,
        ),
    )
    .select(
        "metric_id",
        "run_id",
        "source_file_id",
        "canonical_server_name",
        "snapshot_date",
        "sheet_ordinal",
        "sheet_name",
        "source_row_number",
        "entity_type",
        "entity_name",
        "observed_ts",
        "metric_name",
        "normalized_metric_name",
        "metric_value_string",
        "metric_value_double",
        "metric_value_boolean",
        "metric_value_timestamp",
        "metric_unit",
        "raw_row_json",
        "silvered_ts",
    )
)

metric_df.createOrReplaceTempView("_agent_sql_metric_facts_source")

spark.sql(
    f"""
    MERGE INTO {table_name('agent_sql_metric_facts')} AS target
    USING _agent_sql_metric_facts_source AS source
       ON target.metric_id = source.metric_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    WHEN NOT MATCHED BY SOURCE AND target.run_id = '{RUN_ID}' THEN DELETE
    """
)

spark.catalog.dropTempView("_agent_sql_metric_facts_source")


# -----------------------------------------------------------------------------
# 5. Normalize Windows Events and add non-authoritative diagnostic signals
# -----------------------------------------------------------------------------

windows_bronze_df = spark.table(
    f"{CATALOG}.{SCHEMA}.agent_windows_events_bronze"
)

normalized_message = F.lower(
    F.regexp_replace(F.trim(F.coalesce(F.col("message"), F.lit(""))), r"\s+", " ")
)
level_name = F.lower(F.trim(F.coalesce(F.col("level_display_name"), F.lit(""))))
searchable_event = F.concat_ws(
    " ",
    F.lower(F.coalesce(F.col("provider_name"), F.lit(""))),
    F.lower(F.coalesce(F.col("log_name"), F.lit(""))),
    normalized_message,
)

windows_silver_df = (
    windows_bronze_df
    .withColumn("normalized_message", normalized_message)
    .withColumn(
        "severity_rank",
        F.when(level_name == "critical", F.lit(4))
        .when(level_name == "error", F.lit(3))
        .when(level_name == "warning", F.lit(2))
        .when(level_name.isin("information", "informational"), F.lit(1))
        .otherwise(F.lit(0)),
    )
    .withColumn(
        "severity_class",
        F.when(level_name == "critical", F.lit("CRITICAL"))
        .when(level_name == "error", F.lit("ERROR"))
        .when(level_name == "warning", F.lit("WARNING"))
        .when(level_name.isin("information", "informational"), F.lit("INFORMATION"))
        .otherwise(F.lit("OTHER")),
    )
    .withColumn(
        "security_signal",
        searchable_event.rlike(
            r"security|audit|logon|login failed|authentication|kerberos|"
            r"account lock|access denied|windows defender|malware"
        ),
    )
    .withColumn(
        "availability_signal",
        searchable_event.rlike(
            r"unexpected shutdown|service.*(stopped|terminated)|failover|"
            r"cluster.*(failed|offline)|sql server.*(stopped|terminated)"
        ),
    )
    .withColumn(
        "backup_signal",
        searchable_event.rlike(r"backup|restore|sqlvdi|vss|volsnap"),
    )
    .withColumn(
        "resource_signal",
        searchable_event.rlike(
            r"out of memory|low memory|disk|storage|i/o|io error|cpu|paging|pagefile"
        ),
    )
    .select(
        "event_fingerprint",
        "run_id",
        "source_file_id",
        "canonical_server_name",
        "source_server_name",
        "event_id",
        "provider_name",
        "log_name",
        "container_log",
        "level_display_name",
        "severity_rank",
        "severity_class",
        "event_time",
        "event_date",
        "message",
        "normalized_message",
        "occurrence_count",
        "security_signal",
        "availability_signal",
        "backup_signal",
        "resource_signal",
        "first_seen_ts",
        "last_seen_ts",
        "source_file_path",
        "ingestion_date",
        F.lit(silvered_ts).cast("timestamp").alias("silvered_ts"),
    )
)

windows_silver_df.createOrReplaceTempView("_agent_windows_events_silver_source")

spark.sql(
    f"""
    MERGE INTO {table_name('agent_windows_events_silver')} AS target
    USING _agent_windows_events_silver_source AS source
       ON target.event_fingerprint = source.event_fingerprint
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """
)

spark.catalog.dropTempView("_agent_windows_events_silver_source")


# -----------------------------------------------------------------------------
# 6. Semantic views used by the rules engine and Streamlit Agent
# -----------------------------------------------------------------------------

spark.sql(
    f"""
    CREATE OR REPLACE VIEW {table_name('v_agent_latest_sql_rows_silver')} AS
    SELECT row_data.*
    FROM {table_name('agent_sql_rows_silver')} AS row_data
    INNER JOIN (
        SELECT canonical_server_name, MAX(snapshot_date) AS latest_snapshot_date
        FROM {table_name('agent_sql_rows_silver')}
        GROUP BY canonical_server_name
    ) AS latest
       ON row_data.canonical_server_name = latest.canonical_server_name
      AND row_data.snapshot_date = latest.latest_snapshot_date
    """
)

spark.sql(
    f"""
    CREATE OR REPLACE VIEW {table_name('v_agent_latest_sql_metric_facts')} AS
    SELECT metric.*
    FROM {table_name('agent_sql_metric_facts')} AS metric
    INNER JOIN (
        SELECT canonical_server_name, MAX(snapshot_date) AS latest_snapshot_date
        FROM {table_name('agent_sql_metric_facts')}
        GROUP BY canonical_server_name
    ) AS latest
       ON metric.canonical_server_name = latest.canonical_server_name
      AND metric.snapshot_date = latest.latest_snapshot_date
    """
)

spark.sql(
    f"""
    CREATE OR REPLACE VIEW {table_name('v_agent_backup_health')} AS
    SELECT
        run_id,
        source_file_id,
        canonical_server_name,
        snapshot_date,
        entity_name AS database_name,
        row_values['Recovery Model'] AS recovery_model,
        row_values['Log Reuse Wait Desc'] AS log_reuse_wait_desc,
        try_cast(row_values['Total Data File Size on Disk (MB)'] AS DOUBLE) AS data_size_mb,
        try_cast(row_values['Total Log File Size on Disk (MB)'] AS DOUBLE) AS log_size_mb,
        try_cast(row_values['Log Used %'] AS DOUBLE) AS log_used_percent,
        coalesce(
            try_to_timestamp(row_values['Last Full Backup'], 'M/d/yyyy h:mm:ss a'),
            try_to_timestamp(row_values['Last Full Backup'])
        ) AS last_full_backup_ts,
        coalesce(
            try_to_timestamp(row_values['Last Differential Backup'], 'M/d/yyyy h:mm:ss a'),
            try_to_timestamp(row_values['Last Differential Backup'])
        ) AS last_differential_backup_ts,
        coalesce(
            try_to_timestamp(row_values['Last Log Backup'], 'M/d/yyyy h:mm:ss a'),
            try_to_timestamp(row_values['Last Log Backup'])
        ) AS last_log_backup_ts,
        coalesce(
            try_to_timestamp(row_values['Last Good CheckDB'], 'M/d/yyyy h:mm:ss a'),
            try_to_timestamp(row_values['Last Good CheckDB'])
        ) AS last_good_checkdb_ts,
        try_cast(row_values['Last Full Compressed Backup Size (MB)'] AS DOUBLE) AS compressed_backup_size_mb,
        try_cast(row_values['Backup Compression Ratio'] AS DOUBLE) AS compression_ratio,
        row_values['Last Full Backup Compression Algorithm'] AS compression_algorithm,
        raw_row_json
    FROM {table_name('v_agent_latest_sql_rows_silver')}
    WHERE sheet_name = '8-Last Backup By Database'
    """
)

spark.sql(
    f"""
    CREATE OR REPLACE VIEW {table_name('v_agent_configuration_health')} AS
    SELECT
        run_id,
        source_file_id,
        canonical_server_name,
        snapshot_date,
        row_values['name'] AS configuration_name,
        try_cast(row_values['value'] AS DOUBLE) AS configured_value,
        try_cast(row_values['value_in_use'] AS DOUBLE) AS value_in_use,
        try_cast(row_values['minimum'] AS DOUBLE) AS minimum_value,
        try_cast(row_values['maximum'] AS DOUBLE) AS maximum_value,
        row_values['description'] AS description,
        try_cast(row_values['is_dynamic'] AS BOOLEAN) AS is_dynamic,
        try_cast(row_values['is_advanced'] AS BOOLEAN) AS is_advanced
    FROM {table_name('v_agent_latest_sql_rows_silver')}
    WHERE sheet_name = '4-Configuration Values'
    """
)

spark.sql(
    f"""
    CREATE OR REPLACE VIEW {table_name('v_agent_job_health')} AS
    SELECT
        run_id,
        source_file_id,
        canonical_server_name,
        snapshot_date,
        row_values['Job Name'] AS job_name,
        row_values['Job Description'] AS job_description,
        row_values['CategoryName'] AS category_name,
        row_values['Job Owner'] AS job_owner,
        try_cast(row_values['Job Enabled'] AS INT) AS job_enabled,
        try_cast(row_values['run_status'] AS INT) AS run_status,
        CASE try_cast(row_values['run_status'] AS INT)
            WHEN 0 THEN 'FAILED'
            WHEN 1 THEN 'SUCCEEDED'
            WHEN 2 THEN 'RETRY'
            WHEN 3 THEN 'CANCELLED'
            WHEN 4 THEN 'IN_PROGRESS'
            ELSE 'UNKNOWN'
        END AS run_status_desc,
        coalesce(
            try_to_timestamp(row_values['Last Start Date'], 'M/d/yyyy h:mm:ss a'),
            try_to_timestamp(row_values['Last Start Date'])
        ) AS last_start_ts,
        row_values['Last Duration - HHMMSS'] AS last_duration_hhmmss,
        row_values['Schedule Name'] AS schedule_name,
        try_cast(row_values['Schedule Enabled'] AS INT) AS schedule_enabled,
        row_values['next_run_date'] AS next_run_date_raw,
        row_values['next_run_time'] AS next_run_time_raw,
        raw_row_json
    FROM {table_name('v_agent_latest_sql_rows_silver')}
    WHERE sheet_name = '10-SQL Server Agent Jobs'
    """
)

spark.sql(
    f"""
    CREATE OR REPLACE VIEW {table_name('v_agent_database_health')} AS
    SELECT
        run_id,
        source_file_id,
        canonical_server_name,
        snapshot_date,
        row_values['Database Name'] AS database_name,
        row_values['Database Owner'] AS database_owner,
        try_cast(row_values['DB Compatibility Level'] AS INT) AS compatibility_level,
        row_values['Recovery Model'] AS recovery_model,
        row_values['Log Reuse Wait Description'] AS log_reuse_wait_desc,
        try_cast(row_values['Log Used %'] AS DOUBLE) AS log_used_percent,
        row_values['Page Verify Option'] AS page_verify_option,
        row_values['user_access_desc'] AS user_access_desc,
        row_values['state_desc'] AS state_desc,
        try_cast(row_values['is_auto_close_on'] AS BOOLEAN) AS is_auto_close_on,
        try_cast(row_values['is_auto_shrink_on'] AS BOOLEAN) AS is_auto_shrink_on,
        try_cast(row_values['is_auto_create_stats_on'] AS BOOLEAN) AS is_auto_create_stats_on,
        try_cast(row_values['is_auto_update_stats_on'] AS BOOLEAN) AS is_auto_update_stats_on,
        try_cast(row_values['is_query_store_on'] AS BOOLEAN) AS is_query_store_on,
        try_cast(row_values['is_encrypted'] AS BOOLEAN) AS is_encrypted,
        row_values['encryption_state'] AS encryption_state,
        raw_row_json
    FROM {table_name('v_agent_latest_sql_rows_silver')}
    WHERE sheet_name = '33-Database Properties'
    """
)

spark.sql(
    f"""
    CREATE OR REPLACE VIEW {table_name('v_agent_storage_health')} AS
    SELECT
        run_id,
        source_file_id,
        canonical_server_name,
        snapshot_date,
        sheet_name,
        coalesce(
            row_values['volume_mount_point'],
            row_values['fixed_drive_path']
        ) AS volume_path,
        row_values['file_system_type'] AS file_system_type,
        row_values['logical_volume_name'] AS logical_volume_name,
        try_cast(row_values['Total Size (GB)'] AS DOUBLE) AS total_size_gb,
        coalesce(
            try_cast(row_values['Available Size (GB)'] AS DOUBLE),
            try_cast(row_values['Available Space (GB)'] AS DOUBLE)
        ) AS available_size_gb,
        try_cast(row_values['Space Free %'] AS DOUBLE) AS space_free_percent,
        try_cast(row_values['supports_compression'] AS BOOLEAN) AS supports_compression,
        try_cast(row_values['is_compressed'] AS BOOLEAN) AS is_compressed,
        raw_row_json
    FROM {table_name('v_agent_latest_sql_rows_silver')}
    WHERE sheet_name IN ('27-Fixed Drives', '28-Volume Info')
    """
)

spark.sql(
    f"""
    CREATE OR REPLACE VIEW {table_name('v_agent_io_health')} AS
    SELECT
        run_id,
        source_file_id,
        canonical_server_name,
        snapshot_date,
        sheet_name,
        CASE
            WHEN sheet_name = '29-Drive Level Latency' THEN 'DRIVE'
            ELSE 'DATABASE_FILE'
        END AS io_scope,
        coalesce(row_values['Drive'], row_values['Database Name']) AS entity_name,
        coalesce(row_values['Volume Mount Point'], row_values['physical_name']) AS physical_path,
        coalesce(
            try_cast(row_values['Read Latency'] AS DOUBLE),
            try_cast(row_values['avg_read_latency_ms'] AS DOUBLE)
        ) AS read_latency_ms,
        coalesce(
            try_cast(row_values['Write Latency'] AS DOUBLE),
            try_cast(row_values['avg_write_latency_ms'] AS DOUBLE)
        ) AS write_latency_ms,
        coalesce(
            try_cast(row_values['Overall Latency'] AS DOUBLE),
            try_cast(row_values['avg_io_latency_ms'] AS DOUBLE)
        ) AS overall_latency_ms,
        try_cast(row_values['File Size (MB)'] AS DOUBLE) AS file_size_mb,
        row_values['type_desc'] AS file_type_desc,
        raw_row_json
    FROM {table_name('v_agent_latest_sql_rows_silver')}
    WHERE sheet_name IN ('29-Drive Level Latency', '30-IO Latency by File')
    """
)

spark.sql(
    f"""
    CREATE OR REPLACE VIEW {table_name('v_agent_cpu_health')} AS
    SELECT
        run_id,
        source_file_id,
        canonical_server_name,
        snapshot_date,
        sheet_name,
        row_values['Database Name'] AS database_name,
        try_cast(row_values['CPU Percent'] AS DOUBLE) AS database_cpu_percent,
        try_cast(row_values['SQL Server Process CPU Utilization'] AS DOUBLE) AS sql_cpu_percent,
        try_cast(row_values['System Idle Process'] AS DOUBLE) AS system_idle_percent,
        try_cast(row_values['Other Process CPU Utilization'] AS DOUBLE) AS other_process_cpu_percent,
        coalesce(
            try_to_timestamp(row_values['Event Time'], 'M/d/yyyy h:mm:ss a'),
            try_to_timestamp(row_values['Event Time'])
        ) AS event_time,
        raw_row_json
    FROM {table_name('v_agent_latest_sql_rows_silver')}
    WHERE sheet_name IN ('36-CPU Usage by Database', '45-CPU Utilization History')
    """
)

spark.sql(
    f"""
    CREATE OR REPLACE VIEW {table_name('v_agent_memory_health')} AS
    SELECT
        run_id,
        source_file_id,
        canonical_server_name,
        snapshot_date,
        sheet_name,
        entity_name,
        try_cast(row_values['SQL Server Memory Usage (MB)'] AS DOUBLE) AS sql_memory_usage_mb,
        try_cast(row_values['memory_utilization_percentage'] AS DOUBLE) AS sql_memory_utilization_percent,
        try_cast(row_values['Physical Memory (MB)'] AS DOUBLE) AS physical_memory_mb,
        try_cast(row_values['Available Memory (MB)'] AS DOUBLE) AS available_memory_mb,
        row_values['System Memory State'] AS system_memory_state,
        try_cast(row_values['Page Life Expectancy'] AS DOUBLE) AS page_life_expectancy,
        try_cast(row_values['Memory Grants Pending'] AS DOUBLE) AS memory_grants_pending,
        try_cast(row_values['Memory Usage (MB)'] AS DOUBLE) AS memory_clerk_usage_mb,
        row_values['Memory Clerk Type'] AS memory_clerk_type,
        raw_row_json
    FROM {table_name('v_agent_latest_sql_rows_silver')}
    WHERE sheet_name IN (
        '6-Process Memory',
        '14-System Memory',
        '47-PLE by NUMA Node',
        '48-Memory Grants Pending',
        '49-Memory Clerk Usage'
    )
    """
)

spark.sql(
    f"""
    CREATE OR REPLACE VIEW {table_name('v_agent_wait_health')} AS
    SELECT
        run_id,
        source_file_id,
        canonical_server_name,
        snapshot_date,
        row_values['WaitType'] AS wait_type,
        try_cast(row_values['Wait Percentage'] AS DOUBLE) AS wait_percentage,
        try_cast(row_values['AvgWait_Sec'] AS DOUBLE) AS average_wait_seconds,
        try_cast(row_values['AvgRes_Sec'] AS DOUBLE) AS average_resource_seconds,
        try_cast(row_values['AvgSig_Sec'] AS DOUBLE) AS average_signal_seconds,
        try_cast(row_values['Wait Count'] AS BIGINT) AS wait_count,
        row_values['Help/Info URL'] AS help_url,
        raw_row_json
    FROM {table_name('v_agent_latest_sql_rows_silver')}
    WHERE sheet_name = '40-Top Waits'
    """
)

spark.sql(
    f"""
    CREATE OR REPLACE VIEW {table_name('v_agent_blocking_health')} AS
    SELECT
        run_id,
        source_file_id,
        canonical_server_name,
        snapshot_date,
        row_values['database'] AS database_name,
        row_values['lock type'] AS lock_type,
        row_values['lock req'] AS lock_request,
        try_cast(row_values['waiter sid'] AS INT) AS waiter_session_id,
        try_cast(row_values['wait time'] AS BIGINT) AS wait_time_ms,
        try_cast(row_values['blocker sid'] AS INT) AS blocker_session_id,
        row_values['waiter_stmt'] AS waiter_statement,
        row_values['blocker_batch'] AS blocker_batch,
        raw_row_json
    FROM {table_name('v_agent_latest_sql_rows_silver')}
    WHERE sheet_name = '43-Detect Blocking'
    """
)

spark.sql(
    f"""
    CREATE OR REPLACE VIEW {table_name('v_agent_recent_windows_events_silver')} AS
    SELECT event_data.*
    FROM (
        SELECT
            event_data.*,
            MAX(event_date) OVER (
                PARTITION BY canonical_server_name
            ) AS server_latest_event_date
        FROM {table_name('agent_windows_events_silver')} AS event_data
    ) AS event_data
    WHERE event_date >= date_sub(server_latest_event_date, 7)
    """
)


# -----------------------------------------------------------------------------
# 7. Build one daily coverage and freshness row for every observed server
# -----------------------------------------------------------------------------

sql_summary_df = (
    rows_df
    .groupBy("canonical_server_name")
    .agg(
        F.count("*").cast("long").alias("sql_row_count"),
        F.countDistinct("sheet_name").cast("int").alias("populated_sheet_count"),
        F.min("observed_ts").alias("sql_earliest_observed_ts"),
        F.max("observed_ts").alias("sql_latest_observed_ts"),
    )
)

metric_summary_df = (
    metric_df
    .groupBy("canonical_server_name")
    .agg(F.count("*").cast("long").alias("sql_metric_count"))
)

sheet_summary_df = (
    spark.table(f"{CATALOG}.{SCHEMA}.agent_sheet_manifest")
    .where(F.col("run_id") == RUN_ID)
    .groupBy("canonical_server_name")
    .agg(
        F.sum(
            F.when(F.col("sheet_status") == "NO_DATA", F.lit(1)).otherwise(F.lit(0))
        ).cast("int").alias("no_data_sheet_count")
    )
)

windows_run_df = windows_silver_df.where(F.col("run_id") == RUN_ID)
windows_summary_df = (
    windows_run_df
    .groupBy("canonical_server_name")
    .agg(
        F.count("*").cast("long").alias("windows_unique_event_count"),
        F.sum("occurrence_count").cast("long").alias("windows_source_occurrence_count"),
        F.sum(F.when(F.col("severity_class") == "ERROR", 1).otherwise(0)).cast("long").alias("windows_error_count"),
        F.sum(F.when(F.col("severity_class") == "WARNING", 1).otherwise(0)).cast("long").alias("windows_warning_count"),
        F.min("event_time").alias("windows_earliest_observed_ts"),
        F.max("event_time").alias("windows_latest_observed_ts"),
    )
)

registry_servers_df = (
    spark.table(f"{CATALOG}.{SCHEMA}.agent_server_registry")
    .where(F.col("is_active") == True)
    .select("canonical_server_name")
)

observed_servers_df = (
    registry_servers_df
    .unionByName(sql_summary_df.select("canonical_server_name"))
    .unionByName(windows_summary_df.select("canonical_server_name"))
    .distinct()
)

inventory_df = (
    observed_servers_df
    .join(sql_summary_df, "canonical_server_name", "left")
    .join(metric_summary_df, "canonical_server_name", "left")
    .join(sheet_summary_df, "canonical_server_name", "left")
    .join(windows_summary_df, "canonical_server_name", "left")
    .fillna(
        {
            "sql_row_count": 0,
            "sql_metric_count": 0,
            "populated_sheet_count": 0,
            "no_data_sheet_count": 0,
            "windows_unique_event_count": 0,
            "windows_source_occurrence_count": 0,
            "windows_error_count": 0,
            "windows_warning_count": 0,
        }
    )
    .withColumn("has_sql_workbook", F.col("sql_row_count") > 0)
    .withColumn("has_windows_events", F.col("windows_unique_event_count") > 0)
    .withColumn(
        "earliest_observed_ts",
        F.least("sql_earliest_observed_ts", "windows_earliest_observed_ts"),
    )
    .withColumn(
        "latest_observed_ts",
        F.greatest("sql_latest_observed_ts", "windows_latest_observed_ts"),
    )
    .withColumn(
        "inventory_status",
        F.when(F.col("has_sql_workbook") & F.col("has_windows_events"), "COMPLETE")
        .when(F.col("has_sql_workbook"), "SQL_ONLY")
        .when(F.col("has_windows_events"), "WINDOWS_ONLY")
        .otherwise("MISSING"),
    )
    .select(
        F.lit(RUN_ID).alias("run_id"),
        F.lit(RUN_DATE).cast("date").alias("snapshot_date"),
        "canonical_server_name",
        "has_sql_workbook",
        "has_windows_events",
        "populated_sheet_count",
        "no_data_sheet_count",
        "sql_row_count",
        "sql_metric_count",
        "windows_unique_event_count",
        "windows_source_occurrence_count",
        "windows_error_count",
        "windows_warning_count",
        "earliest_observed_ts",
        "latest_observed_ts",
        "inventory_status",
        F.lit(silvered_ts).cast("timestamp").alias("silvered_ts"),
    )
)

inventory_df.createOrReplaceTempView("_agent_server_daily_inventory_source")

spark.sql(
    f"""
    MERGE INTO {table_name('agent_server_daily_inventory')} AS target
    USING _agent_server_daily_inventory_source AS source
       ON target.run_id = source.run_id
      AND target.canonical_server_name = source.canonical_server_name
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    WHEN NOT MATCHED BY SOURCE AND target.run_id = '{RUN_ID}' THEN DELETE
    """
)

spark.catalog.dropTempView("_agent_server_daily_inventory_source")

spark.sql(
    f"""
    CREATE OR REPLACE VIEW {table_name('v_agent_latest_server_daily_inventory')} AS
    SELECT inventory.*
    FROM {table_name('agent_server_daily_inventory')} AS inventory
    INNER JOIN (
        SELECT canonical_server_name, MAX(snapshot_date) AS latest_snapshot_date
        FROM {table_name('agent_server_daily_inventory')}
        GROUP BY canonical_server_name
    ) AS latest
       ON inventory.canonical_server_name = latest.canonical_server_name
      AND inventory.snapshot_date = latest.latest_snapshot_date
    """
)


# -----------------------------------------------------------------------------
# 8. Validate counts and update the orchestration status
# -----------------------------------------------------------------------------

sql_row_count = rows_df.count()
metric_fact_count = metric_df.count()
windows_silver_count = windows_silver_df.count()
inventory_count = inventory_df.count()

if sql_row_count <= 0 or metric_fact_count <= 0:
    raise RuntimeError(
        "Silver validation failed: SQL row and metric fact counts must be positive."
    )

if PRIOR_RUN_STATUS in PRODUCTION_INPUT_STATUSES:
    FINAL_RUN_STATUS = "ANALYSIS_TABLES_READY"
    FINAL_ERROR_MESSAGE = None
else:
    FINAL_RUN_STATUS = "TEST_ANALYSIS_TABLES_READY"
    FINAL_ERROR_MESSAGE = (
        "Analysis-ready tables were built under allow_incomplete_run=true "
        "for the controlled sample test."
    )

now = datetime.now(timezone.utc)
run_update_df = spark.createDataFrame(
    [(RUN_ID, FINAL_RUN_STATUS, now, FINAL_ERROR_MESSAGE, now)],
    "run_id STRING, run_status STRING, processing_completed_ts TIMESTAMP, "
    "error_message STRING, updated_ts TIMESTAMP",
)
run_update_df.createOrReplaceTempView("_agent_analysis_run_update")

spark.sql(
    f"""
    MERGE INTO {table_name('agent_ingestion_runs')} AS target
    USING _agent_analysis_run_update AS source
       ON target.run_id = source.run_id
    WHEN MATCHED THEN UPDATE SET
        target.run_status = source.run_status,
        target.processing_completed_ts = source.processing_completed_ts,
        target.error_message = source.error_message,
        target.updated_ts = source.updated_ts
    """
)

spark.catalog.dropTempView("_agent_analysis_run_update")

summary_rows = [
    ("run_id", RUN_ID),
    ("sql_silver_row_count", str(sql_row_count)),
    ("sql_metric_fact_count", str(metric_fact_count)),
    ("windows_silver_event_count", str(windows_silver_count)),
    ("server_inventory_count", str(inventory_count)),
    ("final_run_status", FINAL_RUN_STATUS),
]

display(spark.createDataFrame(summary_rows, ["check", "value"]))
display(inventory_df.orderBy("canonical_server_name"))

print(f"Final run status: {FINAL_RUN_STATUS}")

try:
    dbutils.jobs.taskValues.set(key="run_id", value=RUN_ID)
    dbutils.jobs.taskValues.set(
        key="analysis_tables_status",
        value=FINAL_RUN_STATUS,
    )
except Exception:
    pass
