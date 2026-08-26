# Databricks notebook source
# Databricks notebook source
# 00_Setup_Agent_Storage_and_Registry
#
# Purpose:
#   1. Validate access to the existing observability volume.
#   2. Create Agent-specific directories.
#   3. Create isolated Agent configuration, registry, ingestion,
#      manifest and Bronze Delta tables.
#
# Safe to rerun:
#   - Directories use mkdirs.
#   - Tables use CREATE TABLE IF NOT EXISTS.
#   - Configuration uses MERGE and does not overwrite existing values.

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
)

# -------------------------------------------------------------------
# 1. Environment configuration
# -------------------------------------------------------------------

CATALOG = "ent_log_analytics"
SCHEMA = "observability"
VOLUME = "server_observability_vol"

EXPECTED_SERVER_COUNT = 45
SOURCE_TIMEZONE = "America/New_York"

VOLUME_ROOT = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"

SQL_DIAGNOSTICS_INBOX = (
    f"{VOLUME_ROOT}/raw/sql_diagnostics/inbox"
)

SQL_DIAGNOSTICS_BY_SERVER = (
    f"{VOLUME_ROOT}/raw/sql_diagnostics/by_server"
)

WINDOWS_EVENTS_INBOX = (
    f"{VOLUME_ROOT}/raw/windows_events/inbox"
)

AGENT_ROOT = f"{VOLUME_ROOT}/agent"

AGENT_PATHS = {
    "quarantine_sql_diagnostics":
        f"{AGENT_ROOT}/quarantine/sql_diagnostics",

    "quarantine_windows_events":
        f"{AGENT_ROOT}/quarantine/windows_events",

    "run_manifests":
        f"{AGENT_ROOT}/run_manifests",

    "checkpoints_sql_diagnostics":
        f"{AGENT_ROOT}/checkpoints/sql_diagnostics",

    "checkpoints_windows_events":
        f"{AGENT_ROOT}/checkpoints/windows_events",

    "audit_logs":
        f"{AGENT_ROOT}/audit_logs",
}


def table_name(name: str) -> str:
    """Return a safely quoted three-level Unity Catalog table name."""
    return f"`{CATALOG}`.`{SCHEMA}`.`{name}`"


# -------------------------------------------------------------------
# 2. Validate catalog, schema and existing shared volume
# -------------------------------------------------------------------

spark.sql(f"USE CATALOG `{CATALOG}`")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`")
spark.sql(f"USE SCHEMA `{SCHEMA}`")

try:
    dbutils.fs.ls(VOLUME_ROOT)
except Exception as exc:
    raise RuntimeError(
        "The existing Unity Catalog volume is not accessible: "
        f"{VOLUME_ROOT}. Verify the volume name and permissions."
    ) from exc

print(f"Validated volume: {VOLUME_ROOT}")


# -------------------------------------------------------------------
# 3. Create required directories
# -------------------------------------------------------------------

required_paths = {
    "sql_diagnostics_inbox": SQL_DIAGNOSTICS_INBOX,
    "sql_diagnostics_by_server": SQL_DIAGNOSTICS_BY_SERVER,
    "windows_events_inbox": WINDOWS_EVENTS_INBOX,
    **AGENT_PATHS,
}

for path_name, path_value in required_paths.items():
    created_or_exists = dbutils.fs.mkdirs(path_value)

    if not created_or_exists:
        raise RuntimeError(
            f"Databricks could not create or access: {path_value}"
        )

    print(f"Directory ready [{path_name}]: {path_value}")


# -------------------------------------------------------------------
# 4. Agent configuration table
# -------------------------------------------------------------------

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {table_name("agent_config")} (
        config_key          STRING,
        config_value        STRING,
        description         STRING,
        created_ts          TIMESTAMP,
        updated_ts          TIMESTAMP
    )
    USING DELTA
    COMMENT 'Configuration values used by the SQL Server Observability Agent'
    TBLPROPERTIES (
        'delta.enableChangeDataFeed' = 'true'
    )
    """
)


# -------------------------------------------------------------------
# 5. Authoritative server registry
# -------------------------------------------------------------------

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {table_name("agent_server_registry")} (
        server_id                       STRING,
        canonical_server_name           STRING,
        workbook_server_aliases         ARRAY<STRING>,
        windows_event_server_aliases     ARRAY<STRING>,
        expected_daily_workbook          BOOLEAN,
        is_active                        BOOLEAN,
        first_seen_ts                    TIMESTAMP,
        last_seen_ts                     TIMESTAMP,
        created_ts                       TIMESTAMP,
        updated_ts                       TIMESTAMP,
        approved_by                      STRING,
        notes                            STRING
    )
    USING DELTA
    COMMENT 'Authoritative registry for the 45 monitored SQL Servers'
    TBLPROPERTIES (
        'delta.enableChangeDataFeed' = 'true'
    )
    """
)

# -------------------------------------------------------------------
# 5A. Native alert subscription routing
# -------------------------------------------------------------------

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {table_name("agent_alert_subscriptions")} (
        subscription_id             STRING NOT NULL,
        subscriber_email            STRING NOT NULL,
        canonical_server_name       STRING NOT NULL,
        notification_destination_id STRING,
        is_active                   BOOLEAN NOT NULL,
        notes                       STRING,
        created_ts                  TIMESTAMP NOT NULL,
        updated_ts                  TIMESTAMP NOT NULL
    )
    USING DELTA
    COMMENT 'Maps Databricks native-alert recipients to one or more monitored SQL Servers'
    TBLPROPERTIES (
        'delta.enableChangeDataFeed' = 'true',
        'quality' = 'configuration',
        'agent.owner' = 'sql-server-observability-agent'
    )
    """
)

print(
    "Alert subscription table ready: "
    f"{CATALOG}.{SCHEMA}.agent_alert_subscriptions"
)
# -------------------------------------------------------------------
# 6. Daily ingestion-run tracking
# -------------------------------------------------------------------

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {table_name("agent_ingestion_runs")} (
        run_id                       STRING,
        run_date                     DATE,
        source_timezone              STRING,
        run_status                   STRING,
        run_trigger                  STRING,
        expected_server_count        INT,
        discovered_workbook_count    INT,
        identified_server_count      INT,
        valid_workbook_count         INT,
        invalid_workbook_count       INT,
        windows_event_file_count     INT,
        missing_servers              ARRAY<STRING>,
        duplicate_servers            ARRAY<STRING>,
        unexpected_servers           ARRAY<STRING>,
        started_ts                   TIMESTAMP,
        validation_completed_ts      TIMESTAMP,
        processing_completed_ts      TIMESTAMP,
        error_message                STRING,
        created_ts                   TIMESTAMP,
        updated_ts                   TIMESTAMP
    )
    USING DELTA
    COMMENT 'One record for every Agent ingestion and analysis run'
    TBLPROPERTIES (
        'delta.enableChangeDataFeed' = 'true'
    )
    """
)


# -------------------------------------------------------------------
# 7. Source-file registry and idempotency metadata
# -------------------------------------------------------------------

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {table_name("agent_source_files")} (
        source_file_id                STRING,
        run_id                        STRING,
        source_type                   STRING,
        original_file_name            STRING,
        inbox_file_path               STRING,
        archived_file_path            STRING,
        content_sha256                STRING,
        file_size_bytes               BIGINT,
        file_modification_ts          TIMESTAMP,
        filename_server_candidate     STRING,
        workbook_reported_server      STRING,
        canonical_server_name         STRING,
        collection_date               DATE,
        collection_ts                 TIMESTAMP,
        file_status                   STRING,
        validation_message            STRING,
        discovered_ts                 TIMESTAMP,
        processing_started_ts         TIMESTAMP,
        processing_completed_ts       TIMESTAMP,
        source_record_count           BIGINT,
        created_ts                    TIMESTAMP,
        updated_ts                    TIMESTAMP
    )
    USING DELTA
    COMMENT 'File-level audit, identity validation and idempotency metadata'
    TBLPROPERTIES (
        'delta.enableChangeDataFeed' = 'true'
    )
    """
)


# -------------------------------------------------------------------
# 8. Workbook worksheet manifest
# -------------------------------------------------------------------

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {table_name("agent_sheet_manifest")} (
        run_id                    STRING,
        source_file_id            STRING,
        canonical_server_name     STRING,
        sheet_ordinal             INT,
        sheet_name                STRING,
        sheet_status              STRING,
        no_data_marker_found      BOOLEAN,
        source_row_count          BIGINT,
        ingested_row_count        BIGINT,
        detected_columns_json     STRING,
        processing_started_ts     TIMESTAMP,
        processing_completed_ts   TIMESTAMP,
        error_message             STRING,
        created_ts                TIMESTAMP
    )
    USING DELTA
    COMMENT 'Evidence that every workbook worksheet was inspected'
    TBLPROPERTIES (
        'delta.enableChangeDataFeed' = 'true'
    )
    """
)


# -------------------------------------------------------------------
# 9. Agent SQL diagnostics Bronze table
# -------------------------------------------------------------------

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS
        {table_name("agent_sql_diagnostics_bronze")} (
            run_id                    STRING,
            source_file_id            STRING,
            canonical_server_name     STRING,
            workbook_reported_server  STRING,
            snapshot_date             DATE,
            ingestion_date            DATE,
            sheet_ordinal             INT,
            sheet_name                STRING,
            source_row_number          BIGINT,
            row_json                   STRING,
            source_file_path           STRING,
            ingested_ts                TIMESTAMP
        )
    USING DELTA
    PARTITIONED BY (ingestion_date)
    COMMENT 'Raw worksheet rows ingested for the SQL Server Observability Agent'
    TBLPROPERTIES (
        'delta.enableChangeDataFeed' = 'true'
    )
    """
)


# -------------------------------------------------------------------
# 10. Agent Windows Events Bronze table
# -------------------------------------------------------------------

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS
        {table_name("agent_windows_events_bronze")} (
            run_id                    STRING,
            source_file_id            STRING,
            canonical_server_name     STRING,
            source_server_name        STRING,
            event_fingerprint         STRING,
            event_id                  STRING,
            provider_name             STRING,
            log_name                  STRING,
            container_log             STRING,
            level_display_name        STRING,
            event_time                TIMESTAMP,
            event_date                DATE,
            message                   STRING,
            first_seen_ts             TIMESTAMP,
            last_seen_ts              TIMESTAMP,
            occurrence_count          BIGINT,
            raw_row_json              STRING,
            source_file_path          STRING,
            ingestion_date            DATE,
            ingested_ts               TIMESTAMP
        )
    USING DELTA
    PARTITIONED BY (ingestion_date)
    COMMENT 'Normalized and deduplicated Windows Events Bronze data'
    TBLPROPERTIES (
        'delta.enableChangeDataFeed' = 'true'
    )
    """
)


# -------------------------------------------------------------------
# 11. Insert default configuration without overwriting existing values
# -------------------------------------------------------------------

default_config = [
    (
        "agent_schema_version",
        "1",
        "Current SQL Server Observability Agent metadata schema version",
    ),
    (
        "expected_server_count",
        str(EXPECTED_SERVER_COUNT),
        "Number of active SQL Servers expected in each daily run",
    ),
    (
        "source_timezone",
        SOURCE_TIMEZONE,
        "Business timezone used for collection and job scheduling",
    ),
    (
        "sql_diagnostics_inbox_path",
        SQL_DIAGNOSTICS_INBOX,
        "Shared daily SQL diagnostic workbook landing folder",
    ),
    (
        "sql_diagnostics_by_server_path",
        SQL_DIAGNOSTICS_BY_SERVER,
        "Archive root organized by canonical server name",
    ),
    (
        "windows_events_inbox_path",
        WINDOWS_EVENTS_INBOX,
        "Daily consolidated Windows Events landing folder",
    ),
    (
        "agent_root_path",
        AGENT_ROOT,
        "Agent-specific support directory inside the existing volume",
    ),
]

config_schema = StructType(
    [
        StructField("config_key", StringType(), False),
        StructField("config_value", StringType(), False),
        StructField("description", StringType(), False),
    ]
)

config_df = spark.createDataFrame(
    default_config,
    schema=config_schema,
)

config_df.createOrReplaceTempView("_agent_default_config")

spark.sql(
    f"""
    MERGE INTO {table_name("agent_config")} AS target
    USING _agent_default_config AS source
       ON target.config_key = source.config_key

    WHEN NOT MATCHED THEN
      INSERT (
          config_key,
          config_value,
          description,
          created_ts,
          updated_ts
      )
      VALUES (
          source.config_key,
          source.config_value,
          source.description,
          current_timestamp(),
          current_timestamp()
      )
    """
)

spark.catalog.dropTempView("_agent_default_config")


# -------------------------------------------------------------------
# 12. Validate setup
# -------------------------------------------------------------------

expected_tables = [
    "agent_config",
    "agent_server_registry",
    "agent_alert_subscriptions",
    "agent_ingestion_runs",
    "agent_source_files",
    "agent_sheet_manifest",
    "agent_sql_diagnostics_bronze",
    "agent_windows_events_bronze",
]

available_tables = {
    row.tableName
    for row in spark.sql(
        f"SHOW TABLES IN `{CATALOG}`.`{SCHEMA}`"
    ).collect()
}

missing_tables = [
    name
    for name in expected_tables
    if name not in available_tables
]

if missing_tables:
    raise RuntimeError(
        "Agent setup is incomplete. Missing tables: "
        + ", ".join(missing_tables)
    )

print("")
print("SQL Server Observability Agent setup validation succeeded.")
print(f"Catalog: {CATALOG}")
print(f"Schema: {SCHEMA}")
print(f"Shared volume: {VOLUME_ROOT}")
print(f"Expected daily workbooks: {EXPECTED_SERVER_COUNT}")
print("")
print("Created/validated Agent tables:")

for table in expected_tables:
    print(f"  - {CATALOG}.{SCHEMA}.{table}")