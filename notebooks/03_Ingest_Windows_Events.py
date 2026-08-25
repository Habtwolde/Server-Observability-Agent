# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# 03_Ingest_Windows_Events

from __future__ import annotations

import csv
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


# -----------------------------------------------------------------------------
# 1. Runtime parameters
# -----------------------------------------------------------------------------

dbutils.widgets.text("run_id", "", "Run ID (blank = latest run)")
dbutils.widgets.dropdown(
    "allow_incomplete_run",
    "false",
    ["false", "true"],
    "Allow incomplete run for development testing",
)
dbutils.widgets.dropdown(
    "force_reprocess",
    "false",
    ["false", "true"],
    "Force reprocessing of an already ingested source file",
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
AGENT_ROOT = CONFIG["agent_root_path"]

run_id_parameter = dbutils.widgets.get("run_id").strip()
ALLOW_INCOMPLETE_RUN = (
    dbutils.widgets.get("allow_incomplete_run").strip().lower() == "true"
)
FORCE_REPROCESS = (
    dbutils.widgets.get("force_reprocess").strip().lower() == "true"
)

if run_id_parameter:
    RUN_ID = run_id_parameter
else:
    latest_runs = (
        spark.table(f"{CATALOG}.{SCHEMA}.agent_ingestion_runs")
        .orderBy(F.col("run_date").desc(), F.col("updated_ts").desc())
        .select("run_id")
        .limit(1)
        .collect()
    )
    if not latest_runs:
        raise RuntimeError("No Agent ingestion run exists. Run notebooks 01 and 02 first.")
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

run_row = run_rows[0]
RUN_DATE = run_row["run_date"]
PRIOR_RUN_STATUS = run_row["run_status"]

production_ready_statuses = {
    "SQL_DIAGNOSTICS_INGESTED",
    "WINDOWS_EVENTS_INGESTED",
    "INPUTS_INGESTED",
}

if PRIOR_RUN_STATUS not in production_ready_statuses and not ALLOW_INCOMPLETE_RUN:
    raise RuntimeError(
        f"Run {RUN_ID} has status {PRIOR_RUN_STATUS}. "
        "Windows ingestion requires successful SQL diagnostic ingestion. "
        "Use allow_incomplete_run=true only for controlled development testing."
    )

spark.conf.set("spark.sql.session.timeZone", SOURCE_TIMEZONE)

print(f"Run ID: {RUN_ID}")
print(f"Run date: {RUN_DATE}")
print(f"Prior run status: {PRIOR_RUN_STATUS}")
print(f"Source timezone: {SOURCE_TIMEZONE}")
print(f"Allow incomplete run: {ALLOW_INCOMPLETE_RUN}")
print(f"Force reprocess: {FORCE_REPROCESS}")


# -----------------------------------------------------------------------------
# 2. Verified Windows Events CSV contract
# -----------------------------------------------------------------------------

EXPECTED_HEADERS = [
    "Message",
    "ID",
    "ProviderName",
    "LogName",
    "servername",
    "TimeCreated",
    "ContainerLog",
    "LevelDisplayName",
    "CreatedDate",
]

RAW_SCHEMA = StructType(
    [StructField(header, StringType(), True) for header in EXPECTED_HEADERS]
)


def to_fuse_path(path: str) -> str:
    if path.startswith("dbfs:/Volumes/"):
        return path[len("dbfs:"):]
    return path


def normalize_server_name(value: Any) -> str:
    value_text = "" if value is None else str(value)
    return re.sub(r"\s+", "", value_text).strip().upper()


def read_csv_headers(path: str) -> list[str]:
    with open(
        to_fuse_path(path),
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as handle:
        reader = csv.reader(handle)
        return next(reader, [])


def copy_file(source_path: str, destination_path: str) -> bool:
    try:
        destination_directory = str(Path(destination_path).parent)
        dbutils.fs.mkdirs(destination_directory)
        if os.path.exists(to_fuse_path(destination_path)):
            return True
        return bool(dbutils.fs.cp(source_path, destination_path, False))
    except Exception:
        return False


def update_source_file(
    source_file_id: str,
    status: str,
    message: str | None,
    source_record_count: int | None,
    archived_file_path: str | None,
    processing_started_ts: datetime | None,
    processing_completed_ts: datetime | None,
) -> None:
    update_schema = StructType(
        [
            StructField("source_file_id", StringType(), False),
            StructField("file_status", StringType(), False),
            StructField("validation_message", StringType(), True),
            StructField("source_record_count", LongType(), True),
            StructField("archived_file_path", StringType(), True),
            StructField("processing_started_ts", TimestampType(), True),
            StructField("processing_completed_ts", TimestampType(), True),
            StructField("updated_ts", TimestampType(), False),
        ]
    )

    spark.createDataFrame(
        [
            (
                source_file_id,
                status,
                message,
                source_record_count,
                archived_file_path,
                processing_started_ts,
                processing_completed_ts,
                datetime.now(timezone.utc),
            )
        ],
        update_schema,
    ).createOrReplaceTempView("_agent_windows_source_update")

    spark.sql(
        f"""
        MERGE INTO {table_name('agent_source_files')} AS target
        USING _agent_windows_source_update AS source
           ON target.source_file_id = source.source_file_id
        WHEN MATCHED THEN UPDATE SET
            target.file_status = source.file_status,
            target.validation_message = source.validation_message,
            target.source_record_count = source.source_record_count,
            target.archived_file_path = source.archived_file_path,
            target.processing_started_ts = source.processing_started_ts,
            target.processing_completed_ts = source.processing_completed_ts,
            target.updated_ts = source.updated_ts
        """
    )

    spark.catalog.dropTempView("_agent_windows_source_update")


def update_run_status(status: str, error_message: str | None) -> None:
    update_schema = StructType(
        [
            StructField("run_id", StringType(), False),
            StructField("run_status", StringType(), False),
            StructField("processing_completed_ts", TimestampType(), True),
            StructField("error_message", StringType(), True),
            StructField("updated_ts", TimestampType(), False),
        ]
    )

    now = datetime.now(timezone.utc)
    spark.createDataFrame(
        [(RUN_ID, status, now, error_message, now)],
        update_schema,
    ).createOrReplaceTempView("_agent_windows_run_update")

    spark.sql(
        f"""
        MERGE INTO {table_name('agent_ingestion_runs')} AS target
        USING _agent_windows_run_update AS source
           ON target.run_id = source.run_id
        WHEN MATCHED THEN UPDATE SET
            target.run_status = source.run_status,
            target.processing_completed_ts = source.processing_completed_ts,
            target.error_message = source.error_message,
            target.updated_ts = source.updated_ts
        """
    )

    spark.catalog.dropTempView("_agent_windows_run_update")


# -----------------------------------------------------------------------------
# 3. Select the current Windows Events source file
# -----------------------------------------------------------------------------

eligible_statuses = ["VALIDATED", "INGESTED", "INGESTION_FAILED"]

windows_sources = (
    spark.table(f"{CATALOG}.{SCHEMA}.agent_source_files")
    .where(
        (F.col("run_id") == RUN_ID)
        & (F.col("source_type") == "WINDOWS_EVENTS")
        & F.col("file_status").isin(eligible_statuses)
    )
    .orderBy(
        F.col("file_modification_ts").desc(),
        F.col("discovered_ts").desc(),
        F.col("source_file_id").desc(),
    )
    .limit(2)
    .collect()
)

if not windows_sources:
    raise RuntimeError(f"No validated Windows Events CSV was found for {RUN_ID}.")

if len(windows_sources) > 1:
    raise RuntimeError(
        f"More than one eligible Windows Events CSV exists for {RUN_ID}. "
        "Notebook 01 must resolve the daily source file before ingestion."
    )

source = windows_sources[0]
SOURCE_FILE_ID = source["source_file_id"]
SOURCE_PATH = source["inbox_file_path"]

if source["file_status"] == "INGESTED" and not FORCE_REPROCESS:
    print(
        f"Windows Events source {SOURCE_FILE_ID} is already ingested. "
        "No duplicate rows will be written."
    )
    try:
        dbutils.jobs.taskValues.set(key="run_id", value=RUN_ID)
        dbutils.jobs.taskValues.set(
            key="windows_ingestion_status",
            value=PRIOR_RUN_STATUS,
        )
    except Exception:
        pass
    dbutils.notebook.exit("SKIPPED_ALREADY_INGESTED")

if not SOURCE_PATH or not os.path.exists(to_fuse_path(SOURCE_PATH)):
    raise FileNotFoundError(f"Windows Events CSV is not accessible: {SOURCE_PATH}")

actual_headers = read_csv_headers(SOURCE_PATH)
if actual_headers != EXPECTED_HEADERS:
    raise ValueError(
        "Windows Events CSV headers do not match the verified contract. "
        f"Expected={EXPECTED_HEADERS}; actual={actual_headers}"
    )

processing_started = datetime.now(timezone.utc)

archive_directory = (
    f"{AGENT_ROOT}/archive/windows_events/run_date={RUN_DATE.isoformat()}"
)
archive_path = (
    f"{archive_directory}/{SOURCE_FILE_ID[:12]}__{Path(SOURCE_PATH).name}"
)

if not copy_file(SOURCE_PATH, archive_path):
    raise RuntimeError(
        f"Windows Events source could not be archived to {archive_path}"
    )

# -----------------------------------------------------------------------------
# 4. Build the authoritative Windows server alias map
# -----------------------------------------------------------------------------

registry_rows = (
    spark.table(f"{CATALOG}.{SCHEMA}.agent_server_registry")
    .where(F.col("is_active") == True)
    .select(
        "canonical_server_name",
        "workbook_server_aliases",
        "windows_event_server_aliases",
    )
    .collect()
)

alias_to_canonical: dict[str, str] = {}
alias_collisions: list[str] = []

for registry_row in registry_rows:
    canonical = normalize_server_name(registry_row["canonical_server_name"])
    aliases = {
        canonical,
        *(
            normalize_server_name(alias)
            for alias in (registry_row["workbook_server_aliases"] or [])
        ),
        *(
            normalize_server_name(alias)
            for alias in (registry_row["windows_event_server_aliases"] or [])
        ),
    }

    for alias in aliases:
        if not alias:
            continue
        existing = alias_to_canonical.get(alias)
        if existing and existing != canonical:
            alias_collisions.append(f"{alias}: {existing} vs {canonical}")
        else:
            alias_to_canonical[alias] = canonical

if alias_collisions:
    raise RuntimeError(
        "Server alias collisions exist in agent_server_registry: "
        + " | ".join(alias_collisions)
    )

registry_is_initialized = bool(alias_to_canonical)

if not registry_is_initialized and not ALLOW_INCOMPLETE_RUN:
    raise RuntimeError(
        "The Agent server registry is empty. Windows Events cannot be mapped "
        "to the authoritative 45-server fleet."
    )

if registry_is_initialized:
    alias_rows = sorted(alias_to_canonical.items())
    alias_df = spark.createDataFrame(
        alias_rows,
        ["normalized_source_server", "mapped_canonical_server"],
    )
else:
    alias_df = None
    print(
        "Development mode: the server registry is empty, so normalized "
        "uppercase event server names will be used as temporary canonical names."
    )


# -----------------------------------------------------------------------------
# 5. Read, normalize and validate the CSV
# -----------------------------------------------------------------------------

raw_df = (
    spark.read
    .option("header", "true")
    .option("multiLine", "true")
    .option("quote", '"')
    .option("escape", '"')
    .option("mode", "FAILFAST")
    .schema(RAW_SCHEMA)
    .csv(archive_path)
)

raw_row_count = raw_df.count()

if raw_row_count == 0:
    raise ValueError("Windows Events CSV contains no data rows.")

normalized_df = (
    raw_df
    .withColumn(
        "normalized_source_server",
        F.upper(F.regexp_replace(F.trim(F.col("servername")), r"\s+", "")),
    )
    .withColumn(
        "event_time",
        F.coalesce(
            F.expr("try_to_timestamp(TimeCreated, 'M/d/yyyy H:mm')"),
            F.expr("try_to_timestamp(TimeCreated, 'M/d/yyyy H:mm:ss')"),
            F.expr("try_to_timestamp(TimeCreated, 'M/d/yyyy h:mm a')"),
            F.expr("try_to_timestamp(TimeCreated, 'M/d/yyyy h:mm:ss a')"),
            F.expr("try_to_timestamp(TimeCreated)"),
        ),
    )
)

blank_server_count = normalized_df.where(
    F.col("normalized_source_server").isNull()
    | (F.col("normalized_source_server") == "")
).count()

invalid_timestamp_count = normalized_df.where(F.col("event_time").isNull()).count()

if blank_server_count:
    raise ValueError(
        f"Windows Events CSV contains {blank_server_count} rows without servername."
    )

if invalid_timestamp_count:
    invalid_samples = [
        row["TimeCreated"]
        for row in normalized_df.where(F.col("event_time").isNull())
        .select("TimeCreated")
        .distinct()
        .limit(10)
        .collect()
    ]
    raise ValueError(
        f"Windows Events CSV contains {invalid_timestamp_count} invalid TimeCreated "
        f"values. Samples={invalid_samples}"
    )

if registry_is_initialized:
    normalized_df = (
        normalized_df
        .join(F.broadcast(alias_df), "normalized_source_server", "left")
        .withColumn(
            "canonical_server_name",
            F.col("mapped_canonical_server"),
        )
    )

    unmapped_servers = [
        row["normalized_source_server"]
        for row in normalized_df.where(F.col("canonical_server_name").isNull())
        .select("normalized_source_server")
        .distinct()
        .orderBy("normalized_source_server")
        .collect()
    ]

    if unmapped_servers:
        raise ValueError(
            "Windows Events contain servers that are not in the authoritative "
            f"Agent registry: {unmapped_servers}"
        )
else:
    normalized_df = normalized_df.withColumn(
        "canonical_server_name",
        F.col("normalized_source_server"),
    )


# -----------------------------------------------------------------------------
# 6. Create event fingerprints and collapse exact repeated events
# -----------------------------------------------------------------------------

raw_json_columns = [F.col(header).alias(header) for header in EXPECTED_HEADERS]

prepared_df = (
    normalized_df
    .withColumn("event_date", F.to_date(F.col("event_time")))
    .withColumn(
        "normalized_message",
        F.lower(F.regexp_replace(F.trim(F.col("Message")), r"\s+", " ")),
    )
    .withColumn(
        "event_fingerprint",
        F.sha2(
            F.concat_ws(
                "||",
                F.col("canonical_server_name"),
                F.date_format(F.col("event_time"), "yyyy-MM-dd'T'HH:mm:ss"),
                F.upper(F.coalesce(F.trim(F.col("ProviderName")), F.lit(""))),
                F.coalesce(F.trim(F.col("ID")), F.lit("")),
                F.upper(F.coalesce(F.trim(F.col("LogName")), F.lit(""))),
                F.col("normalized_message"),
            ),
            256,
        ),
    )
    .withColumn("raw_row_json", F.to_json(F.struct(*raw_json_columns)))
)

now_timestamp = datetime.now(timezone.utc)

deduplicated_df = (
    prepared_df
    .groupBy("event_fingerprint")
    .agg(
        F.first("canonical_server_name", ignorenulls=True).alias(
            "canonical_server_name"
        ),
        F.first("servername", ignorenulls=True).alias("source_server_name"),
        F.first("ID", ignorenulls=True).alias("event_id"),
        F.first("ProviderName", ignorenulls=True).alias("provider_name"),
        F.first("LogName", ignorenulls=True).alias("log_name"),
        F.first("ContainerLog", ignorenulls=True).alias("container_log"),
        F.first("LevelDisplayName", ignorenulls=True).alias("level_display_name"),
        F.first("event_time", ignorenulls=True).alias("event_time"),
        F.first("event_date", ignorenulls=True).alias("event_date"),
        F.first("Message", ignorenulls=True).alias("message"),
        F.count(F.lit(1)).cast("long").alias("occurrence_count"),
        F.first("raw_row_json", ignorenulls=True).alias("raw_row_json"),
    )
    .select(
        F.lit(RUN_ID).alias("run_id"),
        F.lit(SOURCE_FILE_ID).alias("source_file_id"),
        "canonical_server_name",
        "source_server_name",
        "event_fingerprint",
        "event_id",
        "provider_name",
        "log_name",
        "container_log",
        "level_display_name",
        "event_time",
        "event_date",
        "message",
        F.lit(now_timestamp).cast("timestamp").alias("first_seen_ts"),
        F.lit(now_timestamp).cast("timestamp").alias("last_seen_ts"),
        "occurrence_count",
        "raw_row_json",
        F.lit(archive_path).alias("source_file_path"),
        F.lit(RUN_DATE).cast("date").alias("ingestion_date"),
        F.lit(now_timestamp).cast("timestamp").alias("ingested_ts"),
    )
)

unique_event_count = deduplicated_df.count()
duplicate_rows_collapsed = raw_row_count - unique_event_count
distinct_server_count = deduplicated_df.select("canonical_server_name").distinct().count()

existing_fingerprints = spark.table(
    f"{CATALOG}.{SCHEMA}.agent_windows_events_bronze"
).select("event_fingerprint")

new_event_count = (
    deduplicated_df.select("event_fingerprint")
    .join(existing_fingerprints, "event_fingerprint", "left_anti")
    .count()
)
previously_known_event_count = unique_event_count - new_event_count


# -----------------------------------------------------------------------------
# 7. Idempotently merge Windows Events into Delta
# -----------------------------------------------------------------------------

update_source_file(
    SOURCE_FILE_ID,
    "PROCESSING",
    "Windows Events validation passed; Delta merge started.",
    raw_row_count,
    archive_path,
    processing_started,
    None,
)

deduplicated_df.createOrReplaceTempView("_agent_normalized_windows_events")

try:
    spark.sql(
        f"""
        MERGE INTO {table_name('agent_windows_events_bronze')} AS target
        USING _agent_normalized_windows_events AS source
           ON target.event_fingerprint = source.event_fingerprint

        WHEN MATCHED
          AND source.occurrence_count > target.occurrence_count
        THEN UPDATE SET
            target.occurrence_count = source.occurrence_count,
            target.last_seen_ts = source.last_seen_ts

        WHEN NOT MATCHED THEN INSERT *
        """
    )
except Exception as merge_exc:
    quarantine_directory = (
        f"{AGENT_ROOT}/quarantine/windows_events/"
        f"run_date={RUN_DATE.isoformat()}"
    )
    quarantine_path = (
        f"{quarantine_directory}/{SOURCE_FILE_ID[:12]}__{Path(SOURCE_PATH).name}"
    )
    copy_file(SOURCE_PATH, quarantine_path)
    failure_message = (
        f"Windows Events Delta merge failed: {merge_exc} | "
        f"quarantine={quarantine_path}"
    )[:10000]
    update_source_file(
        SOURCE_FILE_ID,
        "INGESTION_FAILED",
        failure_message,
        raw_row_count,
        archive_path,
        processing_started,
        datetime.now(timezone.utc),
    )
    update_run_status("WINDOWS_EVENTS_INGESTION_FAILED", failure_message)
    raise RuntimeError(failure_message) from merge_exc

spark.catalog.dropTempView("_agent_normalized_windows_events")

completed_ts = datetime.now(timezone.utc)

update_source_file(
    SOURCE_FILE_ID,
    "INGESTED",
    (
        f"Windows Events ingested successfully: raw_rows={raw_row_count}, "
        f"unique_events={unique_event_count}, "
        f"duplicates_collapsed={duplicate_rows_collapsed}, "
        f"new_events={new_event_count}, "
        f"previously_known_events={previously_known_event_count}."
    ),
    raw_row_count,
    archive_path,
    processing_started,
    completed_ts,
)

if PRIOR_RUN_STATUS in production_ready_statuses:
    final_run_status = "INPUTS_INGESTED"
    final_error_message = None
else:
    final_run_status = "TEST_INPUTS_INGESTED"
    final_error_message = (
        "SQL and Windows inputs were ingested under allow_incomplete_run=true "
        "for development testing."
    )

update_run_status(final_run_status, final_error_message)


# -----------------------------------------------------------------------------
# 8. Agent Windows Events views and validation output
# -----------------------------------------------------------------------------

spark.sql(
    f"""
    CREATE OR REPLACE VIEW {table_name('v_agent_windows_events')} AS
    SELECT
        run_id,
        source_file_id,
        canonical_server_name,
        source_server_name,
        event_fingerprint,
        event_id,
        provider_name,
        log_name,
        container_log,
        level_display_name,
        event_time,
        event_date,
        message,
        first_seen_ts,
        last_seen_ts,
        occurrence_count,
        source_file_path,
        ingestion_date,
        ingested_ts
    FROM {table_name('agent_windows_events_bronze')}
    """
)

spark.sql(
    f"""
    CREATE OR REPLACE VIEW {table_name('v_agent_windows_events_by_run')} AS
    SELECT *
    FROM {table_name('v_agent_windows_events')}
    WHERE run_id IS NOT NULL
    """
)

event_time_summary = deduplicated_df.agg(
    F.min("event_time").alias("minimum_event_time"),
    F.max("event_time").alias("maximum_event_time"),
).collect()[0]

summary_rows = [
    ("run_id", RUN_ID),
    ("source_file_id", SOURCE_FILE_ID),
    ("raw_row_count", str(raw_row_count)),
    ("unique_event_count", str(unique_event_count)),
    ("duplicate_rows_collapsed", str(duplicate_rows_collapsed)),
    ("new_event_count", str(new_event_count)),
    ("previously_known_event_count", str(previously_known_event_count)),
    ("distinct_server_count", str(distinct_server_count)),
    ("minimum_event_time", str(event_time_summary["minimum_event_time"])),
    ("maximum_event_time", str(event_time_summary["maximum_event_time"])),
    ("final_run_status", final_run_status),
    ("archive_path", archive_path),
]

display(spark.createDataFrame(summary_rows, ["check", "value"]))

display(
    deduplicated_df
    .groupBy("canonical_server_name", "level_display_name")
    .agg(
        F.count("*").alias("unique_events"),
        F.sum("occurrence_count").alias("source_occurrences"),
    )
    .orderBy("canonical_server_name", "level_display_name")
)

print(f"Final run status: {final_run_status}")

try:
    dbutils.jobs.taskValues.set(key="run_id", value=RUN_ID)
    dbutils.jobs.taskValues.set(
        key="windows_ingestion_status",
        value=final_run_status,
    )
except Exception:
    pass