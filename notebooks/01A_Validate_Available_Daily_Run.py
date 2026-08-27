# Databricks notebook source
# 01A_Validate_Available_Daily_Run
#
# Production orchestration wrapper around 01_Register_and_Validate_Daily_Run.
#
# Goal:
# - keep the authoritative registry and file-validation logic in notebook 01;
# - require a valid Windows Events file;
# - continue a production run when at least one registered SQL Server workbook
#   is valid, while missing/invalid/unexpected workbooks are recorded and skipped;
# - keep TEST behavior isolated from PRODUCTION.

from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from pyspark.sql import functions as F


# -----------------------------------------------------------------------------
# 1. Runtime parameters
# -----------------------------------------------------------------------------

dbutils.widgets.text(
    "run_date",
    "",
    "Run date (YYYY-MM-DD; blank = current business date)",
)

dbutils.widgets.dropdown(
    "bootstrap_registry",
    "false",
    ["false", "true"],
    "Bootstrap registry from a complete first production set",
)

dbutils.widgets.dropdown(
    "execution_mode",
    "PRODUCTION",
    ["PRODUCTION", "TEST"],
    "Execution mode",
)

CATALOG = "ent_log_analytics"
SCHEMA = "observability"

RUN_DATE_PARAMETER = dbutils.widgets.get("run_date").strip()
BOOTSTRAP_REGISTRY = (
    dbutils.widgets.get("bootstrap_registry").strip().lower() == "true"
)
EXECUTION_MODE = dbutils.widgets.get("execution_mode").strip().upper()

if EXECUTION_MODE not in {"PRODUCTION", "TEST"}:
    raise ValueError(f"Invalid execution_mode: {EXECUTION_MODE}")

CONFIG = {
    row["config_key"]: row["config_value"]
    for row in spark.table(f"{CATALOG}.{SCHEMA}.agent_config")
    .select("config_key", "config_value")
    .collect()
}

SOURCE_TIMEZONE = CONFIG.get("source_timezone", "America/New_York")

if RUN_DATE_PARAMETER:
    TARGET_RUN_DATE = RUN_DATE_PARAMETER
else:
    TARGET_RUN_DATE = datetime.now(ZoneInfo(SOURCE_TIMEZONE)).date().isoformat()

RUN_ID = f"RUN-{TARGET_RUN_DATE.replace('-', '')}"

current_notebook_path = (
    dbutils.notebook.entry_point
    .getDbutils()
    .notebook()
    .getContext()
    .notebookPath()
    .get()
)

NOTEBOOKS_ROOT = current_notebook_path.rsplit("/", 1)[0]
VALIDATION_NOTEBOOK = f"{NOTEBOOKS_ROOT}/01_Register_and_Validate_Daily_Run"

print("Daily production-input validation")
print(f"Run ID: {RUN_ID}")
print(f"Run date: {TARGET_RUN_DATE}")
print(f"Execution mode: {EXECUTION_MODE}")
print(f"Registry bootstrap: {BOOTSTRAP_REGISTRY}")


# -----------------------------------------------------------------------------
# 2. Run the authoritative validator without aborting on fleet incompleteness
# -----------------------------------------------------------------------------

# Notebook 01 remains the source of truth for:
# - workbook identity validation;
# - duplicate/unexpected server detection;
# - the 45-server registry;
# - Windows Events file discovery;
# - file audit records and manifests.
#
# fail_on_incomplete=false lets this wrapper decide whether the available
# production inputs are sufficient to process the valid servers.

dbutils.notebook.run(
    VALIDATION_NOTEBOOK,
    timeout_seconds=3600,
    arguments={
        "run_date": TARGET_RUN_DATE,
        "bootstrap_registry": str(BOOTSTRAP_REGISTRY).lower(),
        "fail_on_incomplete": "false",
        "run_trigger": "SCHEDULED",
        "execution_mode": EXECUTION_MODE,
    },
)


# -----------------------------------------------------------------------------
# 3. Read the persisted validation result
# -----------------------------------------------------------------------------

run_rows = (
    spark.table(f"{CATALOG}.{SCHEMA}.agent_ingestion_runs")
    .where(F.col("run_id") == RUN_ID)
    .orderBy(F.col("updated_ts").desc())
    .limit(1)
    .collect()
)

if not run_rows:
    raise RuntimeError(f"Validator did not persist run {RUN_ID}.")

run_row = run_rows[0]
original_status = str(run_row["run_status"])
valid_workbook_count = int(run_row["valid_workbook_count"] or 0)
invalid_workbook_count = int(run_row["invalid_workbook_count"] or 0)
windows_event_file_count = int(run_row["windows_event_file_count"] or 0)
missing_servers = list(run_row["missing_servers"] or [])
duplicate_servers = list(run_row["duplicate_servers"] or [])
unexpected_servers = list(run_row["unexpected_servers"] or [])

registry_count = (
    spark.table(f"{CATALOG}.{SCHEMA}.agent_server_registry")
    .where("is_active = true AND expected_daily_workbook = true")
    .count()
)

print("")
print("Authoritative validation result")
print(f"Original status: {original_status}")
print(f"Registered servers: {registry_count}")
print(f"Valid SQL workbooks: {valid_workbook_count}")
print(f"Invalid/skipped SQL workbooks: {invalid_workbook_count}")
print(f"Windows Events files: {windows_event_file_count}")
print(f"Missing registered servers: {len(missing_servers)}")
print(f"Duplicate servers: {len(duplicate_servers)}")
print(f"Unexpected servers: {len(unexpected_servers)}")


# -----------------------------------------------------------------------------
# 4. Production decision
# -----------------------------------------------------------------------------

if EXECUTION_MODE == "TEST":
    if original_status != "TEST_READY_FOR_INGESTION":
        raise RuntimeError(
            f"TEST validation did not reach TEST_READY_FOR_INGESTION: "
            f"{original_status}"
        )

    final_status = original_status
    final_message = None

else:
    if registry_count <= 0:
        raise RuntimeError(
            "The production server registry is not initialized. "
            "Bootstrap it once from the complete approved server set."
        )

    if windows_event_file_count != 1:
        raise RuntimeError(
            "Production processing requires exactly one valid Windows Events "
            f"CSV for the run date. Found: {windows_event_file_count}."
        )

    if valid_workbook_count <= 0:
        raise RuntimeError(
            "No valid registered SQL Server workbook is available for this run."
        )

    # A complete fleet and a partial-but-usable fleet both enter the normal
    # production pipeline. Notebook 02 only ingests source files whose status
    # is VALIDATED, so missing, duplicate, unexpected and invalid workbooks are
    # automatically excluded from server analysis.
    final_status = "READY_FOR_INGESTION"

    if (
        original_status == "READY_FOR_INGESTION"
        and invalid_workbook_count == 0
        and not missing_servers
        and not duplicate_servers
        and not unexpected_servers
    ):
        final_message = None
    else:
        final_message = (
            "Partial production run accepted. Only validated registered SQL "
            "Server workbooks will be processed. "
            f"valid={valid_workbook_count}; "
            f"invalid_or_skipped={invalid_workbook_count}; "
            f"missing={len(missing_servers)}; "
            f"duplicates={len(duplicate_servers)}; "
            f"unexpected={len(unexpected_servers)}."
        )

    spark.sql(
        f"""
        UPDATE `{CATALOG}`.`{SCHEMA}`.`agent_ingestion_runs`
        SET
            run_status = 'READY_FOR_INGESTION',
            error_message = {("NULL" if final_message is None else repr(final_message))},
            updated_ts = current_timestamp()
        WHERE run_id = '{RUN_ID}'
        """
    )


# -----------------------------------------------------------------------------
# 5. Publish downstream task values
# -----------------------------------------------------------------------------

try:
    dbutils.jobs.taskValues.set(key="run_id", value=RUN_ID)
    dbutils.jobs.taskValues.set(key="run_status", value=final_status)
    dbutils.jobs.taskValues.set(
        key="valid_workbook_count",
        value=valid_workbook_count,
    )
except Exception:
    pass

print("")
print(f"Final orchestration status: {final_status}")
if final_message:
    print(final_message)
