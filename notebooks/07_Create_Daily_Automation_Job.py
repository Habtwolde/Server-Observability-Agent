# Databricks notebook source
from __future__ import annotations

import json
import re
from typing import Any

import requests
from databricks.sdk import WorkspaceClient


# -----------------------------------------------------------------------------
# 1. Deployment parameters
# -----------------------------------------------------------------------------
#
# Run this notebook only after notebooks 01-06 exist in the workspace folder.
#
# It will eventually create/update:
#   1. The Databricks-native SQL Alert.
#   2. The daily production Workflow that invokes that Alert.
#
# Keep the production schedule PAUSED until the real 45-server daily
# source set is available and native notification delivery has been tested.
# -----------------------------------------------------------------------------


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
    "alert_name",
    "prod-server-observability-agent-priority",
    "Databricks SQL Alert name",
)

dbutils.widgets.text(
    "subscriber_email",
    "",
    "Databricks Alert subscriber email",
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


def validate_optional_email(value: str) -> str:
    clean = value.strip()

    if not clean:
        return ""

    if not re.fullmatch(
        r"[^\s@]+@[^\s@]+\.[^\s@]+",
        clean,
    ):
        raise ValueError(
            f"Invalid subscriber email address: {clean!r}"
        )

    return clean


NOTEBOOKS_ROOT = (
    dbutils.widgets.get("notebooks_root")
    .strip()
    .rstrip("/")
)

JOB_NAME = dbutils.widgets.get("job_name").strip()

ALERT_NAME = dbutils.widgets.get("alert_name").strip()

SUBSCRIBER_EMAIL = validate_optional_email(
    dbutils.widgets.get("subscriber_email")
)

WAREHOUSE_ID = (
    dbutils.widgets.get("warehouse_id")
    .strip()
)

TOP_ISSUES_PER_SERVER = int(
    dbutils.widgets.get("top_issues_per_server")
)

SCHEDULE_STATE = (
    dbutils.widgets.get("schedule_state")
    .strip()
    .upper()
)


if not NOTEBOOKS_ROOT.startswith("/Users/"):
    raise ValueError(
        "notebooks_root must be an absolute "
        "/Users/... workspace path."
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


NOTEBOOK_NAMES = [
    "01_Register_and_Validate_Daily_Run",
    "02_Ingest_SQL_Diagnostics_Workbooks",
    "03_Ingest_Windows_Events",
    "04_Build_Analysis_Ready_Tables",
    "05_Evaluate_Server_Health_Rules",
    "06_Prepare_Databricks_Alert",
]


print("Databricks native notification deployment")
print(f"Workflow: {JOB_NAME}")
print(f"Alert: {ALERT_NAME}")
print(f"Warehouse ID: {WAREHOUSE_ID}")
print(
    f"Subscriber: "
    f"{SUBSCRIBER_EMAIL or 'not configured yet'}"
)
print(
    f"Top issues per server: "
    f"{TOP_ISSUES_PER_SERVER}"
)
print(f"Schedule state: {SCHEDULE_STATE}")