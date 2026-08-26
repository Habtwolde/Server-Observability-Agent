# Databricks notebook source
from __future__ import annotations

import hashlib
import re
from datetime import datetime, timezone

from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


# -----------------------------------------------------------------------------
# 1. Configuration
# -----------------------------------------------------------------------------

CATALOG = "ent_log_analytics"
SCHEMA = "observability"

SUBSCRIPTION_TABLE = "agent_alert_subscriptions"
SERVER_REGISTRY_TABLE = "agent_server_registry"


def table_name(name: str) -> str:
    return f"`{CATALOG}`.`{SCHEMA}`.`{name}`"


# -----------------------------------------------------------------------------
# 2. Load active monitored servers
# -----------------------------------------------------------------------------

active_server_rows = (
    spark.table(
        f"{CATALOG}.{SCHEMA}.{SERVER_REGISTRY_TABLE}"
    )
    .where(
        (F.col("is_active") == True)
        & (F.col("expected_daily_workbook") == True)
    )
    .select("canonical_server_name")
    .distinct()
    .orderBy("canonical_server_name")
    .collect()
)

ACTIVE_SERVERS = [
    str(row["canonical_server_name"]).strip().upper()
    for row in active_server_rows
    if row["canonical_server_name"]
]

if not ACTIVE_SERVERS:
    raise RuntimeError(
        "No active monitored servers are available in "
        "agent_server_registry."
    )


# -----------------------------------------------------------------------------
# 3. User configuration widgets
# -----------------------------------------------------------------------------

# Remove the old free-text server widget from the previous notebook version.
try:
    dbutils.widgets.remove("server_names")
except Exception:
    pass


dbutils.widgets.text(
    "subscriber_email",
    "",
    "Subscriber email address",
)

dbutils.widgets.multiselect(
    "selected_servers",
    "<SELECT_SERVER>",
    ["<SELECT_SERVER>", *ACTIVE_SERVERS],
    "Server(s) this subscriber should receive",
)

dbutils.widgets.dropdown(
    "action",
    "UPSERT",
    ["UPSERT", "DISABLE"],
    "Subscription action",
)

dbutils.widgets.text(
    "notes",
    "",
    "Optional notes",
)


# -----------------------------------------------------------------------------
# 4. Read user selections
# -----------------------------------------------------------------------------

SUBSCRIBER_EMAIL_RAW = (
    dbutils.widgets.get("subscriber_email")
    .strip()
    .lower()
)

SELECTED_SERVERS = sorted(
    {
        value.strip().upper()
        for value in dbutils.widgets.get(
            "selected_servers"
        ).split(",")
        if value.strip()
        and value.strip() != "<SELECT_SERVER>"
    }
)

ACTION = (
    dbutils.widgets.get("action")
    .strip()
    .upper()
)

NOTES = (
    dbutils.widgets.get("notes")
    .strip()
)


# -----------------------------------------------------------------------------
# 5. Show configuration interface safely when nothing is entered
# -----------------------------------------------------------------------------

if not SUBSCRIBER_EMAIL_RAW:
    print("")
    print("Alert subscription configuration")
    print("--------------------------------")
    print(
        "Enter a subscriber email address using the "
        "'Subscriber email address' field above."
    )
    print(
        "Then select one or more servers from the "
        "'Server(s) this subscriber should receive' field."
    )
    print("")
    print(
        f"Active monitored servers available: "
        f"{len(ACTIVE_SERVERS)}"
    )

    print("")
    print("Current active subscriptions:")

    display(
        spark.table(
            f"{CATALOG}.{SCHEMA}.{SUBSCRIPTION_TABLE}"
        )
        .where(F.col("is_active") == True)
        .select(
            "subscriber_email",
            "canonical_server_name",
            "notes",
            "updated_ts",
        )
        .orderBy(
            "canonical_server_name",
            "subscriber_email",
        )
    )

    dbutils.notebook.exit(
        "WAITING_FOR_SUBSCRIPTION_CONFIGURATION"
    )


# -----------------------------------------------------------------------------
# 6. Validate email and server selection
# -----------------------------------------------------------------------------

if not re.fullmatch(
    r"[^\s@]+@[^\s@]+\.[^\s@]+",
    SUBSCRIBER_EMAIL_RAW,
):
    raise ValueError(
        f"Invalid subscriber email address: "
        f"{SUBSCRIBER_EMAIL_RAW!r}"
    )

SUBSCRIBER_EMAIL = SUBSCRIBER_EMAIL_RAW


if not SELECTED_SERVERS:
    raise ValueError(
        "Select at least one server from the "
        "'Server(s) this subscriber should receive' field."
    )


INVALID_SERVERS = sorted(
    set(SELECTED_SERVERS) - set(ACTIVE_SERVERS)
)

if INVALID_SERVERS:
    raise RuntimeError(
        "The following selected servers are not active "
        "registered servers: "
        + ", ".join(INVALID_SERVERS)
    )


if ACTION not in {"UPSERT", "DISABLE"}:
    raise ValueError(
        f"Unsupported subscription action: {ACTION}"
    )


print("")
print("Alert subscription request")
print(f"Subscriber: {SUBSCRIBER_EMAIL}")
print(
    "Selected servers: "
    + ", ".join(SELECTED_SERVERS)
)
print(f"Action: {ACTION}")


# -----------------------------------------------------------------------------
# 7. Build deterministic subscription records
# -----------------------------------------------------------------------------

now_utc = datetime.now(timezone.utc)

subscription_rows = []

for server_name in SELECTED_SERVERS:

    subscription_key = (
        f"{SUBSCRIBER_EMAIL}||{server_name}"
    )

    subscription_id = hashlib.sha256(
        subscription_key.encode("utf-8")
    ).hexdigest()

    subscription_rows.append(
        (
            subscription_id,
            SUBSCRIBER_EMAIL,
            server_name,
            None,
            ACTION == "UPSERT",
            NOTES or None,
            now_utc,
            now_utc,
        )
    )


subscription_schema = StructType(
    [
        StructField(
            "subscription_id",
            StringType(),
            False,
        ),
        StructField(
            "subscriber_email",
            StringType(),
            False,
        ),
        StructField(
            "canonical_server_name",
            StringType(),
            False,
        ),
        StructField(
            "notification_destination_id",
            StringType(),
            True,
        ),
        StructField(
            "is_active",
            BooleanType(),
            False,
        ),
        StructField(
            "notes",
            StringType(),
            True,
        ),
        StructField(
            "created_ts",
            TimestampType(),
            False,
        ),
        StructField(
            "updated_ts",
            TimestampType(),
            False,
        ),
    ]
)


subscription_df = spark.createDataFrame(
    subscription_rows,
    subscription_schema,
)

subscription_df.createOrReplaceTempView(
    "_agent_alert_subscription_changes"
)


# -----------------------------------------------------------------------------
# 8. Apply subscription changes
# -----------------------------------------------------------------------------

if ACTION == "UPSERT":

    spark.sql(
        f"""
        MERGE INTO {table_name(SUBSCRIPTION_TABLE)} AS target

        USING _agent_alert_subscription_changes AS source

           ON target.subscription_id =
              source.subscription_id

        WHEN MATCHED THEN UPDATE SET
            target.subscriber_email =
                source.subscriber_email,

            target.canonical_server_name =
                source.canonical_server_name,

            target.is_active = true,

            target.notes =
                source.notes,

            target.updated_ts =
                source.updated_ts

        WHEN NOT MATCHED THEN INSERT *
        """
    )

else:

    spark.sql(
        f"""
        MERGE INTO {table_name(SUBSCRIPTION_TABLE)} AS target

        USING _agent_alert_subscription_changes AS source

           ON target.subscription_id =
              source.subscription_id

        WHEN MATCHED THEN UPDATE SET
            target.is_active = false,
            target.notes = source.notes,
            target.updated_ts = source.updated_ts
        """
    )


spark.catalog.dropTempView(
    "_agent_alert_subscription_changes"
)


# -----------------------------------------------------------------------------
# 9. Validate and display subscriber configuration
# -----------------------------------------------------------------------------

subscriber_df = (
    spark.table(
        f"{CATALOG}.{SCHEMA}.{SUBSCRIPTION_TABLE}"
    )
    .where(
        F.col("subscriber_email")
        == SUBSCRIBER_EMAIL
    )
    .select(
        "subscriber_email",
        "canonical_server_name",
        "is_active",
        "notes",
        "updated_ts",
    )
    .orderBy(
        "canonical_server_name"
    )
)


print("")
print("Subscription configuration complete")
print(f"Subscriber: {SUBSCRIBER_EMAIL}")


display(subscriber_df)