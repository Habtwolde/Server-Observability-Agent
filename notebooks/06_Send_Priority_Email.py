from __future__ import annotations

import hashlib
import html
import json
import re
import smtplib
import ssl
from collections import defaultdict
from datetime import datetime, timezone
from email.message import EmailMessage
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

dbutils.widgets.text("run_id", "", "Run ID (blank = latest run)")
dbutils.widgets.text(
    "recipient_email",
    "habtwolde5@gmail.com",
    "Priority alert recipient",
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
    ["CRITICAL", "HIGH", "MEDIUM"],
    "Minimum severity included",
)
dbutils.widgets.dropdown(
    "allow_test_run",
    "false",
    ["false", "true"],
    "Allow controlled test-run email",
)
dbutils.widgets.dropdown(
    "force_resend",
    "false",
    ["false", "true"],
    "Force resend of an already delivered run",
)
dbutils.widgets.text("smtp_host", "smtp.gmail.com", "SMTP host")
dbutils.widgets.text("smtp_port", "587", "SMTP STARTTLS port")
dbutils.widgets.text(
    "secret_scope",
    "server-observability-agent",
    "Databricks secret scope",
)
dbutils.widgets.text("smtp_user_secret_key", "smtp-user", "SMTP user secret key")
dbutils.widgets.text(
    "smtp_password_secret_key",
    "smtp-app-password",
    "SMTP password secret key",
)

CATALOG = "ent_log_analytics"
SCHEMA = "observability"


def table_name(name: str) -> str:
    return f"`{CATALOG}`.`{SCHEMA}`.`{name}`"


def widget_bool(name: str) -> bool:
    return dbutils.widgets.get(name).strip().lower() == "true"


def widget_int(name: str, minimum: int, maximum: int) -> int:
    raw_value = dbutils.widgets.get(name).strip()
    try:
        value = int(raw_value)
    except ValueError as exc:
        raise ValueError(f"Widget {name} must be an integer: {raw_value!r}") from exc
    if not minimum <= value <= maximum:
        raise ValueError(f"Widget {name} must be between {minimum} and {maximum}: {value}")
    return value


def validate_email(value: str) -> str:
    clean = value.strip()
    if not re.fullmatch(r"[^\s@]+@[^\s@]+\.[^\s@]+", clean):
        raise ValueError(f"Invalid recipient email address: {clean!r}")
    return clean


RUN_ID_PARAMETER = dbutils.widgets.get("run_id").strip()
RECIPIENT_EMAIL = validate_email(dbutils.widgets.get("recipient_email"))
TOP_ISSUES_PER_SERVER = widget_int("top_issues_per_server", 1, 5)
MINIMUM_SEVERITY = dbutils.widgets.get("minimum_severity").strip().upper()
ALLOW_TEST_RUN = widget_bool("allow_test_run")
FORCE_RESEND = widget_bool("force_resend")
SMTP_HOST = dbutils.widgets.get("smtp_host").strip()
SMTP_PORT = widget_int("smtp_port", 1, 65535)
SECRET_SCOPE = dbutils.widgets.get("secret_scope").strip()
SMTP_USER_SECRET_KEY = dbutils.widgets.get("smtp_user_secret_key").strip()
SMTP_PASSWORD_SECRET_KEY = dbutils.widgets.get("smtp_password_secret_key").strip()

if not SMTP_HOST:
    raise ValueError("smtp_host cannot be blank.")
if not SECRET_SCOPE or not SMTP_USER_SECRET_KEY or not SMTP_PASSWORD_SECRET_KEY:
    raise ValueError("Databricks secret scope and SMTP secret keys cannot be blank.")

SEVERITY_RANK = {"LOW": 1, "MEDIUM": 2, "HIGH": 3, "CRITICAL": 4}
if MINIMUM_SEVERITY not in SEVERITY_RANK:
    raise ValueError(f"Unsupported minimum severity: {MINIMUM_SEVERITY}")


# -----------------------------------------------------------------------------
# 2. Select and gate the run
# -----------------------------------------------------------------------------

runs_df = spark.table(f"{CATALOG}.{SCHEMA}.agent_ingestion_runs")
if RUN_ID_PARAMETER:
    run_rows = runs_df.where(F.col("run_id") == RUN_ID_PARAMETER).limit(1).collect()
else:
    run_rows = (
        runs_df.orderBy(F.col("run_date").desc(), F.col("updated_ts").desc())
        .limit(1)
        .collect()
    )

if not run_rows:
    raise RuntimeError(
        f"Agent run was not found: {RUN_ID_PARAMETER or 'latest run'}"
    )

run_row = run_rows[0]
RUN_ID = str(run_row["run_id"])
RUN_DATE = run_row["run_date"]
RUN_STATUS = str(run_row["run_status"])
IS_TEST_RUN = RUN_STATUS.startswith("TEST_")

if RUN_STATUS != "HEALTH_RULES_EVALUATED" and not (
    ALLOW_TEST_RUN and RUN_STATUS == "TEST_HEALTH_RULES_EVALUATED"
):
    raise RuntimeError(
        f"Run {RUN_ID} has status {RUN_STATUS}. Email delivery requires "
        "HEALTH_RULES_EVALUATED. Set allow_test_run=true only for an intentional "
        "historical sample test."
    )

print(f"Run ID: {RUN_ID}")
print(f"Run date: {RUN_DATE}")
print(f"Run status: {RUN_STATUS}")
print(f"Recipient: {RECIPIENT_EMAIL}")
print(f"Priority filter: {MINIMUM_SEVERITY} and above; top {TOP_ISSUES_PER_SERVER} per server")


# -----------------------------------------------------------------------------
# 3. Notification audit table and idempotency key
# -----------------------------------------------------------------------------

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {table_name('agent_notification_log')} (
        notification_id STRING NOT NULL,
        run_id STRING NOT NULL,
        snapshot_date DATE NOT NULL,
        channel STRING NOT NULL,
        recipient STRING NOT NULL,
        minimum_severity STRING NOT NULL,
        top_issues_per_server INT NOT NULL,
        delivery_status STRING NOT NULL,
        subject STRING,
        content_hash STRING,
        affected_server_count INT NOT NULL,
        issue_count INT NOT NULL,
        attempt_count INT NOT NULL,
        sent_ts TIMESTAMP,
        error_message STRING,
        created_ts TIMESTAMP NOT NULL,
        updated_ts TIMESTAMP NOT NULL
    )
    USING DELTA
    TBLPROPERTIES (
        'delta.enableChangeDataFeed' = 'true',
        'quality' = 'operations',
        'agent.owner' = 'sql-server-observability-agent'
    )
    """
)

notification_key = "||".join(
    [RUN_ID, "EMAIL", RECIPIENT_EMAIL.lower(), MINIMUM_SEVERITY, str(TOP_ISSUES_PER_SERVER)]
)
NOTIFICATION_ID = hashlib.sha256(notification_key.encode("utf-8")).hexdigest()

existing_rows = (
    spark.table(f"{CATALOG}.{SCHEMA}.agent_notification_log")
    .where(F.col("notification_id") == NOTIFICATION_ID)
    .limit(1)
    .collect()
)
existing_status = str(existing_rows[0]["delivery_status"]) if existing_rows else ""
existing_attempts = int(existing_rows[0]["attempt_count"] or 0) if existing_rows else 0

if existing_status in {"SENT", "SENDING"} and not FORCE_RESEND:
    result = {
        "run_id": RUN_ID,
        "email_status": f"SKIPPED_ALREADY_{existing_status}",
        "recipient": RECIPIENT_EMAIL,
        "notification_id": NOTIFICATION_ID,
    }
    print(json.dumps(result, indent=2))
    try:
        dbutils.jobs.taskValues.set(key="run_id", value=RUN_ID)
        dbutils.jobs.taskValues.set(key="email_status", value=result["email_status"])
    except Exception:
        pass
    dbutils.notebook.exit(json.dumps(result))


# -----------------------------------------------------------------------------
# 4. Select only priority findings and build the consolidated email
# -----------------------------------------------------------------------------

minimum_rank = SEVERITY_RANK[MINIMUM_SEVERITY]
severity_value = (
    F.when(F.col("severity") == "CRITICAL", F.lit(4))
    .when(F.col("severity") == "HIGH", F.lit(3))
    .when(F.col("severity") == "MEDIUM", F.lit(2))
    .when(F.col("severity") == "LOW", F.lit(1))
    .otherwise(F.lit(0))
)
ranking = Window.partitionBy("canonical_server_name").orderBy(
    severity_value.desc(),
    F.col("priority_score").desc(),
    F.col("rule_id"),
    F.col("entity_name"),
)

priority_df = (
    spark.table(f"{CATALOG}.{SCHEMA}.agent_findings")
    .where(
        (F.col("run_id") == RUN_ID)
        & (F.col("finding_status") == "OPEN")
        & (severity_value >= F.lit(minimum_rank))
    )
    .withColumn("finding_rank", F.row_number().over(ranking))
    .where(F.col("finding_rank") <= TOP_ISSUES_PER_SERVER)
    .select(
        "canonical_server_name",
        "finding_rank",
        "severity",
        "priority_score",
        "domain",
        "rule_title",
        "entity_name",
        "finding_summary",
        "likely_cause",
        "recommended_action",
        "microsoft_reference_url",
        "source_observed_ts",
        "finding_context",
    )
    .orderBy("canonical_server_name", "finding_rank")
)

finding_rows = priority_df.collect()
ISSUE_COUNT = len(finding_rows)
SERVER_COUNT = len({str(row["canonical_server_name"]) for row in finding_rows})

summary_rows = (
    spark.table(f"{CATALOG}.{SCHEMA}.agent_server_health_summary")
    .where(F.col("run_id") == RUN_ID)
    .select(
        "canonical_server_name",
        "health_status",
        "health_score",
        "critical_issue_count",
        "high_issue_count",
        "summary_context",
    )
    .collect()
)
health_by_server = {str(row["canonical_server_name"]): row.asDict() for row in summary_rows}

critical_count = sum(1 for row in finding_rows if row["severity"] == "CRITICAL")
high_count = sum(1 for row in finding_rows if row["severity"] == "HIGH")
test_prefix = "[TEST] " if IS_TEST_RUN else ""
SUBJECT = (
    f"{test_prefix}[SQL DBA Agent] {critical_count} critical, {high_count} high "
    f"priority findings across {SERVER_COUNT} servers | {RUN_DATE}"
)


def escape(value: Any) -> str:
    return html.escape("" if value is None else str(value), quote=True)


def format_score(value: Any) -> str:
    if value is None:
        return "N/A"
    try:
        return f"{float(value):.0f}/100"
    except (TypeError, ValueError):
        return escape(value)


grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
for row in finding_rows:
    grouped[str(row["canonical_server_name"])].append(row.asDict())

plain_lines = [
    "SQL Server Observability Agent — Daily Priority Briefing",
    f"Run: {RUN_ID} | Date: {RUN_DATE} | Status: {RUN_STATUS}",
    f"Included: {MINIMUM_SEVERITY} and above, top {TOP_ISSUES_PER_SERVER} per server",
    "",
]
html_sections: list[str] = []

for server_name in sorted(grouped):
    health = health_by_server.get(server_name, {})
    health_status = health.get("health_status", "UNKNOWN")
    health_score = format_score(health.get("health_score"))
    plain_lines.extend([f"{server_name} — {health_status} — score {health_score}"])
    issue_items: list[str] = []

    for finding in grouped[server_name]:
        rank = finding["finding_rank"]
        severity = str(finding["severity"])
        title = str(finding["rule_title"])
        evidence = str(finding["finding_summary"])
        cause = str(finding["likely_cause"])
        action = str(finding["recommended_action"])
        reference = str(finding["microsoft_reference_url"] or "")

        plain_lines.extend(
            [
                f"  {rank}. [{severity}] {title}",
                f"     Evidence: {evidence}",
                f"     Likely cause: {cause}",
                f"     Immediate action: {action}",
                f"     Microsoft reference: {reference}",
            ]
        )
        issue_items.append(
            "<li style='margin-bottom:16px'>"
            f"<strong>{escape(severity)} — {escape(title)}</strong><br>"
            f"<b>Evidence:</b> {escape(evidence)}<br>"
            f"<b>Likely cause:</b> {escape(cause)}<br>"
            f"<b>Immediate action:</b> {escape(action)}<br>"
            f"<a href='{escape(reference)}'>Microsoft reference</a>"
            "</li>"
        )

    plain_lines.append("")
    html_sections.append(
        "<section style='margin:22px 0;padding:18px;border:1px solid #d0d5dd;"
        "border-radius:10px'>"
        f"<h2 style='margin:0 0 8px'>{escape(server_name)}</h2>"
        f"<p style='margin:0 0 12px'><b>Status:</b> {escape(health_status)} &nbsp; "
        f"<b>Health score:</b> {escape(health_score)}</p>"
        f"<ol>{''.join(issue_items)}</ol>"
        "</section>"
    )

test_warning = (
    "<p style='padding:12px;background:#fff3cd;border-radius:8px'>"
    "<b>Controlled test:</b> This briefing was generated from historical or incomplete sample evidence."
    "</p>"
    if IS_TEST_RUN
    else ""
)

HTML_BODY = f"""
<!doctype html>
<html>
  <body style="font-family:Arial,sans-serif;color:#1d2939;line-height:1.45;max-width:900px;margin:auto">
    <h1>SQL Server Observability Agent</h1>
    <p><b>Daily priority briefing</b><br>
       Run: {escape(RUN_ID)} | Date: {escape(RUN_DATE)} | Status: {escape(RUN_STATUS)}<br>
       Included: {escape(MINIMUM_SEVERITY)} and above, top {TOP_ISSUES_PER_SERVER} per server</p>
    {test_warning}
    <p><b>{ISSUE_COUNT}</b> priority findings require attention across <b>{SERVER_COUNT}</b> servers.</p>
    {''.join(html_sections)}
    <p style="color:#667085;font-size:12px">
      Generated from Agent Delta tables. Healthy and informational observations are intentionally omitted.
    </p>
  </body>
</html>
""".strip()
PLAIN_BODY = "\n".join(plain_lines)
CONTENT_HASH = hashlib.sha256(HTML_BODY.encode("utf-8")).hexdigest()


# -----------------------------------------------------------------------------
# 5. Persist status and send through SMTP without exposing credentials
# -----------------------------------------------------------------------------

log_schema = StructType(
    [
        StructField("notification_id", StringType(), False),
        StructField("run_id", StringType(), False),
        StructField("snapshot_date", DateType(), False),
        StructField("channel", StringType(), False),
        StructField("recipient", StringType(), False),
        StructField("minimum_severity", StringType(), False),
        StructField("top_issues_per_server", IntegerType(), False),
        StructField("delivery_status", StringType(), False),
        StructField("subject", StringType(), True),
        StructField("content_hash", StringType(), True),
        StructField("affected_server_count", IntegerType(), False),
        StructField("issue_count", IntegerType(), False),
        StructField("attempt_count", IntegerType(), False),
        StructField("sent_ts", TimestampType(), True),
        StructField("error_message", StringType(), True),
        StructField("created_ts", TimestampType(), False),
        StructField("updated_ts", TimestampType(), False),
    ]
)


def upsert_delivery_status(
    delivery_status: str,
    *,
    error_message: str | None = None,
    sent_ts: datetime | None = None,
    attempt_count: int | None = None,
) -> None:
    now = datetime.now(timezone.utc)
    row = (
        NOTIFICATION_ID,
        RUN_ID,
        RUN_DATE,
        "EMAIL",
        RECIPIENT_EMAIL,
        MINIMUM_SEVERITY,
        TOP_ISSUES_PER_SERVER,
        delivery_status,
        SUBJECT,
        CONTENT_HASH,
        SERVER_COUNT,
        ISSUE_COUNT,
        existing_attempts if attempt_count is None else attempt_count,
        sent_ts,
        error_message,
        now,
        now,
    )
    spark.createDataFrame([row], log_schema).createOrReplaceTempView(
        "_agent_notification_status"
    )
    spark.sql(
        f"""
        MERGE INTO {table_name('agent_notification_log')} AS target
        USING _agent_notification_status AS source
           ON target.notification_id = source.notification_id
        WHEN MATCHED THEN UPDATE SET
            target.delivery_status = source.delivery_status,
            target.subject = source.subject,
            target.content_hash = source.content_hash,
            target.affected_server_count = source.affected_server_count,
            target.issue_count = source.issue_count,
            target.attempt_count = source.attempt_count,
            target.sent_ts = source.sent_ts,
            target.error_message = source.error_message,
            target.updated_ts = source.updated_ts
        WHEN NOT MATCHED THEN INSERT *
        """
    )
    spark.catalog.dropTempView("_agent_notification_status")


if ISSUE_COUNT == 0:
    upsert_delivery_status("SKIPPED_NO_PRIORITY_ISSUES")
    result = {
        "run_id": RUN_ID,
        "email_status": "SKIPPED_NO_PRIORITY_ISSUES",
        "recipient": RECIPIENT_EMAIL,
        "notification_id": NOTIFICATION_ID,
    }
    print(json.dumps(result, indent=2))
    try:
        dbutils.jobs.taskValues.set(key="run_id", value=RUN_ID)
        dbutils.jobs.taskValues.set(key="email_status", value=result["email_status"])
    except Exception:
        pass
    dbutils.notebook.exit(json.dumps(result))

ATTEMPT_COUNT = existing_attempts + 1
upsert_delivery_status("SENDING", attempt_count=ATTEMPT_COUNT)

try:
    try:
        smtp_user = dbutils.secrets.get(
            scope=SECRET_SCOPE,
            key=SMTP_USER_SECRET_KEY,
        ).strip()
        smtp_password = dbutils.secrets.get(
            scope=SECRET_SCOPE,
            key=SMTP_PASSWORD_SECRET_KEY,
        )
    except Exception as exc:
        raise RuntimeError(
            "SMTP credentials are unavailable. Create Databricks secret scope "
            f"{SECRET_SCOPE!r} with keys {SMTP_USER_SECRET_KEY!r} and "
            f"{SMTP_PASSWORD_SECRET_KEY!r}. For Gmail, store an App Password, "
            "not the normal account password."
        ) from exc

    sender_email = validate_email(smtp_user)
    message = EmailMessage()
    message["Subject"] = SUBJECT
    message["From"] = sender_email
    message["To"] = RECIPIENT_EMAIL
    message.set_content(PLAIN_BODY)
    message.add_alternative(HTML_BODY, subtype="html")

    tls_context = ssl.create_default_context()
    with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=45) as smtp:
        smtp.ehlo()
        smtp.starttls(context=tls_context)
        smtp.ehlo()
        smtp.login(smtp_user, smtp_password)
        smtp.send_message(message)

    sent_at = datetime.now(timezone.utc)
    upsert_delivery_status(
        "SENT",
        sent_ts=sent_at,
        attempt_count=ATTEMPT_COUNT,
    )
    FINAL_EMAIL_STATUS = "SENT"
except Exception as exc:
    safe_error = str(exc)[:2000]
    upsert_delivery_status(
        "FAILED",
        error_message=safe_error,
        attempt_count=ATTEMPT_COUNT,
    )
    raise RuntimeError(f"Priority email delivery failed: {safe_error}") from exc

result = {
    "run_id": RUN_ID,
    "email_status": FINAL_EMAIL_STATUS,
    "recipient": RECIPIENT_EMAIL,
    "notification_id": NOTIFICATION_ID,
    "affected_server_count": SERVER_COUNT,
    "issue_count": ISSUE_COUNT,
}
display(spark.createDataFrame([(key, str(value)) for key, value in result.items()], ["check", "value"]))
print(json.dumps(result, indent=2))

try:
    dbutils.jobs.taskValues.set(key="run_id", value=RUN_ID)
    dbutils.jobs.taskValues.set(key="email_status", value=FINAL_EMAIL_STATUS)
except Exception:
    pass
