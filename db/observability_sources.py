"""Central, validated relation names for the SQL Server Observability Agent."""
from __future__ import annotations

import os
import re

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _identifier(env_name: str, default: str) -> str:
    value = os.getenv(env_name, default).strip()
    if not _IDENTIFIER.fullmatch(value):
        raise RuntimeError(
            f"Invalid Databricks SQL identifier in {env_name}: {value!r}. "
            "Use letters, digits, and underscores only."
        )
    return value


def _qualified(table_env: str, table_default: str) -> str:
    catalog = _identifier("OBS_CATALOG", "ent_log_analytics")
    schema = _identifier("OBS_SCHEMA", "observability")
    relation = _identifier(table_env, table_default)
    return f"`{catalog}`.`{schema}`.`{relation}`"


RUNS_TABLE = _qualified("AGENT_RUNS_TABLE", "agent_ingestion_runs")
SERVER_REGISTRY_TABLE = _qualified("AGENT_SERVER_REGISTRY_TABLE", "agent_server_registry")
ALERT_SUBSCRIPTIONS_TABLE = _qualified(
    "AGENT_ALERT_SUBSCRIPTIONS_TABLE",
    "agent_alert_subscriptions",
)
SOURCE_FILES_TABLE = _qualified("AGENT_SOURCE_FILES_TABLE", "agent_source_files")
SHEET_MANIFEST_TABLE = _qualified("AGENT_SHEET_MANIFEST_TABLE", "agent_sheet_manifest")

SQL_ROWS_TABLE = _qualified("AGENT_SQL_ROWS_TABLE", "agent_sql_rows_silver")
SQL_METRIC_FACTS_TABLE = _qualified("AGENT_SQL_METRICS_TABLE", "agent_sql_metric_facts")
WINDOWS_EVENTS_TABLE = _qualified("AGENT_WINDOWS_EVENTS_TABLE", "agent_windows_events_silver")
SERVER_INVENTORY_VIEW = _qualified(
    "AGENT_SERVER_INVENTORY_VIEW", "v_agent_latest_server_daily_inventory"
)

RULE_CATALOG_TABLE = _qualified("AGENT_RULE_CATALOG_TABLE", "agent_rule_catalog")
FINDINGS_TABLE = _qualified("AGENT_FINDINGS_TABLE", "agent_findings")
CURRENT_FINDINGS_VIEW = _qualified(
    "AGENT_CURRENT_FINDINGS_VIEW", "v_agent_current_findings"
)
TOP_FINDINGS_VIEW = _qualified(
    "AGENT_TOP_FINDINGS_VIEW", "v_agent_top_server_findings"
)
HEALTH_SUMMARY_VIEW = _qualified(
    "AGENT_HEALTH_SUMMARY_VIEW", "v_agent_latest_server_health_summary"
)
NOTIFICATION_LOG_TABLE = _qualified(
    "AGENT_NOTIFICATION_LOG_TABLE", "agent_notification_log"
)

# db.connection imports this compatibility hook. The Agent intentionally does
# not rewrite old Dashboard relations because their column contract differs
# from the new Agent Silver and Gold tables.
LEGACY_RELATION_REPLACEMENTS: dict[str, str] = {}
