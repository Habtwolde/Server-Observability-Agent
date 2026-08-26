from __future__ import annotations

import os
import re

import pandas as pd
import streamlit as st
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import sql as dbsql

from db.observability_sources import LEGACY_RELATION_REPLACEMENTS


class DatabricksQueryError(RuntimeError):
    """A query failure that can be rendered safely by the Streamlit entry point."""


@st.cache_resource
def get_workspace_client() -> WorkspaceClient:
    """Return a cached Databricks workspace client."""
    return WorkspaceClient()


_RELATION_PATTERNS = tuple(
    (
        re.compile(
            rf"(?i)(?<![\w`])`?{re.escape(legacy.split('.')[0])}`?\s*\.\s*"
            rf"`?{re.escape(legacy.split('.')[1])}`?\s*\.\s*"
            rf"`?{re.escape(legacy.split('.')[2])}`?(?![\w`])"
        ),
        configured,
    )
    for legacy, configured in LEGACY_RELATION_REPLACEMENTS.items()
)


def _normalize_observability_sources(query: str) -> str:
    """Replace historic hard-coded FQNs with the configured source contract.

    This compatibility layer is deliberately restricted to known observability
    relations. It does not rewrite arbitrary SQL and does not affect Vector
    Search index identifiers, which are not sent through this SQL execution path.
    """
    normalized = query
    for pattern, configured_relation in _RELATION_PATTERNS:
        normalized = pattern.sub(configured_relation, normalized)
    return normalized


def clear_query_cache() -> None:
    """Clear cached SQL results before a user-requested dashboard refresh."""
    run_query.clear()


@st.cache_data(ttl=60, show_spinner=False)
def run_query(query: str) -> pd.DataFrame:
    """Execute a Databricks SQL statement and return the first result chunk as a DataFrame."""
    if not query or not str(query).strip():
        raise ValueError("Query must not be empty.")

    warehouse_id = os.getenv("DATABRICKS_WAREHOUSE_ID", "").strip()
    if not warehouse_id:
        raise DatabricksQueryError("DATABRICKS_WAREHOUSE_ID is not configured for this application.")

    statement = _normalize_observability_sources(str(query))
    workspace = get_workspace_client()

    try:
        response = workspace.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            statement=statement,
            wait_timeout="30s",
        )
    except Exception as exc:
        raise DatabricksQueryError(f"Databricks SQL request could not be submitted: {exc}") from exc

    state = response.status.state if response.status else None
    if state != dbsql.StatementState.SUCCEEDED:
        detail = "No Databricks error detail was returned."
        if response.status and getattr(response.status, "error", None):
            detail = str(response.status.error)
        raise DatabricksQueryError(f"Databricks SQL query failed. {detail}")

    rows = response.result.data_array if (response.result and response.result.data_array) else []
    columns = []
    if response.manifest and response.manifest.schema and response.manifest.schema.columns:
        columns = [column.name for column in response.manifest.schema.columns]

    if not columns:
        return pd.DataFrame()
    return pd.DataFrame(rows, columns=columns)
def execute_sql(statement: str) -> None:
    """Execute a Databricks SQL write/DDL statement without result caching."""

    if not statement or not str(statement).strip():
        raise ValueError("SQL statement must not be empty.")

    warehouse_id = os.getenv(
        "DATABRICKS_WAREHOUSE_ID",
        "",
    ).strip()

    if not warehouse_id:
        raise DatabricksQueryError(
            "DATABRICKS_WAREHOUSE_ID is not configured "
            "for this application."
        )

    sql_statement = _normalize_observability_sources(
        str(statement)
    )

    workspace = get_workspace_client()

    try:
        response = workspace.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            statement=sql_statement,
            wait_timeout="30s",
        )
    except Exception as exc:
        raise DatabricksQueryError(
            "Databricks SQL write request could not be "
            f"submitted: {exc}"
        ) from exc

    state = (
        response.status.state
        if response.status
        else None
    )

    if state != dbsql.StatementState.SUCCEEDED:
        detail = "No Databricks error detail was returned."

        if (
            response.status
            and getattr(response.status, "error", None)
        ):
            detail = str(response.status.error)

        raise DatabricksQueryError(
            f"Databricks SQL statement failed. {detail}"
        )