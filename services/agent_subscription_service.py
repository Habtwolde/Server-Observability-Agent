"""Backend service for server-specific Databricks alert subscriptions."""

from __future__ import annotations

import hashlib
import re
from typing import Iterable

import pandas as pd

from db.connection import (
    clear_query_cache,
    execute_sql,
    run_query,
)
from db.observability_sources import (
    ALERT_SUBSCRIPTIONS_TABLE,
    HEALTH_SUMMARY_VIEW,
    SERVER_REGISTRY_TABLE,
)


_EMAIL_PATTERN = re.compile(
    r"^[^\s@]+@[^\s@]+\.[^\s@]+$"
)

_SERVER_PATTERN = re.compile(
    r"^[A-Z0-9][A-Z0-9._$\\-]{0,127}$"
)


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------


def _sql_quote(value: str) -> str:
    """Escape a string for use inside a SQL string literal."""
    return str(value).replace("'", "''")


def _normalize_email(value: str) -> str:
    email = str(value or "").strip().lower()

    if not email:
        raise ValueError("Subscriber email is required.")

    if not _EMAIL_PATTERN.fullmatch(email):
        raise ValueError(
            f"Invalid subscriber email address: {email!r}"
        )

    return email


def _normalize_server(value: str) -> str:
    server = str(value or "").strip().upper()

    if not server:
        raise ValueError("Server name is required.")

    if not _SERVER_PATTERN.fullmatch(server):
        raise ValueError(
            f"Invalid canonical server name: {server!r}"
        )

    return server


def _normalize_servers(
    server_names: Iterable[str],
) -> list[str]:
    servers = sorted(
        {
            _normalize_server(value)
            for value in server_names
            if str(value or "").strip()
        }
    )

    if not servers:
        raise ValueError(
            "Select at least one server."
        )

    return servers


def _subscription_id(
    subscriber_email: str,
    canonical_server_name: str,
) -> str:
    key = (
        f"{subscriber_email.lower()}"
        f"||{canonical_server_name.upper()}"
    )

    return hashlib.sha256(
        key.encode("utf-8")
    ).hexdigest()


# -----------------------------------------------------------------------------
# Available servers
# -----------------------------------------------------------------------------


def load_available_servers() -> list[str]:
    """
    Return servers available for subscription.

    Production source:
        active expected servers in agent_server_registry.

    Development fallback:
        servers currently present in the Agent health-summary view.

    The fallback lets the Streamlit configuration UI work before the
    authoritative 45-server registry has been populated.
    """

    registry_df = run_query(
        f"""
        SELECT DISTINCT
            canonical_server_name
        FROM {SERVER_REGISTRY_TABLE}
        WHERE is_active = true
          AND expected_daily_workbook = true
          AND canonical_server_name IS NOT NULL
        ORDER BY canonical_server_name
        """
    )

    if (
        not registry_df.empty
        and "canonical_server_name"
        in registry_df.columns
    ):
        servers = (
            registry_df["canonical_server_name"]
            .dropna()
            .astype(str)
            .str.strip()
            .str.upper()
            .drop_duplicates()
            .sort_values()
            .tolist()
        )

        if servers:
            return servers

    # Controlled fallback while the authoritative registry is empty.
    health_df = run_query(
        f"""
        SELECT DISTINCT
            canonical_server_name
        FROM {HEALTH_SUMMARY_VIEW}
        WHERE canonical_server_name IS NOT NULL
        ORDER BY canonical_server_name
        """
    )

    if (
        health_df.empty
        or "canonical_server_name"
        not in health_df.columns
    ):
        return []

    return (
        health_df["canonical_server_name"]
        .dropna()
        .astype(str)
        .str.strip()
        .str.upper()
        .drop_duplicates()
        .sort_values()
        .tolist()
    )


# -----------------------------------------------------------------------------
# Read subscriptions
# -----------------------------------------------------------------------------


def list_subscriptions(
    *,
    subscriber_email: str | None = None,
    server_name: str | None = None,
    active_only: bool = False,
) -> pd.DataFrame:
    """Return configured server-to-email routing records."""

    filters: list[str] = []

    if subscriber_email:
        email = _normalize_email(
            subscriber_email
        )
        filters.append(
            "subscriber_email = "
            f"'{_sql_quote(email)}'"
        )

    if server_name:
        server = _normalize_server(
            server_name
        )
        filters.append(
            "canonical_server_name = "
            f"'{_sql_quote(server)}'"
        )

    if active_only:
        filters.append("is_active = true")

    where_clause = ""

    if filters:
        where_clause = (
            "WHERE "
            + " AND ".join(filters)
        )

    return run_query(
        f"""
        SELECT
            subscription_id,
            subscriber_email,
            canonical_server_name,
            notification_destination_id,
            is_active,
            notes,
            created_ts,
            updated_ts
        FROM {ALERT_SUBSCRIPTIONS_TABLE}
        {where_clause}
        ORDER BY
            canonical_server_name,
            subscriber_email
        """
    )


# -----------------------------------------------------------------------------
# Add / reactivate subscriptions
# -----------------------------------------------------------------------------


def add_subscription(
    *,
    subscriber_email: str,
    server_names: Iterable[str],
    notes: str | None = None,
) -> pd.DataFrame:
    """
    Add or reactivate one subscriber for one or more servers.

    No email address or server is hardcoded. Both are supplied by the
    Streamlit UI.
    """

    email = _normalize_email(
        subscriber_email
    )

    servers = _normalize_servers(
        server_names
    )

    available_servers = set(
        load_available_servers()
    )

    invalid_servers = sorted(
        set(servers) - available_servers
    )

    if invalid_servers:
        raise ValueError(
            "The following servers are not "
            "available for subscription: "
            + ", ".join(invalid_servers)
        )

    notes_value = (
        str(notes).strip()
        if notes is not None
        else ""
    )

    values_sql: list[str] = []

    for server in servers:
        subscription_id = _subscription_id(
            email,
            server,
        )

        notes_sql = (
            f"'{_sql_quote(notes_value)}'"
            if notes_value
            else "CAST(NULL AS STRING)"
        )

        values_sql.append(
            "("
            f"'{subscription_id}', "
            f"'{_sql_quote(email)}', "
            f"'{_sql_quote(server)}', "
            "CAST(NULL AS STRING), "
            "true, "
            f"{notes_sql}, "
            "current_timestamp(), "
            "current_timestamp()"
            ")"
        )

    execute_sql(
        f"""
        MERGE INTO {ALERT_SUBSCRIPTIONS_TABLE}
        AS target

        USING (
            SELECT *
            FROM VALUES
                {", ".join(values_sql)}
            AS source(
                subscription_id,
                subscriber_email,
                canonical_server_name,
                notification_destination_id,
                is_active,
                notes,
                created_ts,
                updated_ts
            )
        ) AS source

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

        WHEN NOT MATCHED THEN INSERT (
            subscription_id,
            subscriber_email,
            canonical_server_name,
            notification_destination_id,
            is_active,
            notes,
            created_ts,
            updated_ts
        )
        VALUES (
            source.subscription_id,
            source.subscriber_email,
            source.canonical_server_name,
            source.notification_destination_id,
            source.is_active,
            source.notes,
            source.created_ts,
            source.updated_ts
        )
        """
    )

    clear_query_cache()

    return list_subscriptions(
        subscriber_email=email,
    )


# -----------------------------------------------------------------------------
# Disable subscriptions
# -----------------------------------------------------------------------------


def remove_subscription(
    *,
    subscriber_email: str,
    server_names: Iterable[str],
) -> pd.DataFrame:
    """
    Disable routing for one subscriber and one or more servers.

    Records are retained for auditability rather than physically deleted.
    """

    email = _normalize_email(
        subscriber_email
    )

    servers = _normalize_servers(
        server_names
    )

    server_list_sql = ", ".join(
        f"'{_sql_quote(server)}'"
        for server in servers
    )

    execute_sql(
        f"""
        UPDATE {ALERT_SUBSCRIPTIONS_TABLE}

        SET
            is_active = false,
            updated_ts = current_timestamp()

        WHERE
            subscriber_email =
                '{_sql_quote(email)}'

            AND canonical_server_name
                IN ({server_list_sql})
        """
    )

    clear_query_cache()

    return list_subscriptions(
        subscriber_email=email,
    )


# -----------------------------------------------------------------------------
# Convenience lookups for Streamlit
# -----------------------------------------------------------------------------


def load_server_subscribers(
    server_name: str,
) -> pd.DataFrame:
    """Return active subscribers for one selected server."""

    return list_subscriptions(
        server_name=server_name,
        active_only=True,
    )


def load_subscriber_servers(
    subscriber_email: str,
) -> pd.DataFrame:
    """Return active servers assigned to one subscriber."""

    return list_subscriptions(
        subscriber_email=subscriber_email,
        active_only=True,
    )