"""Streamlit UI for server-specific Databricks alert subscriptions."""

from __future__ import annotations

import pandas as pd
import streamlit as st

from db.connection import DatabricksQueryError
from services.agent_subscription_service import (
    add_subscription,
    list_subscriptions,
    load_available_servers,
    remove_subscription,
)


def render_alert_subscriptions_view() -> None:
    """Render subscriber-to-server notification routing controls."""

    st.subheader("Notification subscriptions")

    st.caption(
        "Assign an email address to one or more SQL Servers. "
        "Subscribers receive only notifications for the servers "
        "assigned to them."
    )

    # -------------------------------------------------------------------------
    # 1. Available servers
    # -------------------------------------------------------------------------

    try:
        available_servers = load_available_servers()
    except Exception as exc:
        st.error(
            f"Could not load the available server list: {exc}"
        )
        return

    if not available_servers:
        st.warning(
            "No servers are currently available for subscription."
        )
        return

    # -------------------------------------------------------------------------
    # 2. Add / update subscription
    # -------------------------------------------------------------------------

    st.markdown("#### Add subscriber")

    with st.form(
        "add_alert_subscription_form",
        clear_on_submit=True,
    ):
        subscriber_email = st.text_input(
            "Subscriber email",
            placeholder="name@example.com",
        )

        selected_servers = st.multiselect(
            "Server(s)",
            options=available_servers,
            help=(
                "Select every server for which this person "
                "should receive DBA priority notifications."
            ),
        )

        notes = st.text_input(
            "Notes",
            placeholder="Optional responsibility or team note",
        )

        add_clicked = st.form_submit_button(
            "Add subscription",
            type="primary",
        )

    if add_clicked:
        try:
            add_subscription(
                subscriber_email=subscriber_email,
                server_names=selected_servers,
                notes=notes,
            )

            st.success(
                "Subscription routing updated successfully."
            )

            st.rerun()

        except ValueError as exc:
            st.warning(str(exc))

        except DatabricksQueryError as exc:
            st.error(
                f"Databricks could not update the subscription: {exc}"
            )

        except Exception as exc:
            st.error(
                f"Subscription update failed: {exc}"
            )

    # -------------------------------------------------------------------------
    # 3. Current active subscriptions
    # -------------------------------------------------------------------------

    st.markdown("#### Current subscriptions")

    try:
        subscriptions_df = list_subscriptions(
            active_only=True
        )
    except Exception as exc:
        st.error(
            f"Could not load current subscriptions: {exc}"
        )
        return

    if subscriptions_df.empty:
        st.info(
            "No active notification subscriptions are configured yet."
        )
        return

    display_columns = [
        column
        for column in [
            "subscriber_email",
            "canonical_server_name",
            "notes",
            "updated_ts",
        ]
        if column in subscriptions_df.columns
    ]

    st.dataframe(
        subscriptions_df[display_columns],
        hide_index=True,
        use_container_width=True,
    )

    # -------------------------------------------------------------------------
    # 4. Remove / disable subscription routing
    # -------------------------------------------------------------------------

    st.markdown("#### Remove subscription")

    subscriber_options = sorted(
        subscriptions_df["subscriber_email"]
        .dropna()
        .astype(str)
        .unique()
        .tolist()
    )

    subscriber_to_remove = st.selectbox(
        "Subscriber",
        options=subscriber_options,
        key="remove_subscription_email",
    )

    assigned_servers_df = subscriptions_df[
        subscriptions_df["subscriber_email"]
        == subscriber_to_remove
    ]

    assigned_servers = sorted(
        assigned_servers_df[
            "canonical_server_name"
        ]
        .dropna()
        .astype(str)
        .unique()
        .tolist()
    )

    with st.form(
        "remove_alert_subscription_form"
    ):
        servers_to_remove = st.multiselect(
            "Server(s) to remove",
            options=assigned_servers,
        )

        remove_clicked = st.form_submit_button(
            "Remove selected subscription(s)"
        )

    if remove_clicked:
        if not servers_to_remove:
            st.warning(
                "Select at least one server to remove."
            )

        else:
            try:
                remove_subscription(
                    subscriber_email=subscriber_to_remove,
                    server_names=servers_to_remove,
                )

                st.success(
                    "Selected subscription routing removed."
                )

                st.rerun()

            except ValueError as exc:
                st.warning(str(exc))

            except DatabricksQueryError as exc:
                st.error(
                    f"Databricks could not remove the "
                    f"subscription: {exc}"
                )

            except Exception as exc:
                st.error(
                    f"Subscription removal failed: {exc}"
                )