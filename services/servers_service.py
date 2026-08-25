from __future__ import annotations

import pandas as pd
import streamlit as st

from db.connection import run_query
from db.observability_sources import FILES_TABLE


@st.cache_data(ttl=300, show_spinner=False)
def load_servers() -> pd.DataFrame:
    query = f"""
    SELECT DISTINCT server_name
    FROM {FILES_TABLE}
    WHERE server_name IS NOT NULL
    ORDER BY server_name
    """
    return run_query(query)


@st.cache_data(ttl=300, show_spinner=False)
def get_ingestion_dates(server_name: str) -> list[str]:
    safe_server = str(server_name).replace("'", "''")
    query = f"""
    SELECT DISTINCT CAST(ingestion_date AS STRING) AS ingestion_date
    FROM {FILES_TABLE}
    WHERE server_name = '{safe_server}'
      AND ingestion_date IS NOT NULL
    ORDER BY ingestion_date DESC
    """
    df = run_query(query)
    if df.empty or "ingestion_date" not in df.columns:
        return []
    return [str(value) for value in df["ingestion_date"].tolist() if value is not None]
