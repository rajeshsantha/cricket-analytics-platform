"""Shared helpers for loading Delta tables."""

import os
import pandas as pd
import streamlit as st
from deltalake import DeltaTable


def kpi_path(delta_base: str, kpi_name: str) -> str:
    """Return the absolute path for a batch-KPI Delta table."""
    return os.path.join(delta_base, "gold", "batch_kpis", kpi_name)


@st.cache_data(ttl=300, show_spinner="Loading data …")
def load_kpi(delta_base: str, kpi_name: str) -> pd.DataFrame:
    """Load a Gold batch-KPI Delta table into a Pandas DataFrame.

    Returns an empty DataFrame (with no columns) on any read error so that
    callers can test ``df.empty`` and show a friendly message.
    """
    path = kpi_path(delta_base, kpi_name)
    try:
        return DeltaTable(path).to_pandas()
    except Exception:
        return pd.DataFrame()


def no_data_warning(kpi_name: str) -> None:
    st.info(
        f"No data available for **{kpi_name}**. "
        "Run the batch pipeline first:\n\n"
        "```bash\nbash scripts/run_batch.sh /path/to/cricsheet\n```"
    )

