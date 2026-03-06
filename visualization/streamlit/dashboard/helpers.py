"""Shared helpers for loading KPI data.

Strategy:
1. If the Delta table exists on disk (local dev), read it via deltalake.
2. Otherwise fall back to the bundled Parquet files shipped in data/.
   This is what Streamlit Cloud uses.
"""

import os
import pandas as pd
import streamlit as st

# Try importing deltalake — it may not be installed on Cloud
try:
    from deltalake import DeltaTable
except ImportError:
    DeltaTable = None

# Path to bundled Parquet files (relative to this file → ../data/)
_DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")


def kpi_path(delta_base: str, kpi_name: str) -> str:
    """Return the absolute path for a batch-KPI Delta table."""
    return os.path.join(delta_base, "gold", "batch_kpis", kpi_name)


@st.cache_data(ttl=300, show_spinner="Loading data …")
def load_kpi(delta_base: str, kpi_name: str) -> pd.DataFrame:
    """Load a Gold batch-KPI into a Pandas DataFrame.

    • Local dev  → reads the Delta table from *delta_base*.
    • Cloud      → reads the bundled ``data/<kpi>.parquet`` file.

    Automatically coerces object columns that look numeric
    (Delta / Parquet sometimes yields Decimal or object types).

    Returns an empty DataFrame on any read error.
    """
    df = pd.DataFrame()

    # 1. Try Delta table (local)
    delta_dir = kpi_path(delta_base, kpi_name)
    if DeltaTable is not None and os.path.isdir(delta_dir):
        try:
            df = DeltaTable(delta_dir).to_pandas()
        except Exception:
            pass

    # 2. Fallback: bundled Parquet
    if df.empty:
        parquet_file = os.path.join(_DATA_DIR, f"{kpi_name}.parquet")
        if os.path.isfile(parquet_file):
            try:
                df = pd.read_parquet(parquet_file)
            except Exception:
                pass

    # Coerce object columns that look numeric
    for col in df.columns:
        if df[col].dtype == object:
            converted = pd.to_numeric(df[col], errors="coerce")
            if converted.notna().sum() > 0.5 * len(df):
                df[col] = converted

    return df


def no_data_warning(kpi_name: str) -> None:
    st.info(
        f"No data available for **{kpi_name}**. "
        "Run the batch pipeline first:\n\n"
        "```bash\nbash scripts/run_batch.sh /path/to/cricsheet\n```"
    )
