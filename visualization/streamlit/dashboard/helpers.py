"""Shared helpers for loading KPI data.

Strategy:
1. If the Delta table exists on disk (local dev), read it via deltalake.
2. Otherwise fall back to the bundled Parquet files shipped in data/.
   This is what Streamlit Cloud uses.
"""

import json
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

# ── Country flag emojis ──────────────────────────────────────────────────────
TEAM_FLAGS: dict[str, str] = {
    "Afghanistan":              "🇦🇫",
    "Australia":                "🇦🇺",
    "Bangladesh":               "🇧🇩",
    "Canada":                   "🇨🇦",
    "England":                  "🏴",
    "India":                    "🇮🇳",
    "Ireland":                  "🇮🇪",
    "Italy":                    "🇮🇹",
    "Namibia":                  "🇳🇦",
    "Nepal":                    "🇳🇵",
    "Netherlands":              "🇳🇱",
    "New Zealand":              "🇳🇿",
    "Oman":                     "🇴🇲",
    "Pakistan":                 "🇵🇰",
    "Scotland":                 "🏴",
    "South Africa":             "🇿🇦",
    "Sri Lanka":                "🇱🇰",
    "United Arab Emirates":     "🇦🇪",
    "United States of America": "🇺🇸",
    "West Indies":              "🏝️",
    "Zimbabwe":                 "🇿🇼",
}

# Short display names for teams (for tight UI)
TEAM_SHORT: dict[str, str] = {
    "United Arab Emirates":     "UAE",
    "United States of America": "USA",
    "South Africa":             "SA",
    "New Zealand":              "NZ",
    "Sri Lanka":                "SL",
    "West Indies":              "WI",
}


@st.cache_data(ttl=3600)
def _load_player_teams() -> dict[str, str]:
    """Load the player→team JSON mapping (built by build_player_map.py)."""
    path = os.path.join(_DATA_DIR, "player_teams.json")
    if os.path.isfile(path):
        return json.load(open(path))
    return {}


def player_team(name: str) -> str:
    """Return the team name for a player, or '' if unknown."""
    return _load_player_teams().get(name, "")


def flag(team: str) -> str:
    """Return the flag emoji for a team name."""
    return TEAM_FLAGS.get(team, "🏳️")


def player_flag(name: str) -> str:
    """Return the flag emoji for a player by name."""
    t = player_team(name)
    return flag(t) if t else ""


def flagged_name(name: str) -> str:
    """Return  'flag Name' — e.g. '🇮🇳 V Kohli'."""
    f = player_flag(name)
    return f"{f} {name}" if f else name


def team_short(team: str) -> str:
    """Return a short display name, e.g. 'South Africa' → 'SA'."""
    return TEAM_SHORT.get(team, team)


def enrich_player_df(df: pd.DataFrame, player_col: str = "batsman") -> pd.DataFrame:
    """Add 'team' and 'flag' columns to a DataFrame based on player name."""
    if player_col not in df.columns or df.empty:
        return df
    df = df.copy()
    mapping = _load_player_teams()
    df["team"] = df[player_col].map(mapping).fillna("")
    df["flag"] = df["team"].map(lambda t: TEAM_FLAGS.get(t, ""))
    # Create display column: "flag  Name"
    df["player_display"] = df.apply(
        lambda r: f"{r['flag']} {r[player_col]}" if r["flag"] else r[player_col],
        axis=1,
    )
    return df


def team_filter(df: pd.DataFrame, key: str, team_col: str = "team",
                label: str = "Filter by Team") -> pd.DataFrame:
    """Render a selectbox to filter a DataFrame by team.
    Returns the filtered DataFrame. 'All Teams' shows everything."""
    if team_col not in df.columns or df.empty:
        return df
    teams = sorted(df[team_col].dropna().unique())
    teams = [t for t in teams if t]  # drop blanks
    options = ["All Teams"] + teams
    sel = st.selectbox(label, options, key=key)
    if sel != "All Teams":
        df = df[df[team_col] == sel]
    return df


# ─── Existing helpers ─────────────────────────────────────────────────────────

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


# ─── Chart type selector & universal renderer ────────────────────────────────

CHART_TYPES = ["Bar", "Horizontal Bar", "Pie", "Donut", "Treemap"]


def chart_type_selector(key: str, default: str = "Bar",
                        label: str = "📊 Chart Type") -> str:
    """Render a chart-type selectbox and return the chosen type."""
    return st.selectbox(label, CHART_TYPES, index=CHART_TYPES.index(default),
                        key=key)


def render_chart(df: pd.DataFrame, name_col: str, value_col: str,
                 chart_type: str, color_scale: str = "Blues",
                 axis_label: str = "", height: int = 420,
                 text_col: str | None = None, **bar_kwargs) -> None:
    """Render a chart in the selected format (Bar/HBar/Pie/Donut/Treemap).

    Parameters
    ----------
    df          : DataFrame with at least *name_col* and *value_col*.
    name_col    : Column used for labels / category axis.
    value_col   : Column used for values / measure axis.
    chart_type  : One of CHART_TYPES.
    color_scale : Plotly sequential color scale name (for bar/treemap).
    axis_label  : Label for the value axis.
    height      : Chart height in pixels.
    text_col    : Column to show as text on bars (defaults to *value_col*).
    **bar_kwargs: Extra kwargs forwarded to px.bar (e.g. barmode, color).
    """
    import plotly.express as px

    txt = text_col or value_col

    if chart_type == "Bar":
        fig = px.bar(df, x=name_col, y=value_col, text=txt,
                     color=bar_kwargs.pop("color", value_col),
                     color_continuous_scale=color_scale, **bar_kwargs)
        fig.update_layout(xaxis_title="", yaxis_title=axis_label,
                          coloraxis_showscale=False, height=height,
                          xaxis_tickangle=-45)

    elif chart_type == "Horizontal Bar":
        df_sorted = df.sort_values(value_col, ascending=True)
        fig = px.bar(df_sorted, x=value_col, y=name_col, orientation="h",
                     text=txt, color=bar_kwargs.pop("color", value_col),
                     color_continuous_scale=color_scale, **bar_kwargs)
        fig.update_layout(yaxis_title="", xaxis_title=axis_label,
                          coloraxis_showscale=False, height=height)

    elif chart_type in ("Pie", "Donut"):
        fig = px.pie(df, values=value_col, names=name_col)
        if chart_type == "Donut":
            fig.update_traces(hole=0.4)
        fig.update_layout(height=height, showlegend=True,
                          legend=dict(font=dict(size=10)))

    elif chart_type == "Treemap":
        fig = px.treemap(df, path=[name_col], values=value_col,
                         color=value_col, color_continuous_scale=color_scale)
        fig.update_layout(height=height, coloraxis_showscale=False)

    else:
        fig = px.bar(df, x=name_col, y=value_col, text=txt)
        fig.update_layout(xaxis_title="", yaxis_title=axis_label, height=height)

    st.plotly_chart(fig, use_container_width=True)

