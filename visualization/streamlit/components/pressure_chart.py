"""
Pressure Chart Component
Displays a line chart of the pressure index over time, color-coded by pressure level.
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from deltalake import DeltaTable
import os


def render_pressure_chart(delta_base: str, match_id: str = "") -> None:
    """
    Render the Pressure Chart tab showing over-by-over pressure index.
    Color-coded by pressure level: Low (green), Medium (orange), High (red).

    Args:
        delta_base:  Base path for Delta Lake tables
        match_id:    Optional match ID to filter data
    """
    st.header("📈 Pressure Index Analysis")
    st.caption(
        "Pressure Index = f(required RR, current RR, wickets lost, balls remaining). "
        "Higher values indicate greater match pressure."
    )

    pressure_path = os.path.join(delta_base, "gold", "batch_kpis", "pressure_index_per_over")

    try:
        df = DeltaTable(pressure_path).to_pandas()

        if match_id:
            df = df[df["match_id"] == match_id]

        if df.empty:
            st.info("No pressure index data available. Run the batch pipeline first.")
            return

        # ─── Match selector ───────────────────────────────────────────────────
        if not match_id:
            available_matches = df["match_id"].unique().tolist()
            selected_match = st.selectbox("Select Match", options=available_matches[:50])
            df = df[df["match_id"] == selected_match]

        # ─── Color mapping ────────────────────────────────────────────────────
        color_map = {"Low": "#2ecc71", "Medium": "#f39c12", "High": "#e74c3c"}

        df = df.sort_values(["inning", "over_num"])

        # ─── Pressure line chart ──────────────────────────────────────────────
        fig = go.Figure()

        for inning in df["inning"].unique():
            inning_df = df[df["inning"] == inning]

            # Draw the pressure line
            fig.add_trace(go.Scatter(
                x=inning_df["over_num"],
                y=inning_df["pressure_index"],
                mode="lines+markers",
                name=str(inning),
                marker=dict(
                    color=[color_map.get(lvl, "gray") for lvl in inning_df["pressure_level"]],
                    size=10
                ),
                line=dict(width=2)
            ))

        # Add threshold lines
        fig.add_hline(y=1.0, line_dash="dot", line_color="orange",
                      annotation_text="Medium pressure threshold")
        fig.add_hline(y=2.0, line_dash="dot", line_color="red",
                      annotation_text="High pressure threshold")

        fig.update_layout(
            title="Pressure Index by Over",
            xaxis_title="Over Number",
            yaxis_title="Pressure Index",
            height=450,
            legend_title="Innings"
        )
        st.plotly_chart(fig, use_container_width=True)

        # ─── Legend ───────────────────────────────────────────────────────────
        col1, col2, col3 = st.columns(3)
        with col1:
            st.success("🟢 Low Pressure (< 1.0)")
        with col2:
            st.warning("🟡 Medium Pressure (1.0 - 2.0)")
        with col3:
            st.error("🔴 High Pressure (> 2.0)")

        # ─── Data table ───────────────────────────────────────────────────────
        with st.expander("View raw pressure data"):
            st.dataframe(df[["inning", "over_num", "runs_in_over",
                              "wickets_in_over", "pressure_index", "pressure_level"]])

    except Exception as e:
        st.error(f"Could not load pressure index data: {e}")
        st.info("Run the batch pipeline first: `bash scripts/run_batch.sh`")
