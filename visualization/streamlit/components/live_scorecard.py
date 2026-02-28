"""
Live Scorecard Component
Displays current match score, run rate, required rate, and over-by-over runs chart.
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from deltalake import DeltaTable
import os


def render_live_scorecard(delta_base: str, match_id: str = "") -> None:
    """
    Render the live scorecard tab showing current match state.

    Args:
        delta_base:  Base path for Delta Lake tables
        match_id:    Optional match ID to filter data
    """
    st.header("📊 Live Scorecard")

    live_kpis_path = os.path.join(delta_base, "gold", "live_kpis")

    try:
        dt = DeltaTable(live_kpis_path)
        df = dt.to_pandas()

        if match_id:
            df = df[df["match_id"] == match_id]

        if df.empty:
            st.info("No live match data available. Start the streaming pipeline to see live data.")
            return

        # Sort by most recent window
        df = df.sort_values("computed_at", ascending=False)
        latest = df.iloc[0]

        # ─── Key metrics row ──────────────────────────────────────────────────
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric("Match ID", latest.get("match_id", "N/A"))

        with col2:
            run_rate = latest.get("run_rate", 0.0)
            st.metric("Current Run Rate", f"{run_rate:.2f}")

        with col3:
            wickets = latest.get("wickets_in_window", 0)
            st.metric("Wickets (window)", int(wickets))

        with col4:
            economy = latest.get("bowler_economy", 0.0)
            st.metric("Bowler Economy", f"{economy:.2f}")

        st.markdown("---")

        # ─── Current batsmen/bowler ───────────────────────────────────────────
        col5, col6 = st.columns(2)
        with col5:
            st.subheader("🏏 Current Batsman")
            st.write(latest.get("current_batsman", "N/A"))
            strike_rate = latest.get("batting_strike_rate", 0.0)
            st.caption(f"Strike Rate: {strike_rate:.1f}")

        with col6:
            st.subheader("🎳 Current Bowler")
            st.write(latest.get("current_bowler", "N/A"))

        # ─── Over-by-over run rate chart ─────────────────────────────────────
        st.subheader("📈 Run Rate Over Time")
        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=df["window_start"],
            y=df["run_rate"],
            name="Run Rate",
            marker_color="steelblue"
        ))
        fig.update_layout(
            xaxis_title="Window Start",
            yaxis_title="Run Rate (per over)",
            height=350
        )
        st.plotly_chart(fig, use_container_width=True)

    except Exception as e:
        st.error(f"Could not load live scorecard data: {e}")
        st.info("Make sure the streaming pipeline is running and Delta tables exist.")
