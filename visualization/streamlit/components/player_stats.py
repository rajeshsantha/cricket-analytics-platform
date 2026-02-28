"""
Player Stats Component
Displays top batsmen and bowlers from Gold Delta batch KPI tables.
Provides a sortable, filterable table view.
"""

import streamlit as st
import pandas as pd
from deltalake import DeltaTable
import os


def render_player_stats(delta_base: str) -> None:
    """
    Render the Player Stats tab showing batting and bowling leaderboards.

    Args:
        delta_base:  Base path for Delta Lake tables
    """
    st.header("👤 Player Statistics")

    batch_kpis_base = os.path.join(delta_base, "gold", "batch_kpis")

    col1, col2 = st.columns(2)

    # ─── Top Batsmen ──────────────────────────────────────────────────────────
    with col1:
        st.subheader("🏏 Top Run Scorers")
        run_scorers_path = os.path.join(batch_kpis_base, "top_run_scorers")
        try:
            df = DeltaTable(run_scorers_path).to_pandas()
            df = df.sort_values("total_runs", ascending=False).reset_index(drop=True)
            df.index += 1  # 1-based rank

            sort_col = st.selectbox(
                "Sort batsmen by",
                options=["total_runs", "balls_faced", "matches"],
                key="batsmen_sort"
            )
            df = df.sort_values(sort_col, ascending=False)

            st.dataframe(
                df[["batsman", "total_runs", "balls_faced", "matches"]],
                use_container_width=True,
                height=400
            )
        except Exception as e:
            st.warning(f"Batsmen data not available: {e}")
            st.info("Run the batch pipeline first: `bash scripts/run_batch.sh`")

    # ─── Top Bowlers ──────────────────────────────────────────────────────────
    with col2:
        st.subheader("🎳 Top Wicket Takers")
        wicket_takers_path = os.path.join(batch_kpis_base, "top_wicket_takers")
        try:
            df = DeltaTable(wicket_takers_path).to_pandas()
            df = df.sort_values("wickets", ascending=False).reset_index(drop=True)
            df.index += 1

            sort_col = st.selectbox(
                "Sort bowlers by",
                options=["wickets", "balls_bowled", "matches"],
                key="bowlers_sort"
            )
            df = df.sort_values(sort_col, ascending=False)

            st.dataframe(
                df[["bowler", "wickets", "balls_bowled", "matches"]],
                use_container_width=True,
                height=400
            )
        except Exception as e:
            st.warning(f"Bowler data not available: {e}")

    # ─── Batting statistics ───────────────────────────────────────────────────
    st.markdown("---")
    st.subheader("📊 Best Batting Averages")
    avg_path = os.path.join(batch_kpis_base, "best_batting_average")
    try:
        df = DeltaTable(avg_path).to_pandas()
        st.dataframe(df.head(20), use_container_width=True)
    except Exception as e:
        st.info(f"Batting average data not yet available: {e}")
