"""Bowling page — KPIs 2, 4, 6, 11, 20, 29."""

import streamlit as st
import plotly.express as px
from .helpers import (load_kpi, no_data_warning, enrich_player_df, team_filter,
                      chart_type_selector, render_chart)


def render(delta_base: str) -> None:
    st.title("🎳 Bowling Analytics")

    tabs = st.tabs([
        "Top Wicket Takers", "Bowling Average", "Economy Rate",
        "Death-Over Economy", "Dot Ball %", "Best Spells",
    ])

    # ── Tab 0: Top Wicket Takers (KPI 2) ─────────────────────────────────────
    with tabs[0]:
        st.subheader("Top 10 Wicket Takers")
        df = load_kpi(delta_base, "top_wicket_takers")
        if df.empty:
            no_data_warning("top_wicket_takers"); return
        df = enrich_player_df(df, "bowler")
        df = team_filter(df, key="bowl_top_team")
        df = df.sort_values("wickets", ascending=False).reset_index(drop=True)
        df.index += 1
        c1, c2 = st.columns([1, 2])
        with c1:
            st.dataframe(df[["player_display", "team", "wickets", "balls_bowled", "matches"]].rename(
                columns={"player_display": "Bowler"}),
                use_container_width=True, height=400)
        with c2:
            ct = chart_type_selector(key="bowl_top_chart", default="Bar")
            render_chart(df, "player_display", "wickets", ct,
                         "Blues", "Wickets", height=400)

    # ── Tab 1: Best Bowling Average (KPI 4) ──────────────────────────────────
    with tabs[1]:
        st.subheader("Best Bowling Average (min 3 wickets)")
        df = load_kpi(delta_base, "best_bowling_average")
        if df.empty:
            no_data_warning("best_bowling_average"); return
        df = enrich_player_df(df, "bowler")
        df = team_filter(df, key="bowl_avg_team")
        df = df.sort_values("bowling_average", ascending=True).head(20)
        ct = chart_type_selector(key="bowl_avg_chart", default="Bar")
        render_chart(df, "player_display", "bowling_average", ct,
                     "Teal", "Bowling Average", height=420)
        with st.expander("View data"):
            st.dataframe(df[["player_display", "team", "bowling_average", "wickets", "runs_conceded"]].rename(
                columns={"player_display": "Bowler"}), use_container_width=True)

    # ── Tab 2: Best Economy Rate (KPI 6) ─────────────────────────────────────
    with tabs[2]:
        st.subheader("Best Economy Rate (min 5 overs)")
        df = load_kpi(delta_base, "best_economy_rate")
        if df.empty:
            no_data_warning("best_economy_rate"); return
        df = enrich_player_df(df, "bowler")
        df = team_filter(df, key="bowl_eco_team")
        df = df.sort_values("economy_rate", ascending=True).head(20)
        ct = chart_type_selector(key="bowl_eco_chart", default="Bar")
        render_chart(df, "player_display", "economy_rate", ct,
                     "Mint", "Economy Rate", height=420)
        with st.expander("View data"):
            st.dataframe(df[["player_display", "team", "economy_rate", "overs_bowled", "runs_conceded"]].rename(
                columns={"player_display": "Bowler"}), use_container_width=True)

    # ── Tab 3: Death-Over Economy (KPI 11) ───────────────────────────────────
    with tabs[3]:
        st.subheader("Death-Over Economy (overs 16–20)")
        df = load_kpi(delta_base, "death_over_economy")
        if df.empty:
            no_data_warning("death_over_economy"); return
        df = enrich_player_df(df, "bowler")
        df = team_filter(df, key="bowl_death_team")
        df = df.sort_values("death_economy", ascending=True).head(20)
        ct = chart_type_selector(key="bowl_death_chart", default="Bar")
        render_chart(df, "player_display", "death_economy", ct,
                     "Burg", "Death Economy", height=420)

    # ── Tab 4: Dot Ball Percentage (KPI 20) ──────────────────────────────────
    with tabs[4]:
        st.subheader("Dot Ball Percentage (min 10 balls)")
        df = load_kpi(delta_base, "dot_ball_pct")
        if df.empty:
            no_data_warning("dot_ball_pct"); return
        df = enrich_player_df(df, "bowler")
        df = team_filter(df, key="bowl_dot_team")
        df = df.sort_values("dot_ball_pct", ascending=False).head(20)
        ct = chart_type_selector(key="bowl_dot_chart", default="Bar")
        render_chart(df, "player_display", "dot_ball_pct", ct,
                     "Purples", "Dot Ball %", height=420)

    # ── Tab 5: Best Bowling Spells (KPI 29) ──────────────────────────────────
    with tabs[5]:
        st.subheader("Best Bowling Spells (3+ wickets in 5-over window)")
        df = load_kpi(delta_base, "best_bowling_spells")
        if df.empty:
            no_data_warning("best_bowling_spells"); return
        df = enrich_player_df(df, "bowler")
        df = team_filter(df, key="bowl_spell_team")
        df = df.sort_values(["wickets", "runs_conceded"],
                            ascending=[False, True]).head(20).reset_index(drop=True)
        df.index += 1
        st.dataframe(
            df[["player_display", "team", "wickets", "runs_conceded", "balls",
                "spell_start_over", "spell_end_over", "match_type", "match_id"]].rename(
                columns={"player_display": "Bowler"}),
            use_container_width=True, height=500,
        )

