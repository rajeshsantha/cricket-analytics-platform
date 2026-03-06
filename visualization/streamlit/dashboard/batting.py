"""Batting page — KPIs 1, 3, 5, 7, 8, 9, 18, 19, 28, 30."""

import streamlit as st
import pandas as pd
import plotly.express as px
from .helpers import (load_kpi, no_data_warning, enrich_player_df, team_filter,
                      flagged_name, chart_type_selector, render_chart)


def render(delta_base: str) -> None:
    st.title("🏏 Batting Analytics")

    tabs = st.tabs([
        "Top Scorers", "Batting Avg", "Strike Rate",
        "Highest Scores", "Sixes & Fours", "By Match Type",
        "Partnerships", "Consistency", "Win Contribution",
    ])

    # ── Tab 0: Top Run Scorers (KPI 1) ───────────────────────────────────────
    with tabs[0]:
        st.subheader("Top Run Scorers")
        df = load_kpi(delta_base, "top_run_scorers")
        if df.empty:
            no_data_warning("top_run_scorers"); return
        df = enrich_player_df(df, "batsman")
        df = team_filter(df, key="bat_top_team")
        df = df.sort_values("total_runs", ascending=False).reset_index(drop=True)
        df.index += 1

        # Ensure numeric columns
        for c in ["total_runs", "balls_faced", "matches", "innings", "not_outs",
                   "batting_average", "strike_rate"]:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")

        # Build display columns
        display_cols = ["player_display", "team", "matches"]
        rename_map = {"player_display": "Player"}
        if "innings" in df.columns:
            display_cols.append("innings")
            rename_map["innings"] = "Innings"
        if "batting_average" in df.columns:
            display_cols.append("batting_average")
            rename_map["batting_average"] = "Bat Avg"
        display_cols.append("total_runs")
        rename_map["total_runs"] = "Runs"
        if "not_outs" in df.columns:
            display_cols.append("not_outs")
            rename_map["not_outs"] = "Not Outs"
        if "strike_rate" in df.columns:
            display_cols.append("strike_rate")
            rename_map["strike_rate"] = "Strike Rate"
        if "highest_score" in df.columns:
            display_cols.append("highest_score")
            rename_map["highest_score"] = "Highest Score"

        col1, col2 = st.columns([3, 2])
        with col1:
            st.dataframe(
                df[display_cols].rename(columns=rename_map),
                use_container_width=True,
                height=min(600, 40 + 35 * len(df)),
            )
        with col2:
            ct = chart_type_selector(key="bat_top_chart", default="Bar")
            top_chart = df.head(15)
            render_chart(top_chart, "player_display", "total_runs", ct,
                         "YlOrRd", "Runs", height=400)

    # ── Tab 1: Best Batting Average (KPI 3) ──────────────────────────────────
    with tabs[1]:
        st.subheader("Best Batting Average (min 3 innings)")
        df = load_kpi(delta_base, "best_batting_average")
        if df.empty:
            no_data_warning("best_batting_average"); return
        df = enrich_player_df(df, "batsman")
        df = team_filter(df, key="bat_avg_team")
        df = df.sort_values("batting_average", ascending=False).head(20)
        ct = chart_type_selector(key="bat_avg_chart", default="Bar")
        render_chart(df, "player_display", "batting_average", ct,
                     "Greens", "Average", height=420)
        with st.expander("View data"):
            st.dataframe(df[["player_display", "team", "batting_average", "total_runs", "innings", "dismissals"]].rename(
                columns={"player_display": "Player"}), use_container_width=True)

    # ── Tab 2: Best Strike Rate (KPI 5) ──────────────────────────────────────
    with tabs[2]:
        st.subheader("Best Strike Rate (min 30 balls)")
        df = load_kpi(delta_base, "best_strike_rate")
        if df.empty:
            no_data_warning("best_strike_rate"); return
        df = enrich_player_df(df, "batsman")
        df = team_filter(df, key="bat_sr_team")
        df = df.sort_values("strike_rate", ascending=False).head(20)
        ct = chart_type_selector(key="bat_sr_chart", default="Bar")
        render_chart(df, "player_display", "strike_rate", ct,
                     "Reds", "Strike Rate", height=420)
        with st.expander("View data"):
            st.dataframe(df[["player_display", "team", "strike_rate", "total_runs", "balls_faced"]].rename(
                columns={"player_display": "Player"}), use_container_width=True)

    # ── Tab 3: Highest Individual Scores (KPI 7) ─────────────────────────────
    with tabs[3]:
        st.subheader("Highest Individual Scores")
        df = load_kpi(delta_base, "highest_individual_scores")
        if df.empty:
            no_data_warning("highest_individual_scores"); return
        df = enrich_player_df(df, "batsman")
        df = team_filter(df, key="bat_hi_team")
        df = df.sort_values("score", ascending=False).head(20).reset_index(drop=True)
        df.index += 1
        st.dataframe(
            df[["player_display", "team", "score", "balls_faced", "match_type", "match_id"]].rename(
                columns={"player_display": "Player"}),
            use_container_width=True, height=500,
        )

    # ── Tab 4: Most Sixes & Fours (KPIs 8, 9) ───────────────────────────────
    with tabs[4]:
        ct = chart_type_selector(key="bat_sf_chart", default="Horizontal Bar")
        c1, c2 = st.columns(2)
        with c1:
            st.subheader("💥 Most Sixes")
            df = load_kpi(delta_base, "most_sixes")
            if df.empty:
                no_data_warning("most_sixes")
            else:
                df = enrich_player_df(df, "batsman")
                df = df.sort_values("sixes", ascending=False).head(15)
                render_chart(df, "player_display", "sixes", ct,
                             "Purples", "Sixes", height=450)
        with c2:
            st.subheader("🏓 Most Fours")
            df = load_kpi(delta_base, "most_fours")
            if df.empty:
                no_data_warning("most_fours")
            else:
                df = enrich_player_df(df, "batsman")
                df = df.sort_values("fours", ascending=False).head(15)
                render_chart(df, "player_display", "fours", ct,
                             "Oranges", "Fours", height=450)

    # ── Tab 5: Player by Match Type (KPI 18) ─────────────────────────────────
    with tabs[5]:
        st.subheader("Player Performance by Match Type")
        df = load_kpi(delta_base, "player_by_match_type")
        if df.empty:
            no_data_warning("player_by_match_type"); return
        df = enrich_player_df(df, "batsman")
        mt = st.selectbox("Match type", df["match_type"].unique(), key="bat_mt")
        subset = df[df["match_type"] == mt].copy()
        subset = team_filter(subset, key="bat_mt_team")
        subset = subset.sort_values("total_runs", ascending=False).head(15)
        for c in ["balls_faced", "total_runs", "strike_rate"]:
            subset[c] = pd.to_numeric(subset[c], errors="coerce")
        subset = subset.dropna(subset=["strike_rate"])
        fig = px.scatter(subset, x="balls_faced", y="total_runs", size="strike_rate",
                         color="team", hover_name="player_display",
                         labels={"balls_faced": "Balls Faced", "total_runs": "Total Runs"})
        fig.update_layout(height=500)
        st.plotly_chart(fig, use_container_width=True)
        with st.expander("View data"):
            st.dataframe(subset[["player_display", "team", "total_runs", "balls_faced", "strike_rate"]].rename(
                columns={"player_display": "Player"}), use_container_width=True)

    # ── Tab 6: Top Partnerships (KPI 19) ─────────────────────────────────────
    with tabs[6]:
        st.subheader("Top Partnerships")
        df = load_kpi(delta_base, "partnership_analysis")
        if df.empty:
            no_data_warning("partnership_analysis"); return
        df = enrich_player_df(df, "batsman")
        df = df.sort_values("partnership_runs", ascending=False).head(20).reset_index(drop=True)
        df.index += 1
        df["pair"] = df["player_display"] + " & " + df["non_striker"].map(flagged_name)
        ct = chart_type_selector(key="bat_part_chart", default="Bar")
        render_chart(df, "pair", "partnership_runs", ct,
                     "Sunset", "Runs", height=420)

    # ── Tab 7: Player Consistency (KPI 28) ───────────────────────────────────
    with tabs[7]:
        st.subheader("Most Consistent Batsmen (lowest score std-dev)")
        df = load_kpi(delta_base, "player_consistency")
        if df.empty:
            no_data_warning("player_consistency"); return
        df = enrich_player_df(df, "batsman")
        df = team_filter(df, key="bat_cons_team")
        df = df.sort_values("score_stddev", ascending=True).head(20)
        fig = px.scatter(df, x="avg_score", y="score_stddev", hover_name="player_display",
                         size="innings", color="team",
                         labels={"avg_score": "Avg Score", "score_stddev": "Std Dev"})
        fig.update_layout(height=480)
        st.plotly_chart(fig, use_container_width=True)
        with st.expander("View data"):
            st.dataframe(df[["player_display", "team", "avg_score", "score_stddev", "innings"]].rename(
                columns={"player_display": "Player"}), use_container_width=True)

    # ── Tab 8: Win Contribution (KPI 30) ─────────────────────────────────────
    with tabs[8]:
        st.subheader("Match-Winning Contribution Index")
        df = load_kpi(delta_base, "win_contribution")
        if df.empty:
            no_data_warning("win_contribution"); return
        df = enrich_player_df(df, "batsman")
        df = team_filter(df, key="bat_wc_team")
        df = df.sort_values("win_contribution_pct", ascending=False).head(20)
        fig = px.bar(df, x="player_display", y="win_contribution_pct",
                     text="win_contribution_pct", color="team",
                     barmode="group")
        fig.update_layout(xaxis_title="", yaxis_title="Win Contribution %",
                          height=420, xaxis_tickangle=-45)
        st.plotly_chart(fig, use_container_width=True)
        with st.expander("View data"):
            st.dataframe(df[["player_display", "team", "win_contribution_pct", "winning_runs", "total_runs", "matches"]].rename(
                columns={"player_display": "Player"}), use_container_width=True)
