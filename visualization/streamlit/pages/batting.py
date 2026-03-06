"""Batting page — KPIs 1, 3, 5, 7, 8, 9, 18, 19, 28, 30."""

import streamlit as st
import plotly.express as px
from .helpers import load_kpi, no_data_warning


def render(delta_base: str) -> None:
    st.title("🏏 Batting Analytics")

    tabs = st.tabs([
        "Top Scorers", "Batting Avg", "Strike Rate",
        "Highest Scores", "Sixes & Fours", "By Match Type",
        "Partnerships", "Consistency", "Win Contribution",
    ])

    # ── Tab 0: Top Run Scorers (KPI 1) ───────────────────────────────────────
    with tabs[0]:
        st.subheader("Top 10 Run Scorers — All Time")
        df = load_kpi(delta_base, "top_run_scorers")
        if df.empty:
            no_data_warning("top_run_scorers"); return
        df = df.sort_values("total_runs", ascending=False).reset_index(drop=True)
        df.index += 1

        col1, col2 = st.columns([1, 2])
        with col1:
            st.dataframe(df[["batsman", "total_runs", "balls_faced", "matches"]],
                         use_container_width=True, height=400)
        with col2:
            fig = px.bar(df, x="batsman", y="total_runs", text="total_runs",
                         color="total_runs", color_continuous_scale="YlOrRd")
            fig.update_layout(xaxis_title="", yaxis_title="Runs",
                              coloraxis_showscale=False, height=400)
            st.plotly_chart(fig, use_container_width=True)

    # ── Tab 1: Best Batting Average (KPI 3) ──────────────────────────────────
    with tabs[1]:
        st.subheader("Best Batting Average (min 20 innings)")
        df = load_kpi(delta_base, "best_batting_average")
        if df.empty:
            no_data_warning("best_batting_average"); return
        df = df.sort_values("batting_average", ascending=False).head(20)
        fig = px.bar(df, x="batsman", y="batting_average", text="batting_average",
                     color="batting_average", color_continuous_scale="Greens")
        fig.update_layout(xaxis_title="", yaxis_title="Average",
                          coloraxis_showscale=False, height=420)
        st.plotly_chart(fig, use_container_width=True)
        with st.expander("View data"):
            st.dataframe(df, use_container_width=True)

    # ── Tab 2: Best Strike Rate (KPI 5) ──────────────────────────────────────
    with tabs[2]:
        st.subheader("Best Strike Rate (min 500 balls)")
        df = load_kpi(delta_base, "best_strike_rate")
        if df.empty:
            no_data_warning("best_strike_rate"); return
        df = df.sort_values("strike_rate", ascending=False).head(20)
        fig = px.bar(df, x="batsman", y="strike_rate", text="strike_rate",
                     color="strike_rate", color_continuous_scale="Reds")
        fig.update_layout(xaxis_title="", yaxis_title="Strike Rate",
                          coloraxis_showscale=False, height=420)
        st.plotly_chart(fig, use_container_width=True)
        with st.expander("View data"):
            st.dataframe(df, use_container_width=True)

    # ── Tab 3: Highest Individual Scores (KPI 7) ─────────────────────────────
    with tabs[3]:
        st.subheader("Highest Individual Scores")
        df = load_kpi(delta_base, "highest_individual_scores")
        if df.empty:
            no_data_warning("highest_individual_scores"); return
        df = df.sort_values("score", ascending=False).head(20).reset_index(drop=True)
        df.index += 1
        st.dataframe(
            df[["batsman", "score", "balls_faced", "match_type", "match_id"]],
            use_container_width=True, height=500,
        )

    # ── Tab 4: Most Sixes & Fours (KPIs 8, 9) ───────────────────────────────
    with tabs[4]:
        c1, c2 = st.columns(2)
        with c1:
            st.subheader("💥 Most Sixes")
            df = load_kpi(delta_base, "most_sixes")
            if df.empty:
                no_data_warning("most_sixes")
            else:
                df = df.sort_values("sixes", ascending=True).tail(15)
                fig = px.bar(df, x="sixes", y="batsman", orientation="h",
                             text="sixes", color="sixes",
                             color_continuous_scale="Purples")
                fig.update_layout(yaxis_title="", xaxis_title="Sixes",
                                  coloraxis_showscale=False, height=450)
                st.plotly_chart(fig, use_container_width=True)
        with c2:
            st.subheader("🏓 Most Fours")
            df = load_kpi(delta_base, "most_fours")
            if df.empty:
                no_data_warning("most_fours")
            else:
                df = df.sort_values("fours", ascending=True).tail(15)
                fig = px.bar(df, x="fours", y="batsman", orientation="h",
                             text="fours", color="fours",
                             color_continuous_scale="Oranges")
                fig.update_layout(yaxis_title="", xaxis_title="Fours",
                                  coloraxis_showscale=False, height=450)
                st.plotly_chart(fig, use_container_width=True)

    # ── Tab 5: Player by Match Type (KPI 18) ─────────────────────────────────
    with tabs[5]:
        st.subheader("Player Performance by Match Type")
        df = load_kpi(delta_base, "player_by_match_type")
        if df.empty:
            no_data_warning("player_by_match_type"); return
        mt = st.selectbox("Match type", df["match_type"].unique(), key="bat_mt")
        subset = df[df["match_type"] == mt].sort_values("total_runs", ascending=False).head(15)
        fig = px.scatter(subset, x="balls_faced", y="total_runs", size="strike_rate",
                         color="strike_rate", hover_name="batsman",
                         color_continuous_scale="Turbo",
                         labels={"balls_faced": "Balls Faced", "total_runs": "Total Runs"})
        fig.update_layout(height=500)
        st.plotly_chart(fig, use_container_width=True)
        with st.expander("View data"):
            st.dataframe(subset, use_container_width=True)

    # ── Tab 6: Top Partnerships (KPI 19) ─────────────────────────────────────
    with tabs[6]:
        st.subheader("Top Partnerships")
        df = load_kpi(delta_base, "partnership_analysis")
        if df.empty:
            no_data_warning("partnership_analysis"); return
        df = df.sort_values("partnership_runs", ascending=False).head(20).reset_index(drop=True)
        df.index += 1
        df["pair"] = df["batsman"] + " & " + df["non_striker"]
        fig = px.bar(df, x="pair", y="partnership_runs", text="partnership_runs",
                     color="partnership_runs", color_continuous_scale="Sunset")
        fig.update_layout(xaxis_title="", yaxis_title="Runs",
                          coloraxis_showscale=False, height=420,
                          xaxis_tickangle=-45)
        st.plotly_chart(fig, use_container_width=True)

    # ── Tab 7: Player Consistency (KPI 28) ───────────────────────────────────
    with tabs[7]:
        st.subheader("Most Consistent Batsmen (lowest score std-dev)")
        df = load_kpi(delta_base, "player_consistency")
        if df.empty:
            no_data_warning("player_consistency"); return
        df = df.sort_values("score_stddev", ascending=True).head(20)
        fig = px.scatter(df, x="avg_score", y="score_stddev", hover_name="batsman",
                         size="innings", color="match_type",
                         labels={"avg_score": "Avg Score", "score_stddev": "Std Dev"})
        fig.update_layout(height=480)
        st.plotly_chart(fig, use_container_width=True)
        with st.expander("View data"):
            st.dataframe(df, use_container_width=True)

    # ── Tab 8: Win Contribution (KPI 30) ─────────────────────────────────────
    with tabs[8]:
        st.subheader("Match-Winning Contribution Index")
        df = load_kpi(delta_base, "win_contribution")
        if df.empty:
            no_data_warning("win_contribution"); return
        df = df.sort_values("win_contribution_pct", ascending=False).head(20)
        fig = px.bar(df, x="batsman", y="win_contribution_pct",
                     text="win_contribution_pct", color="match_type",
                     barmode="group")
        fig.update_layout(xaxis_title="", yaxis_title="Win Contribution %",
                          height=420, xaxis_tickangle=-45)
        st.plotly_chart(fig, use_container_width=True)
        with st.expander("View data"):
            st.dataframe(df, use_container_width=True)

