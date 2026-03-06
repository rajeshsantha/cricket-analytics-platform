"""Venue & Toss page — KPIs 12, 13, 14, 26."""

import streamlit as st
import plotly.express as px
from .helpers import load_kpi, no_data_warning


def render(delta_base: str) -> None:
    st.title("🏟️ Venue & Toss Analytics")

    tabs = st.tabs([
        "Toss Decision", "Bat First Win %", "Avg Score by Venue",
        "Home vs Away",
    ])

    # ── Tab 0: Win % by Toss Decision (KPI 12) ──────────────────────────────
    with tabs[0]:
        st.subheader("Win % by Toss Decision")
        df = load_kpi(delta_base, "win_by_toss_decision")
        if df.empty:
            no_data_warning("win_by_toss_decision"); return
        fig = px.bar(df, x="toss_decision", y="win_pct", color="match_type",
                     barmode="group", text="win_pct",
                     labels={"win_pct": "Win %", "toss_decision": "Toss Decision"})
        fig.update_layout(height=400, yaxis_range=[0, 100])
        st.plotly_chart(fig, use_container_width=True)

        st.markdown("**Does winning the toss matter?**")
        for _, row in df.iterrows():
            delta = "advantage" if row["win_pct"] > 50 else "no advantage"
            st.caption(
                f"{row['match_type']} — {row['toss_decision']}: "
                f"{row['win_pct']:.1f}% win rate → **{delta}**"
            )

    # ── Tab 1: Win % Batting First (KPI 13) ─────────────────────────────────
    with tabs[1]:
        st.subheader("Win % Batting First vs Chasing")
        df = load_kpi(delta_base, "win_batting_first")
        if df.empty:
            no_data_warning("win_batting_first"); return
        df["chasing_win_pct"] = 100 - df["batting_first_win_pct"]
        fig = px.bar(df, x="match_type",
                     y=["batting_first_win_pct", "chasing_win_pct"],
                     barmode="group", text_auto=".1f",
                     labels={"value": "Win %", "variable": ""})
        fig.update_layout(height=400, yaxis_range=[0, 100])
        st.plotly_chart(fig, use_container_width=True)
        with st.expander("View data"):
            st.dataframe(df, use_container_width=True)

    # ── Tab 2: Average Score by Venue (KPI 14) ──────────────────────────────
    with tabs[2]:
        st.subheader("Average Innings Score by Venue")
        df = load_kpi(delta_base, "avg_score_by_venue")
        if df.empty:
            no_data_warning("avg_score_by_venue"); return
        # Let user filter by match type
        if "match_type" in df.columns:
            mt = st.selectbox("Match type", df["match_type"].unique(), key="venue_mt")
            df = df[df["match_type"] == mt]
        df = df.sort_values("avg_score", ascending=True).tail(20)
        fig = px.bar(df, x="avg_score", y="venue", orientation="h",
                     color="avg_score", text="avg_score",
                     color_continuous_scale="YlOrRd")
        fig.update_layout(yaxis_title="", xaxis_title="Avg Score",
                          coloraxis_showscale=False, height=550)
        st.plotly_chart(fig, use_container_width=True)

    # ── Tab 3: Home vs Away Win % (KPI 26) ──────────────────────────────────
    with tabs[3]:
        st.subheader("Home vs Away Wins (approximated by venue)")
        df = load_kpi(delta_base, "home_away_win_pct")
        if df.empty:
            no_data_warning("home_away_win_pct"); return
        if "home_away" in df.columns:
            agg = df.groupby("home_away", as_index=False)["wins"].sum()
            fig = px.pie(agg, names="home_away", values="wins",
                         color="home_away",
                         color_discrete_map={"Home": "#2ecc71", "Away": "#e74c3c"},
                         hole=0.4)
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        with st.expander("View data"):
            st.dataframe(df.head(30), use_container_width=True)

