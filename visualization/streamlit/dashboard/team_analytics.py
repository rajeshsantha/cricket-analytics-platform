"""Team Analytics page — KPIs 10, 15, 16, 17, 22, 25, 27."""

import streamlit as st
import plotly.express as px
from .helpers import load_kpi, no_data_warning, flag


def _flag_team_col(df, col="team"):
    """Add flag emojis to a team column for display."""
    df = df.copy()
    df[col] = df[col].map(lambda t: f"{flag(t)} {t}")
    return df


def render(delta_base: str) -> None:
    st.title("👥 Team Analytics")

    tabs = st.tabs([
        "Most Wins", "Powerplay RR", "Highest Totals",
        "Lowest Chases", "Runs / Wicket", "Extras",
        "Head-to-Head",
    ])

    # ── Tab 0: Most Wins by Team (KPI 17) ────────────────────────────────────
    with tabs[0]:
        st.subheader("🏆 Most Matches Won by Team")
        df = load_kpi(delta_base, "most_wins_by_team")
        if df.empty:
            no_data_warning("most_wins_by_team"); return
        df = _flag_team_col(df)
        fig = px.bar(df.sort_values("wins", ascending=False).head(15),
                     x="team", y="wins", color="match_type",
                     barmode="group", text_auto=True)
        fig.update_layout(xaxis_title="", yaxis_title="Wins", height=450,
                          xaxis_tickangle=-45)
        st.plotly_chart(fig, use_container_width=True)

    # ── Tab 1: Powerplay Run Rate (KPI 10) ───────────────────────────────────
    with tabs[1]:
        st.subheader("Powerplay Run Rate by Team (overs 1–6)")
        df = load_kpi(delta_base, "powerplay_run_rate")
        if df.empty:
            no_data_warning("powerplay_run_rate"); return
        df = df.sort_values("powerplay_run_rate", ascending=False).head(20)
        df = _flag_team_col(df, "team")
        fig = px.bar(df, x="team", y="powerplay_run_rate", color="match_type",
                     text="powerplay_run_rate", barmode="group")
        fig.update_layout(xaxis_title="", yaxis_title="Powerplay RR", height=420,
                          xaxis_tickangle=-45)
        st.plotly_chart(fig, use_container_width=True)

    # ── Tab 2: Highest Team Totals (KPI 15) ──────────────────────────────────
    with tabs[2]:
        st.subheader("Highest Team Totals")
        df = load_kpi(delta_base, "highest_team_totals")
        if df.empty:
            no_data_warning("highest_team_totals"); return
        df = df.sort_values("team_total", ascending=False).head(20).reset_index(drop=True)
        df.index += 1
        df = _flag_team_col(df)
        fig = px.bar(df, x="team", y="team_total", text="team_total",
                     color="team_total", color_continuous_scale="Inferno",
                     hover_data=["venue", "match_id"])
        fig.update_layout(xaxis_title="", yaxis_title="Total",
                          coloraxis_showscale=False, height=420,
                          xaxis_tickangle=-45)
        st.plotly_chart(fig, use_container_width=True)
        with st.expander("View data"):
            st.dataframe(df, use_container_width=True)

    # ── Tab 3: Lowest Successful Chases (KPI 16) ─────────────────────────────
    with tabs[3]:
        st.subheader("Lowest Successful Chases")
        df = load_kpi(delta_base, "lowest_successful_chases")
        if df.empty:
            no_data_warning("lowest_successful_chases"); return
        df = df.sort_values("chase_total", ascending=True).head(20).reset_index(drop=True)
        df.index += 1
        df = _flag_team_col(df)
        st.dataframe(
            df[["team", "chase_total", "venue", "match_type", "match_id"]],
            use_container_width=True, height=500,
        )

    # ── Tab 4: Average Runs per Wicket (KPI 22) ──────────────────────────────
    with tabs[4]:
        st.subheader("Average Runs per Wicket by Innings")
        df = load_kpi(delta_base, "avg_runs_per_wicket")
        if df.empty:
            no_data_warning("avg_runs_per_wicket"); return
        fig = px.bar(df, x="inning", y="avg_runs_per_wicket", color="match_type",
                     barmode="group", text_auto=True)
        fig.update_layout(xaxis_title="Innings", yaxis_title="Avg Runs / Wicket",
                          height=400)
        st.plotly_chart(fig, use_container_width=True)

    # ── Tab 5: Extras Analysis (KPI 25) ──────────────────────────────────────
    with tabs[5]:
        st.subheader("Extras per Match by Team")
        df = load_kpi(delta_base, "extras_analysis")
        if df.empty:
            no_data_warning("extras_analysis"); return
        df = df.sort_values("avg_extras_per_match", ascending=False).head(20)
        df = _flag_team_col(df)
        fig = px.bar(df, x="team", y="avg_extras_per_match", color="match_type",
                     text="avg_extras_per_match", barmode="group")
        fig.update_layout(xaxis_title="", yaxis_title="Avg Extras / Match",
                          height=420, xaxis_tickangle=-45)
        st.plotly_chart(fig, use_container_width=True)

    # ── Tab 6: Head-to-Head (KPI 27) ─────────────────────────────────────────
    with tabs[6]:
        st.subheader("Head-to-Head Records")
        df = load_kpi(delta_base, "head_to_head")
        if df.empty:
            no_data_warning("head_to_head"); return
        df = df.sort_values("matches_played", ascending=False).head(30).reset_index(drop=True)
        df.index += 1

        # Let user pick a team pair
        df["matchup"] = df["team1"].map(lambda t: f"{flag(t)} {t}") + " vs " + df["team2"].map(lambda t: f"{flag(t)} {t}")
        selected = st.multiselect("Filter matchups", df["matchup"].unique(),
                                  default=df["matchup"].unique()[:10],
                                  key="h2h_filter")
        filtered = df[df["matchup"].isin(selected)] if selected else df

        fig = px.bar(filtered, x="matchup", y=["team1_wins", "team2_wins", "no_result"],
                     barmode="stack", text_auto=True,
                     labels={"value": "Matches", "variable": "Result"})
        fig.update_layout(xaxis_title="", yaxis_title="Matches", height=450,
                          xaxis_tickangle=-45)
        st.plotly_chart(fig, use_container_width=True)
        with st.expander("View data"):
            st.dataframe(filtered, use_container_width=True)

