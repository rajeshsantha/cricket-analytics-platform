"""Overview page — headline numbers + quick charts from multiple KPIs."""

import streamlit as st
import plotly.express as px
from .helpers import (load_kpi, no_data_warning, enrich_player_df, flag,
                      flagged_name, chart_type_selector, render_chart)


def render(delta_base: str) -> None:
    st.title("🏆 T20 World Cup 2026 — Analytics Dashboard")
    st.markdown(
        "**ICC Men's T20 World Cup 2026** · India & Sri Lanka · Feb–Mar 2026\n\n"
        "48 matches analyzed across **30 Gold-layer KPIs** computed from Cricsheet ball-by-ball data."
    )

    # ── Row 1: headline metric cards ──────────────────────────────────────────
    top_scorers = load_kpi(delta_base, "top_run_scorers")
    top_wickets = load_kpi(delta_base, "top_wicket_takers")
    most_sixes  = load_kpi(delta_base, "most_sixes")
    most_fours  = load_kpi(delta_base, "most_fours")

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        if not top_scorers.empty:
            row = top_scorers.sort_values("total_runs", ascending=False).iloc[0]
            st.metric("🏏 Top Run Scorer",
                      flagged_name(row["batsman"]),
                      f"↑ {int(row['total_runs']):,} runs")
        else:
            st.metric("🏏 Top Run Scorer", "—")
    with c2:
        if not top_wickets.empty:
            row = top_wickets.sort_values("wickets", ascending=False).iloc[0]
            st.metric("🎳 Top Wicket Taker",
                      flagged_name(row["bowler"]),
                      f"↑ {int(row['wickets'])} wkts")
        else:
            st.metric("🎳 Top Wicket Taker", "—")
    with c3:
        if not most_sixes.empty:
            row = most_sixes.sort_values("sixes", ascending=False).iloc[0]
            st.metric("💥 Most Sixes",
                      flagged_name(row["batsman"]),
                      f"↑ {int(row['sixes'])} sixes")
        else:
            st.metric("💥 Most Sixes", "—")
    with c4:
        if not most_fours.empty:
            row = most_fours.sort_values("fours", ascending=False).iloc[0]
            st.metric("🏓 Most Fours",
                      flagged_name(row["batsman"]),
                      f"↑ {int(row['fours'])} fours")
        else:
            st.metric("🏓 Most Fours", "—")

    st.markdown("---")

    # ── Row 2: Top Run Scorers bar chart + Top Wicket Takers bar chart ────────
    chart_type = chart_type_selector(key="overview_chart_type", default="Horizontal Bar")

    col_a, col_b = st.columns(2)

    with col_a:
        st.subheader("🏏 Top 10 Run Scorers")
        if not top_scorers.empty:
            df = enrich_player_df(top_scorers, "batsman")
            df = df.sort_values("total_runs", ascending=False).head(10)
            render_chart(df, "player_display", "total_runs", chart_type,
                         "Oranges", "Runs", height=400)
        else:
            no_data_warning("top_run_scorers")

    with col_b:
        st.subheader("🎳 Top 10 Wicket Takers")
        if not top_wickets.empty:
            df = enrich_player_df(top_wickets, "bowler")
            df = df.sort_values("wickets", ascending=False).head(10)
            render_chart(df, "player_display", "wickets", chart_type,
                         "Blues", "Wickets", height=400)
        else:
            no_data_warning("top_wicket_takers")

    st.markdown("---")

    # ── Row 3: Most Wins by Team ──────────────────────────────────────────────
    st.subheader("🏆 Most Wins by Team")
    wins = load_kpi(delta_base, "most_wins_by_team")
    if not wins.empty:
        wins = wins.copy()
        wins["team_display"] = wins["team"].map(lambda t: f"{flag(t)} {t}")
        fig = px.bar(wins.sort_values("wins", ascending=False).head(15),
                     x="team_display", y="wins", color="match_type",
                     barmode="group", text_auto=True)
        fig.update_layout(xaxis_title="", yaxis_title="Wins", height=420)
        st.plotly_chart(fig, use_container_width=True)
    else:
        no_data_warning("most_wins_by_team")

    # ── Row 4: Run Rate Progression ───────────────────────────────────────────
    st.subheader("📈 Average Run-Rate Progression (over by over)")
    rr = load_kpi(delta_base, "run_rate_progression")
    if not rr.empty:
        teams = sorted(rr["team"].unique()) if "team" in rr.columns else []
        selected_team = st.selectbox("Select Team", ["All Teams"] + teams,
                                     key="overview_rr_team")
        if selected_team != "All Teams" and "team" in rr.columns:
            rr_plot = rr[rr["team"] == selected_team].copy()
        else:
            # Recalculate overall average from per-team per-over values
            rr_plot = rr.groupby(["match_type", "over_num"], as_index=False).agg(
                avg_runs_per_over=("avg_runs_per_over", "mean"),
            )
            rr_plot = rr_plot.sort_values("over_num")
            cumulative = rr_plot.groupby("match_type")["avg_runs_per_over"].cumsum()
            overs_so_far = rr_plot.groupby("match_type").cumcount() + 1
            rr_plot["avg_run_rate"] = (cumulative / overs_so_far).round(2)
            rr_plot["avg_runs_per_over"] = rr_plot["avg_runs_per_over"].round(2)

        fig = px.line(rr_plot, x="over_num", y="avg_run_rate",
                      color="match_type" if "match_type" in rr_plot.columns else None,
                      markers=True)
        fig.update_layout(xaxis_title="Over", yaxis_title="Avg Run Rate",
                          height=380)
        st.plotly_chart(fig, use_container_width=True)
    else:
        no_data_warning("run_rate_progression")

