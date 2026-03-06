"""Match Trends page — KPIs 21, 23, 24."""

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from .helpers import load_kpi, no_data_warning


def render(delta_base: str) -> None:
    st.title("📈 Match Trends & Phase Analysis")

    tabs = st.tabs([
        "Boundary % by Phase", "Pressure Index", "Run-Rate Progression",
    ])

    # ── Tab 0: Boundary % per Phase (KPI 21) ────────────────────────────────
    with tabs[0]:
        st.subheader("Boundary % per Over Phase")
        st.caption("Powerplay (1–6), Middle (7–15), Death (16–20)")
        df = load_kpi(delta_base, "boundary_pct_per_phase")
        if df.empty:
            no_data_warning("boundary_pct_per_phase"); return

        phase_order = ["Powerplay", "Middle", "Death"]
        df["phase"] = df["phase"].astype("category")

        fig = px.bar(df, x="phase", y="boundary_pct", color="match_type",
                     barmode="group", text="boundary_pct",
                     category_orders={"phase": phase_order})
        fig.update_layout(xaxis_title="Phase", yaxis_title="Boundary %",
                          height=420)
        st.plotly_chart(fig, use_container_width=True)
        with st.expander("View data"):
            st.dataframe(df, use_container_width=True)

    # ── Tab 1: Pressure Index per Over (KPI 23) ─────────────────────────────
    with tabs[1]:
        st.subheader("Pressure Index per Over")
        st.caption(
            "Pressure = wickets × 10 + (6 − runs_in_over). "
            "Higher ⇒ more pressure on batting side."
        )
        df = load_kpi(delta_base, "pressure_index_per_over")
        if df.empty:
            no_data_warning("pressure_index_per_over"); return

        # Match selector
        matches = df["match_id"].unique()
        selected_match = st.selectbox(
            "Select a match", matches[:100], key="pi_match"
        )
        mdf = df[df["match_id"] == selected_match].sort_values(
            ["inning", "over_num"]
        )

        fig = go.Figure()
        for inning in sorted(mdf["inning"].unique()):
            idf = mdf[mdf["inning"] == inning]
            fig.add_trace(go.Scatter(
                x=idf["over_num"], y=idf["pressure_index"],
                mode="lines+markers", name=str(inning),
                marker=dict(size=8),
            ))

        fig.add_hline(y=6, line_dash="dot", line_color="orange",
                      annotation_text="Moderate pressure")
        fig.add_hline(y=16, line_dash="dot", line_color="red",
                      annotation_text="High pressure (wicket)")

        fig.update_layout(
            xaxis_title="Over", yaxis_title="Pressure Index",
            height=480, legend_title="Innings",
        )
        st.plotly_chart(fig, use_container_width=True)

        # Legend
        c1, c2, c3 = st.columns(3)
        c1.success("🟢  < 6  — Low pressure")
        c2.warning("🟡  6–16 — Moderate pressure")
        c3.error("🔴  > 16 — High pressure")

        with st.expander("View over data"):
            st.dataframe(mdf, use_container_width=True)

    # ── Tab 2: Run-Rate Progression (KPI 24) ─────────────────────────────────
    with tabs[2]:
        st.subheader("Average Run-Rate Progression Over by Over")
        df = load_kpi(delta_base, "run_rate_progression")
        if df.empty:
            no_data_warning("run_rate_progression"); return

        fig = px.line(df, x="over_num", y="avg_runs_per_over",
                      color="match_type", markers=True,
                      labels={"over_num": "Over", "avg_runs_per_over": "Avg Runs / Over"})
        fig.update_layout(height=420)
        st.plotly_chart(fig, use_container_width=True)

        fig2 = px.area(df, x="over_num", y="avg_run_rate",
                       color="match_type",
                       labels={"over_num": "Over", "avg_run_rate": "Avg Run Rate"})
        fig2.update_layout(height=380)
        st.plotly_chart(fig2, use_container_width=True)

