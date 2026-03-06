"""Live Scorecard page — reads the Gold live-KPI Delta table (streaming)."""

import os
import time
import streamlit as st
import plotly.graph_objects as go
import pandas as pd
from .helpers import no_data_warning

try:
    from deltalake import DeltaTable
except ImportError:
    DeltaTable = None


def render(delta_base: str) -> None:
    st.title("📊 Live Match Scorecard")

    match_id = st.text_input("Match ID (leave blank for latest)", value="", key="live_mid")

    auto = st.checkbox("Auto-refresh (5 s)", value=False, key="live_auto")

    live_path = os.path.join(delta_base, "gold", "live_kpis")

    try:
        dt = DeltaTable(live_path)
        df = dt.to_pandas()
    except Exception:
        st.info(
            "No live KPI data found. Start the **streaming pipeline** first:\n\n"
            "```bash\nbash scripts/run_streaming.sh\n```"
        )
        return

    if match_id:
        df = df[df["match_id"] == match_id]

    if df.empty:
        st.info("No data for this match ID.")
        return

    df = df.sort_values("computed_at", ascending=False)
    latest = df.iloc[0]

    # ── Key metrics ──────────────────────────────────────────────────────────
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Match", latest.get("match_id", "—"))
    c2.metric("Run Rate", f"{latest.get('run_rate', 0):.2f}")
    c3.metric("Wickets (window)", int(latest.get("wickets_in_window", 0)))
    c4.metric("Bowler Economy", f"{latest.get('bowler_economy', 0):.2f}")

    st.markdown("---")

    col_a, col_b = st.columns(2)
    with col_a:
        st.subheader("🏏 Batsman")
        st.write(latest.get("current_batsman", "—"))
        st.caption(f"Strike Rate: {latest.get('batting_strike_rate', 0):.1f}")
    with col_b:
        st.subheader("🎳 Bowler")
        st.write(latest.get("current_bowler", "—"))

    # ── Run-rate timeline ────────────────────────────────────────────────────
    st.subheader("📈 Run Rate Over Time")
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=df["window_start"] if "window_start" in df.columns else df.index,
        y=df["run_rate"],
        marker_color="steelblue",
    ))
    fig.update_layout(xaxis_title="Window", yaxis_title="Run Rate", height=350)
    st.plotly_chart(fig, use_container_width=True)

    if auto:
        time.sleep(5)
        st.rerun()

