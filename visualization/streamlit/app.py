"""
Cricket Analytics Platform - Streamlit Dashboard
Reads from Gold Delta tables and provides live/batch analytics visualization.
Auto-refreshes every 5 seconds for live match data.
"""

import streamlit as st
import time
from components.live_scorecard import render_live_scorecard
from components.player_stats import render_player_stats
from components.pressure_chart import render_pressure_chart

# ─── Page configuration ───────────────────────────────────────────────────────
st.set_page_config(
    page_title="Cricket Analytics Platform",
    page_icon="🏏",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ─── Sidebar configuration ────────────────────────────────────────────────────
st.sidebar.title("🏏 Cricket Analytics")
st.sidebar.markdown("---")

delta_base = st.sidebar.text_input(
    "Delta Base Path",
    value="/tmp/cricket-delta",
    help="Path to the Delta Lake tables"
)

match_id = st.sidebar.text_input(
    "Match ID (for live data)",
    value="",
    help="Enter a match ID to filter live scorecard"
)

auto_refresh = st.sidebar.checkbox("Auto-refresh (5s)", value=True)
refresh_interval = st.sidebar.slider("Refresh interval (seconds)", 2, 30, 5)

st.sidebar.markdown("---")
st.sidebar.markdown("**Data Paths:**")
st.sidebar.code(f"""
Gold Live:  {delta_base}/gold/live_kpis
Gold Batch: {delta_base}/gold/batch_kpis
""")

# ─── Main dashboard ───────────────────────────────────────────────────────────
st.title("🏏 Cricket Analytics Platform")
st.markdown("Real-time and historical cricket analytics powered by Spark + Delta Lake")

# Tab navigation
tab1, tab2, tab3 = st.tabs(["📊 Live Scorecard", "👤 Player Stats", "📈 Pressure Chart"])

with tab1:
    render_live_scorecard(delta_base, match_id)

with tab2:
    render_player_stats(delta_base)

with tab3:
    render_pressure_chart(delta_base, match_id)

# ─── Auto-refresh ────────────────────────────────────────────────────────────
if auto_refresh:
    time.sleep(refresh_interval)
    st.rerun()
