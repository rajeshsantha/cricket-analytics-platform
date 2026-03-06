"""
Cricket Analytics Platform — Streamlit Dashboard
Reads all 30 Gold Delta batch-KPI tables and the live-KPI table,
then renders an interactive, multi-page dashboard.
"""

import streamlit as st

st.set_page_config(
    page_title="🏏 Cricket Analytics Platform",
    page_icon="🏏",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─── Sidebar ───────────────────────────────────────────────────────────────────
st.sidebar.title("🏏 Cricket Analytics")
st.sidebar.markdown("---")

DELTA_BASE = st.sidebar.text_input(
    "Delta Lake Base Path",
    value="/tmp/cricket-delta",
    help="Root folder that contains gold/batch_kpis/* and gold/live_kpis",
)

PAGES = [
    "🏠 Overview",
    "🏏 Batting",
    "🎳 Bowling",
    "👥 Team Analytics",
    "🏟️ Venue & Toss",
    "📈 Match Trends",
    "📊 Live Scorecard",
]

page = st.sidebar.radio("Navigate", PAGES)

st.sidebar.markdown("---")
st.sidebar.caption("Powered by Apache Spark · Delta Lake · Streamlit")

# ─── Route to page modules ────────────────────────────────────────────────────
if page == PAGES[0]:
    from pages import overview
    overview.render(DELTA_BASE)
elif page == PAGES[1]:
    from pages import batting
    batting.render(DELTA_BASE)
elif page == PAGES[2]:
    from pages import bowling
    bowling.render(DELTA_BASE)
elif page == PAGES[3]:
    from pages import team_analytics
    team_analytics.render(DELTA_BASE)
elif page == PAGES[4]:
    from pages import venue_toss
    venue_toss.render(DELTA_BASE)
elif page == PAGES[5]:
    from pages import match_trends
    match_trends.render(DELTA_BASE)
elif page == PAGES[6]:
    from pages import live_scorecard
    live_scorecard.render(DELTA_BASE)
