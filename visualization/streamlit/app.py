"""
Cricket Analytics Platform — T20 World Cup 2026 Edition
Streamlit Dashboard powered by 30 Gold-layer KPIs from Cricsheet data.
"""

import streamlit as st

st.set_page_config(
    page_title="🏏 T20 World Cup 2026 Analytics",
    page_icon="🏆",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─── Sidebar ───────────────────────────────────────────────────────────────────
st.sidebar.title("🏆 T20 World Cup 2026")
st.sidebar.caption("ICC Men's T20 World Cup · India & Sri Lanka")
st.sidebar.markdown("---")

DELTA_BASE = st.sidebar.text_input(
    "Delta Lake Base Path",
    value="/tmp/cricket-delta",
    help="Root folder that contains gold/batch_kpis/* and gold/live_kpis",
)

PAGES = [
    "🏠 Overview",
    "📡 Live Score",
    "🏏 Batting",
    "🎳 Bowling",
    "👥 Team Analytics",
    "🏟️ Venue & Toss",
    "📈 Match Trends",
    "📊 Live Scorecard (Spark)",
]

# Default to Live Score if match_id is in query params
default_idx = 0
if st.query_params.get("match_id"):
    default_idx = 1

page = st.sidebar.radio("Navigate", PAGES, index=default_idx)

st.sidebar.markdown("---")
st.sidebar.caption(
    "Data: [Cricsheet](https://cricsheet.org) · "
    "48 matches · 11,306 deliveries\n\n"
    "Powered by Apache Spark · Delta Lake · Streamlit"
)

# ─── Route to page modules ────────────────────────────────────────────────────
if page == PAGES[0]:
    from dashboard import overview
    overview.render(DELTA_BASE)
elif page == PAGES[1]:
    from dashboard import live_score
    live_score.render(DELTA_BASE)
elif page == PAGES[2]:
    from dashboard import batting
    batting.render(DELTA_BASE)
elif page == PAGES[3]:
    from dashboard import bowling
    bowling.render(DELTA_BASE)
elif page == PAGES[4]:
    from dashboard import team_analytics
    team_analytics.render(DELTA_BASE)
elif page == PAGES[5]:
    from dashboard import venue_toss
    venue_toss.render(DELTA_BASE)
elif page == PAGES[6]:
    from dashboard import match_trends
    match_trends.render(DELTA_BASE)
elif page == PAGES[7]:
    from dashboard import live_scorecard
    live_scorecard.render(DELTA_BASE)
