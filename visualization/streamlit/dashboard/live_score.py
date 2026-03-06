"""
Live Score Dashboard — real-time match scores via CricAPI.

No Kafka or Spark required. Polls CricAPI directly from the Streamlit app.
Enter your CricAPI key in the sidebar and provide a Match ID.
"""

import os
import time
import streamlit as st
import pandas as pd
import plotly.graph_objects as go

# Absolute import so it works whether run as module or directly
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from components.live_score_poller import LiveScorePoller

from dashboard.helpers import flag, TEAM_FLAGS


def render(delta_base: str) -> None:
    st.title("📡 Live Match Score")

    # ── Sidebar config ────────────────────────────────────────────────────
    api_key = st.sidebar.text_input(
        "🔑 CricAPI Key",
        value=os.environ.get("CRICAPI_KEY", ""),
        type="password",
        help="Get a free API key at https://cricapi.com",
        key="live_api_key",
    )

    # Check query params for match_id
    query_params = st.query_params
    default_match_id = query_params.get("match_id", "")

    match_id = st.text_input(
        "🏏 Match ID",
        value=default_match_id,
        placeholder="e.g. d9032b36-d872-4011-b96c-73a9137e7ced",
        help=(
            "Paste the CricAPI match ID. You can get it from "
            "[CricAPI Match List](https://cricapi.com) or use the "
            "'Browse Live Matches' button below."
        ),
        key="live_match_id",
    )

    col_refresh, col_auto = st.columns([1, 1])
    with col_refresh:
        refresh_interval = st.selectbox(
            "⏱️ Refresh interval",
            options=[5, 10, 15, 30, 60],
            index=1,
            format_func=lambda x: f"{x} seconds",
            key="live_refresh_interval",
        )
    with col_auto:
        auto_refresh = st.checkbox("🔄 Auto-refresh", value=False, key="live_auto_refresh")

    st.markdown("---")

    poller = LiveScorePoller(api_key=api_key)

    # ── Browse current matches ────────────────────────────────────────────
    if not match_id:
        st.subheader("📋 Browse Current Matches")
        if not poller.is_configured():
            st.warning(
                "⚠️ **No API key configured.** Enter your CricAPI key in the sidebar.\n\n"
                "Get a free key at [cricapi.com](https://cricapi.com) "
                "(100 requests/day on free tier)."
            )
            _show_demo_mode()
            return

        with st.spinner("Fetching current matches…"):
            matches = poller.fetch_current_matches()

        if not matches:
            st.info("No live/recent matches found at the moment.")
            _show_demo_mode()
            return

        # Show match cards
        for m in matches:
            teams_str = " vs ".join(m.get("teams", []))
            match_status = "🟢 LIVE" if m.get("matchStarted") and not m.get("matchEnded") else (
                "✅ Completed" if m.get("matchEnded") else "⏳ Upcoming"
            )
            scores = m.get("score", [])
            score_lines = []
            for s in scores:
                inning = s.get("inning", "")
                r = s.get("r", 0)
                w = s.get("w", 0)
                o = s.get("o", 0)
                score_lines.append(f"{inning}: **{r}/{w}** ({o} ov)")

            with st.container():
                c1, c2, c3 = st.columns([3, 2, 1])
                with c1:
                    st.markdown(f"**{teams_str}**")
                    st.caption(f"{m.get('match_type', 'T20').upper()} · {m.get('venue', '')}")
                with c2:
                    for line in score_lines:
                        st.markdown(line)
                    if not score_lines:
                        st.caption(m.get("status", ""))
                with c3:
                    st.markdown(f"**{match_status}**")
                    if st.button("📊 View", key=f"view_{m['id']}"):
                        st.query_params["match_id"] = m["id"]
                        st.rerun()
                st.markdown("---")
        return

    # ── Live score display ────────────────────────────────────────────────
    if not poller.is_configured():
        st.error("⚠️ CricAPI key required. Enter it in the sidebar.")
        return

    with st.spinner("Fetching live score…"):
        scorecard = poller.fetch_scorecard(match_id)

    if scorecard["status"] in ("no_api_key", "error", "timeout", "connection_error"):
        st.error(f"❌ Failed to fetch score: **{scorecard['status']}**")
        if "error" in scorecard["status"]:
            st.caption(scorecard["status"])
        return

    # ── Match header ──────────────────────────────────────────────────────
    teams = scorecard.get("teams", [])
    team_str = " vs ".join(teams) if teams else "Match"
    team_flags = " ".join([flag(t) for t in teams])

    st.markdown(f"## {team_flags} {team_str}")

    # Status badge
    status = scorecard.get("result") or scorecard.get("status", "")
    if "won" in status.lower() or "result" in status.lower():
        st.success(f"✅ {status}")
    elif any(kw in status.lower() for kw in ("live", "progress", "play", "break")):
        st.info(f"🟢 {status}")
    else:
        st.info(f"ℹ️ {status}")

    # Toss + venue
    toss = scorecard.get("toss", "")
    venue = scorecard.get("venue", "")
    meta_parts = []
    if venue:
        meta_parts.append(f"🏟️ {venue}")
    if toss:
        meta_parts.append(f"🪙 {toss}")
    if meta_parts:
        st.caption(" · ".join(meta_parts))

    st.markdown("---")

    # ── Score summary ─────────────────────────────────────────────────────
    score_list = scorecard.get("score", [])
    if score_list:
        cols = st.columns(len(score_list))
        for i, s in enumerate(score_list):
            inning = s.get("inning", f"Innings {i+1}")
            runs = s.get("r", 0)
            wickets = s.get("w", 0)
            overs = s.get("o", 0)
            rr = round(runs / overs, 2) if overs else 0

            # Find team flag
            inning_flag = ""
            for t in teams:
                if t.lower() in inning.lower():
                    inning_flag = flag(t)
                    break

            with cols[i]:
                st.metric(
                    label=f"{inning_flag} {inning}",
                    value=f"{runs}/{wickets}",
                    delta=f"RR: {rr} · {overs} ov",
                )

    st.markdown("---")

    # ── Batting scorecard ─────────────────────────────────────────────────
    batting = scorecard.get("batting", [])
    if batting:
        # Group by innings
        innings_names = list(dict.fromkeys(b["inning"] for b in batting))
        tabs = st.tabs([f"🏏 {name}" for name in innings_names])
        for tab, inning_name in zip(tabs, innings_names):
            with tab:
                inning_batters = [b for b in batting if b["inning"] == inning_name]
                if inning_batters:
                    df_bat = pd.DataFrame(inning_batters)
                    df_bat = df_bat[["name", "dismissal", "runs", "balls", "fours", "sixes", "sr"]]
                    df_bat.columns = ["Batsman", "Dismissal", "Runs", "Balls", "4s", "6s", "SR"]
                    df_bat.index = range(1, len(df_bat) + 1)

                    # Highlight top scorer
                    st.dataframe(
                        df_bat,
                        use_container_width=True,
                        height=min(500, 40 + 35 * len(df_bat)),
                    )

                    # Mini bar chart for runs
                    fig = go.Figure()
                    fig.add_trace(go.Bar(
                        x=df_bat["Batsman"],
                        y=df_bat["Runs"],
                        text=df_bat["Runs"],
                        textposition="outside",
                        marker_color=["#FFD700" if r == df_bat["Runs"].max() else "#4169E1"
                                      for r in df_bat["Runs"]],
                    ))
                    fig.update_layout(
                        height=300,
                        xaxis_title="",
                        yaxis_title="Runs",
                        showlegend=False,
                        margin=dict(t=20, b=20),
                    )
                    st.plotly_chart(fig, use_container_width=True)

    # ── Bowling scorecard ─────────────────────────────────────────────────
    bowling = scorecard.get("bowling", [])
    if bowling:
        innings_names_bowl = list(dict.fromkeys(b["inning"] for b in bowling))
        tabs_bowl = st.tabs([f"🎳 {name}" for name in innings_names_bowl])
        for tab, inning_name in zip(tabs_bowl, innings_names_bowl):
            with tab:
                inning_bowlers = [b for b in bowling if b["inning"] == inning_name]
                if inning_bowlers:
                    df_bowl = pd.DataFrame(inning_bowlers)
                    df_bowl = df_bowl[["name", "overs", "maidens", "runs", "wickets", "economy"]]
                    df_bowl.columns = ["Bowler", "Overs", "Maidens", "Runs", "Wickets", "Economy"]
                    df_bowl.index = range(1, len(df_bowl) + 1)
                    st.dataframe(
                        df_bowl,
                        use_container_width=True,
                        height=min(400, 40 + 35 * len(df_bowl)),
                    )

    # ── Footer ────────────────────────────────────────────────────────────
    st.markdown("---")
    last_updated = scorecard.get("last_updated", "")
    st.caption(f"Last updated: {last_updated} · Match ID: `{match_id}`")

    # Shareable link
    st.caption(f"📎 Share this score: `?match_id={match_id}`")

    # Auto-refresh
    if auto_refresh:
        time.sleep(refresh_interval)
        st.rerun()


def _show_demo_mode():
    """Show demo/instructions when no API key is available."""
    st.markdown("---")
    st.subheader("🚀 How to Use Live Scores")
    st.markdown("""
    1. **Get a free API key** from [cricapi.com](https://cricapi.com)
       (100 requests/day on the free tier)

    2. **Set the key** — either:
       - Enter it in the sidebar text field, OR
       - Set the `CRICAPI_KEY` environment variable:
         ```bash
         export CRICAPI_KEY="your-api-key-here"
         ```

    3. **Find a Match ID** — click "Browse Live Matches" to see current matches,
       or get a match ID from the CricAPI website.

    4. **Enter the Match ID** in the text field above.

    5. **Enable auto-refresh** to see live updates every 5–60 seconds.

    ---

    **💡 Tip:** You can also share a live score link with a match ID in the URL:
    ```
    https://your-app.streamlit.app/?match_id=abc123
    ```

    **Spark Streaming mode** (Kafka required):
    Use the "📊 Live Scorecard" page for the full Spark-based streaming pipeline
    that uses Kafka and Delta Lake.
    """)

