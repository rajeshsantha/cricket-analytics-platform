"""
Lightweight live score poller — Python-only, no Kafka or Spark required.

Polls CricAPI (or Cricbuzz-style free endpoints) every N seconds and returns
structured match data for the Streamlit dashboard.

Supports:
  - CricAPI v1 (https://cricapi.com) — needs API key
  - Free fallback mode using cricsheet static data for testing

Usage:
    from components.live_score_poller import LiveScorePoller

    poller = LiveScorePoller(api_key="YOUR_KEY")
    data = poller.fetch_live_score(match_id="abc123")
"""

import os
import json
import time
from datetime import datetime
from typing import Optional
import requests
import streamlit as st


# ─── Data Models ──────────────────────────────────────────────────────────────

def empty_match_data() -> dict:
    """Return an empty match data structure."""
    return {
        "match_id": "",
        "status": "unknown",
        "match_type": "",
        "venue": "",
        "date": "",
        "teams": [],
        "score": [],
        "current_innings": "",
        "current_score": "",
        "current_rr": 0.0,
        "required_rr": None,
        "batting": [],       # list of {name, runs, balls, fours, sixes, sr}
        "bowling": [],       # list of {name, overs, maidens, runs, wickets, economy}
        "recent_balls": "",  # e.g. "1 4 0 W 2 6"
        "toss": "",
        "result": "",
        "last_updated": datetime.now().isoformat(),
    }


# ─── CricAPI Poller ───────────────────────────────────────────────────────────

class LiveScorePoller:
    """Polls CricAPI REST API for live match scores."""

    CRICAPI_BASE = "https://api.cricapi.com/v1"

    def __init__(self, api_key: str = ""):
        self.api_key = api_key or os.environ.get("CRICAPI_KEY", "")

    def is_configured(self) -> bool:
        """Check if the API key is set."""
        return bool(self.api_key)

    # ── Fetch list of current/recent matches ──────────────────────────────

    def fetch_current_matches(self) -> list[dict]:
        """Fetch currently live matches from CricAPI."""
        if not self.is_configured():
            return []
        try:
            url = f"{self.CRICAPI_BASE}/currentMatches?apikey={self.api_key}&offset=0"
            resp = requests.get(url, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            if data.get("status") != "success":
                return []
            matches = data.get("data", [])
            return [
                {
                    "id": m.get("id", ""),
                    "name": m.get("name", ""),
                    "status": m.get("status", ""),
                    "match_type": m.get("matchType", ""),
                    "venue": m.get("venue", ""),
                    "date": m.get("date", ""),
                    "teams": m.get("teams", []),
                    "score": m.get("score", []),
                    "matchStarted": m.get("matchStarted", False),
                    "matchEnded": m.get("matchEnded", False),
                }
                for m in matches
                if m.get("matchType", "").lower() in ("t20", "t20i", "odi", "test")
            ]
        except Exception as e:
            st.warning(f"Failed to fetch current matches: {e}")
            return []

    # ── Fetch live score for a specific match ─────────────────────────────

    def fetch_live_score(self, match_id: str) -> dict:
        """Fetch detailed live score for a specific match from CricAPI."""
        result = empty_match_data()
        result["match_id"] = match_id

        if not self.is_configured():
            result["status"] = "no_api_key"
            return result

        try:
            url = f"{self.CRICAPI_BASE}/match_info?apikey={self.api_key}&id={match_id}"
            resp = requests.get(url, timeout=10)
            resp.raise_for_status()
            data = resp.json()

            if data.get("status") != "success":
                result["status"] = data.get("status", "error")
                return result

            match_data = data.get("data", {})
            result["status"] = match_data.get("status", "unknown")
            result["match_type"] = match_data.get("matchType", "")
            result["venue"] = match_data.get("venue", "")
            result["date"] = match_data.get("date", "")
            result["teams"] = match_data.get("teams", [])
            result["toss"] = (
                f"{match_data.get('tossWinner', '')} elected to "
                f"{match_data.get('tossChoice', '')}"
            )
            result["result"] = match_data.get("status", "")
            result["last_updated"] = datetime.now().isoformat()

            # Score info
            score_list = match_data.get("score", [])
            result["score"] = score_list
            if score_list:
                latest = score_list[0]
                result["current_innings"] = latest.get("inning", "")
                runs = latest.get("r", 0)
                wickets = latest.get("w", 0)
                overs = latest.get("o", 0)
                result["current_score"] = f"{runs}/{wickets} ({overs} ov)"
                if overs and overs > 0:
                    result["current_rr"] = round(runs / overs, 2)

        except requests.exceptions.Timeout:
            result["status"] = "timeout"
        except requests.exceptions.ConnectionError:
            result["status"] = "connection_error"
        except Exception as e:
            result["status"] = f"error: {str(e)}"

        return result

    # ── Fetch detailed scorecard ──────────────────────────────────────────

    def fetch_scorecard(self, match_id: str) -> dict:
        """Fetch full scorecard (batting + bowling) from CricAPI."""
        result = empty_match_data()
        result["match_id"] = match_id

        if not self.is_configured():
            result["status"] = "no_api_key"
            return result

        try:
            url = f"{self.CRICAPI_BASE}/match_scorecard?apikey={self.api_key}&id={match_id}"
            resp = requests.get(url, timeout=10)
            resp.raise_for_status()
            data = resp.json()

            if data.get("status") != "success":
                result["status"] = data.get("status", "error")
                return result

            match_data = data.get("data", {})
            result["status"] = match_data.get("status", "unknown")
            result["match_type"] = match_data.get("matchType", "")
            result["venue"] = match_data.get("venue", "")
            result["teams"] = match_data.get("teams", [])
            result["toss"] = (
                f"{match_data.get('tossWinner', '')} elected to "
                f"{match_data.get('tossChoice', '')}"
            )

            # Score info
            score_list = match_data.get("score", [])
            result["score"] = score_list
            if score_list:
                latest = score_list[0]
                runs = latest.get("r", 0)
                wickets = latest.get("w", 0)
                overs = latest.get("o", 0)
                result["current_innings"] = latest.get("inning", "")
                result["current_score"] = f"{runs}/{wickets} ({overs} ov)"
                if overs and overs > 0:
                    result["current_rr"] = round(runs / overs, 2)

            # Parse scorecard innings
            scorecard = match_data.get("scorecard", [])
            batting_all = []
            bowling_all = []
            for innings_data in scorecard:
                inning_name = innings_data.get("inning", "")
                # Batting
                for b in innings_data.get("batting", []):
                    batting_all.append({
                        "inning": inning_name,
                        "name": b.get("batsman", {}).get("name", ""),
                        "dismissal": b.get("dismissal-text", ""),
                        "runs": b.get("r", 0),
                        "balls": b.get("b", 0),
                        "fours": b.get("4s", 0),
                        "sixes": b.get("6s", 0),
                        "sr": b.get("sr", 0.0),
                    })
                # Bowling
                for bw in innings_data.get("bowling", []):
                    bowling_all.append({
                        "inning": inning_name,
                        "name": bw.get("bowler", {}).get("name", ""),
                        "overs": bw.get("o", 0),
                        "maidens": bw.get("m", 0),
                        "runs": bw.get("r", 0),
                        "wickets": bw.get("w", 0),
                        "economy": bw.get("eco", 0.0),
                    })
            result["batting"] = batting_all
            result["bowling"] = bowling_all
            result["last_updated"] = datetime.now().isoformat()

        except Exception as e:
            result["status"] = f"error: {str(e)}"

        return result

