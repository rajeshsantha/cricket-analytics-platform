#!/usr/bin/env python3
"""
Cricket Analytics Platform — Live Score CLI
Author: Rajesh Santha

CLI tool to fetch live cricket scores from CricAPI.

Usage:
    # List current live matches
    python3 scripts/live_score.py --list

    # Get live score for a specific match
    python3 scripts/live_score.py --match-id abc123

    # Watch mode: auto-refresh every 10 seconds
    python3 scripts/live_score.py --match-id abc123 --watch --interval 10

Environment:
    CRICAPI_KEY  — Your CricAPI API key (required)
"""

import argparse
import json
import os
import sys
import time

# Add the streamlit directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "visualization", "streamlit"))
from components.live_score_poller import LiveScorePoller


def print_match_list(poller: LiveScorePoller):
    """Print list of current live/recent matches."""
    matches = poller.fetch_current_matches()
    if not matches:
        print("  No live/recent matches found.")
        return

    print(f"\n{'─' * 80}")
    print(f"  {'ID':<40} {'Match':<30} {'Status':<10}")
    print(f"{'─' * 80}")
    for m in matches:
        teams = " vs ".join(m.get("teams", []))[:28]
        status = "🟢 LIVE" if m.get("matchStarted") and not m.get("matchEnded") else (
            "✅ Done" if m.get("matchEnded") else "⏳ Soon"
        )
        mid = m.get("id", "")[:38]
        print(f"  {mid:<40} {teams:<30} {status}")

        # Print scores
        for s in m.get("score", []):
            inning = s.get("inning", "")[:35]
            runs = s.get("r", 0)
            wickets = s.get("w", 0)
            overs = s.get("o", 0)
            print(f"    → {inning}: {runs}/{wickets} ({overs} ov)")
    print(f"{'─' * 80}\n")


def print_live_score(poller: LiveScorePoller, match_id: str):
    """Print live score for a specific match."""
    data = poller.fetch_scorecard(match_id)

    if data["status"] in ("no_api_key",):
        print("❌ No API key configured. Set CRICAPI_KEY environment variable.")
        return
    if "error" in str(data["status"]):
        print(f"❌ Error: {data['status']}")
        return

    teams = data.get("teams", [])
    teams_str = " vs ".join(teams) if teams else "Unknown"

    print(f"\n{'═' * 70}")
    print(f"  🏏  {teams_str}")
    print(f"{'═' * 70}")

    # Status
    status = data.get("result") or data.get("status", "")
    print(f"  Status: {status}")

    # Venue & Toss
    if data.get("venue"):
        print(f"  Venue:  {data['venue']}")
    if data.get("toss"):
        print(f"  Toss:   {data['toss']}")

    # Scores
    print(f"\n{'─' * 70}")
    for s in data.get("score", []):
        inning = s.get("inning", "")
        runs = s.get("r", 0)
        wickets = s.get("w", 0)
        overs = s.get("o", 0)
        rr = round(runs / overs, 2) if overs else 0
        print(f"  {inning}: {runs}/{wickets} ({overs} ov)  RR: {rr}")

    # Batting
    batting = data.get("batting", [])
    if batting:
        innings_seen = set()
        for b in batting:
            inn = b.get("inning", "")
            if inn not in innings_seen:
                innings_seen.add(inn)
                print(f"\n{'─' * 70}")
                print(f"  BATTING — {inn}")
                print(f"  {'Batsman':<25} {'R':>5} {'B':>5} {'4s':>4} {'6s':>4} {'SR':>7}  Dismissal")
                print(f"  {'─' * 65}")
            print(f"  {b['name']:<25} {b['runs']:>5} {b['balls']:>5} {b['fours']:>4} {b['sixes']:>4} {b['sr']:>7.1f}  {b['dismissal']}")

    # Bowling
    bowling = data.get("bowling", [])
    if bowling:
        innings_seen = set()
        for bw in bowling:
            inn = bw.get("inning", "")
            if inn not in innings_seen:
                innings_seen.add(inn)
                print(f"\n{'─' * 70}")
                print(f"  BOWLING — {inn}")
                print(f"  {'Bowler':<25} {'O':>5} {'M':>4} {'R':>5} {'W':>4} {'Econ':>6}")
                print(f"  {'─' * 55}")
            print(f"  {bw['name']:<25} {bw['overs']:>5} {bw['maidens']:>4} {bw['runs']:>5} {bw['wickets']:>4} {bw['economy']:>6.1f}")

    print(f"\n{'═' * 70}")
    print(f"  Last updated: {data.get('last_updated', 'N/A')}")
    print(f"  Match ID:     {match_id}")
    print(f"{'═' * 70}\n")


def main():
    parser = argparse.ArgumentParser(
        description="🏏 Live Cricket Score CLI — powered by CricAPI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # List current matches
  python3 scripts/live_score.py --list

  # Show live score
  python3 scripts/live_score.py --match-id d9032b36-d872-4011-b96c-73a9137e7ced

  # Watch mode (auto-refresh)
  python3 scripts/live_score.py --match-id abc123 --watch --interval 15

  # Output as JSON
  python3 scripts/live_score.py --match-id abc123 --json
        """,
    )
    parser.add_argument("--list", action="store_true", help="List current live matches")
    parser.add_argument("--match-id", type=str, help="CricAPI match ID to fetch score for")
    parser.add_argument("--watch", action="store_true", help="Auto-refresh the score")
    parser.add_argument("--interval", type=int, default=10, help="Refresh interval in seconds (default: 10)")
    parser.add_argument("--json", action="store_true", help="Output raw JSON")
    parser.add_argument("--api-key", type=str, default="", help="CricAPI key (or set CRICAPI_KEY env var)")
    args = parser.parse_args()

    api_key = args.api_key or os.environ.get("CRICAPI_KEY", "")
    if not api_key:
        print("❌ No API key. Set CRICAPI_KEY env var or use --api-key flag.")
        print("   Get a free key at https://cricapi.com")
        sys.exit(1)

    poller = LiveScorePoller(api_key=api_key)

    if args.list:
        print("\n🏏 Current / Recent Matches:")
        print_match_list(poller)
        return

    if not args.match_id:
        print("❌ Please provide --match-id or use --list to browse matches.")
        parser.print_help()
        sys.exit(1)

    if args.json:
        data = poller.fetch_scorecard(args.match_id)
        print(json.dumps(data, indent=2, default=str))
        return

    if args.watch:
        print(f"🔄 Watch mode: refreshing every {args.interval} seconds (Ctrl+C to stop)\n")
        try:
            while True:
                os.system("clear" if os.name != "nt" else "cls")
                print_live_score(poller, args.match_id)
                print(f"  ⏱  Next refresh in {args.interval}s… (Ctrl+C to stop)")
                time.sleep(args.interval)
        except KeyboardInterrupt:
            print("\n👋 Stopped watching.")
    else:
        print_live_score(poller, args.match_id)


if __name__ == "__main__":
    main()

