#!/usr/bin/env python3
"""
Lightweight KPI computation — pure Python/Pandas, no Spark.

Reads all Cricsheet JSON files, flattens ball-by-ball data, computes
the same 30 Gold KPIs that the Spark pipeline does, and writes them
as Parquet files directly into visualization/streamlit/data/.

This is used in GitHub Actions CI where Spark is too heavy to install.
For local dev, prefer the full Spark pipeline (scripts/run_batch.sh).
"""
import json
import os
import sys
from pathlib import Path
import pandas as pd
import numpy as np

RAW_DIR = Path("visualization/streamlit/data/raw_json")
OUT_DIR = Path("visualization/streamlit/data")

# ─── Step 1: Flatten all match JSONs into ball-by-ball DataFrame ──────────────

def flatten_match(fpath: Path) -> list[dict]:
    """Flatten one Cricsheet JSON file into delivery rows."""
    with open(fpath) as f:
        d = json.load(f)
    info = d.get("info", {})
    teams = info.get("teams", [])
    rows = []
    for innings in d.get("innings", []):
        inning_team = innings.get("team", "")
        for over_data in innings.get("overs", []):
            over_num = over_data.get("over", 0)
            for delivery in over_data.get("deliveries", []):
                runs = delivery.get("runs", {})
                wickets = delivery.get("wickets", [])
                wicket_kind = wickets[0].get("kind") if wickets else None
                wicket_player = wickets[0].get("player_out") if wickets else None
                rows.append({
                    "match_id": fpath.stem,
                    "match_type": info.get("match_type", "T20"),
                    "team1": teams[0] if len(teams) > 0 else "",
                    "team2": teams[1] if len(teams) > 1 else "",
                    "venue": info.get("venue", ""),
                    "match_date": str(info.get("dates", [""])[0]),
                    "winner": info.get("outcome", {}).get("winner", ""),
                    "toss_winner": info.get("toss", {}).get("winner", ""),
                    "toss_decision": info.get("toss", {}).get("decision", ""),
                    "inning": inning_team,
                    "over_num": over_num,
                    "batsman": delivery.get("batter", ""),
                    "bowler": delivery.get("bowler", ""),
                    "non_striker": delivery.get("non_striker", ""),
                    "runs_batsman": int(runs.get("batter", 0)),
                    "runs_extras": int(runs.get("extras", 0)),
                    "runs_total": int(runs.get("total", 0)),
                    "wicket_kind": wicket_kind,
                    "wicket_player_out": wicket_player,
                })
    return rows


def load_all_deliveries() -> pd.DataFrame:
    """Load all match JSONs and return a combined DataFrame."""
    all_rows = []
    json_files = sorted(RAW_DIR.glob("*.json"))
    if not json_files:
        print(f"  ⚠️  No JSON files found in {RAW_DIR}")
        sys.exit(1)
    for f in json_files:
        all_rows.extend(flatten_match(f))
    df = pd.DataFrame(all_rows)
    print(f"  Loaded {len(df)} deliveries across {df['match_id'].nunique()} matches")
    return df


def load_matches(df: pd.DataFrame) -> pd.DataFrame:
    """Create a match-level DataFrame from deliveries."""
    return df.groupby("match_id").first().reset_index()[
        ["match_id", "match_type", "team1", "team2", "venue",
         "match_date", "winner", "toss_winner", "toss_decision"]
    ]


# ─── Step 2: Compute each KPI ────────────────────────────────────────────────

def kpi_top_run_scorers(df):
    g = df.groupby("batsman").agg(
        total_runs=("runs_batsman", "sum"),
        balls_faced=("batsman", "count"),
        matches=("match_id", "nunique"),
    ).reset_index().sort_values("total_runs", ascending=False).head(10)
    return g

def kpi_top_wicket_takers(df):
    wk = df[df["wicket_kind"].notna() & ~df["wicket_kind"].isin(["run out", "retired hurt", "obstructing the field"])]
    g = wk.groupby("bowler").agg(
        wickets=("bowler", "count"),
    ).reset_index()
    balls = df.groupby("bowler").agg(balls_bowled=("bowler", "count"), matches=("match_id", "nunique")).reset_index()
    g = g.merge(balls, on="bowler").sort_values("wickets", ascending=False).head(10)
    return g

def kpi_best_batting_average(df):
    innings = df.groupby("batsman").apply(lambda x: x.groupby(["match_id", "inning"]).ngroups).reset_index(name="innings")
    runs = df.groupby("batsman")["runs_batsman"].sum().reset_index(name="total_runs")
    dismissed = df[df["wicket_player_out"] == df["batsman"]].groupby("batsman").apply(
        lambda x: x.groupby(["match_id", "inning"]).ngroups).reset_index(name="dismissals")
    g = runs.merge(innings, on="batsman").merge(dismissed, on="batsman", how="left").fillna({"dismissals": 0})
    g = g[g["innings"] >= 3]
    g["batting_average"] = g.apply(lambda r: round(r["total_runs"] / r["dismissals"], 2) if r["dismissals"] > 0 else r["total_runs"], axis=1)
    return g.sort_values("batting_average", ascending=False).head(20)

def kpi_best_bowling_average(df):
    wk = df[df["wicket_kind"].notna() & ~df["wicket_kind"].isin(["run out", "retired hurt", "obstructing the field"])]
    wickets = wk.groupby("bowler")["bowler"].count().reset_index(name="wickets")
    runs = df.groupby("bowler")["runs_total"].sum().reset_index(name="runs_conceded")
    g = wickets.merge(runs, on="bowler")
    g = g[g["wickets"] >= 3]
    g["bowling_average"] = round(g["runs_conceded"] / g["wickets"], 2)
    return g.sort_values("bowling_average").head(20)

def kpi_best_strike_rate(df):
    g = df.groupby("batsman").agg(total_runs=("runs_batsman", "sum"), balls_faced=("batsman", "count")).reset_index()
    g = g[g["balls_faced"] >= 30]
    g["strike_rate"] = round(g["total_runs"] * 100.0 / g["balls_faced"], 2)
    return g.sort_values("strike_rate", ascending=False).head(20)

def kpi_best_economy_rate(df):
    g = df.groupby("bowler").agg(runs_conceded=("runs_total", "sum"), balls=("bowler", "count")).reset_index()
    g["overs_bowled"] = round(g["balls"] / 6.0, 1)
    g = g[g["overs_bowled"] >= 5]
    g["economy_rate"] = round(g["runs_conceded"] * 6.0 / g["balls"], 2)
    return g.sort_values("economy_rate").head(20)

def kpi_highest_individual_scores(df):
    g = df.groupby(["batsman", "match_id", "inning", "match_type"]).agg(
        score=("runs_batsman", "sum"), balls_faced=("batsman", "count")).reset_index()
    return g.sort_values("score", ascending=False).head(20)

def kpi_most_sixes(df):
    sixes = df[df["runs_batsman"] == 6].groupby("batsman").agg(sixes=("batsman", "count")).reset_index()
    return sixes.sort_values("sixes", ascending=False).head(20)

def kpi_most_fours(df):
    fours = df[df["runs_batsman"] == 4].groupby("batsman").agg(fours=("batsman", "count")).reset_index()
    return fours.sort_values("fours", ascending=False).head(20)

def kpi_powerplay_run_rate(df):
    pp = df[df["over_num"] < 6]
    g = pp.groupby(["inning", "match_type"]).agg(runs=("runs_total", "sum"), balls=("over_num", "count")).reset_index()
    g["powerplay_run_rate"] = round(g["runs"] * 6.0 / g["balls"], 2)
    g = g.rename(columns={"inning": "team"})
    return g[["team", "match_type", "powerplay_run_rate"]].sort_values("powerplay_run_rate", ascending=False)

def kpi_death_over_economy(df):
    death = df[df["over_num"] >= 16]
    g = death.groupby("bowler").agg(runs=("runs_total", "sum"), balls=("bowler", "count")).reset_index()
    g["overs"] = round(g["balls"] / 6.0, 1)
    g = g[g["overs"] >= 2]
    g["death_economy"] = round(g["runs"] * 6.0 / g["balls"], 2)
    return g.sort_values("death_economy").head(20)

def kpi_win_by_toss_decision(df, matches):
    m = matches.copy()
    m["toss_won_match"] = m["toss_winner"] == m["winner"]
    g = m.groupby(["toss_decision", "match_type"]).agg(
        matches=("match_id", "count"),
        wins=("toss_won_match", "sum"),
    ).reset_index()
    g["win_pct"] = round(g["wins"] * 100.0 / g["matches"], 2)
    return g

def kpi_win_batting_first(df, matches):
    # First innings team is whoever batted in inning that matches team1 or team2
    first_innings = df.groupby("match_id").apply(
        lambda x: x.sort_values("over_num")["inning"].iloc[0] if len(x) > 0 else ""
    ).reset_index(name="batting_first")
    m = matches.merge(first_innings, on="match_id")
    m["bat_first_won"] = m["batting_first"] == m["winner"]
    g = m.groupby("match_type").agg(
        matches=("match_id", "count"),
        bat_first_wins=("bat_first_won", "sum"),
    ).reset_index()
    g["batting_first_win_pct"] = round(g["bat_first_wins"] * 100.0 / g["matches"], 2)
    return g

def kpi_avg_score_by_venue(df):
    scores = df.groupby(["match_id", "inning", "venue", "match_type"]).agg(
        innings_total=("runs_total", "sum")).reset_index()
    g = scores.groupby(["venue", "match_type"]).agg(
        avg_score=("innings_total", "mean"), matches=("match_id", "nunique")).reset_index()
    g["avg_score"] = round(g["avg_score"], 2)
    return g.sort_values("avg_score", ascending=False).head(30)

def kpi_highest_team_totals(df):
    g = df.groupby(["match_id", "inning", "match_type", "venue"]).agg(
        team_total=("runs_total", "sum")).reset_index()
    g = g.rename(columns={"inning": "team"})
    return g.sort_values("team_total", ascending=False).head(20)

def kpi_lowest_successful_chases(df, matches):
    # Get 2nd innings totals where the chasing team won
    second = df.copy()
    first_inning = df.groupby("match_id").apply(lambda x: x.sort_values("over_num")["inning"].iloc[0]).reset_index(name="first_inning_team")
    second = second.merge(first_inning, on="match_id")
    second = second[second["inning"] != second["first_inning_team"]]
    chase = second.groupby(["match_id", "inning", "match_type", "venue"]).agg(chase_total=("runs_total", "sum")).reset_index()
    chase = chase.merge(matches[["match_id", "winner"]], on="match_id")
    chase = chase[chase["inning"] == chase["winner"]]
    chase = chase.rename(columns={"inning": "team"})
    return chase.sort_values("chase_total").head(20)

def kpi_most_wins_by_team(matches):
    m = matches[matches["winner"] != ""]
    g = m.groupby(["winner", "match_type"]).agg(wins=("match_id", "count")).reset_index()
    g = g.rename(columns={"winner": "team"})
    return g.sort_values("wins", ascending=False)

def kpi_player_by_match_type(df):
    g = df.groupby(["batsman", "match_type"]).agg(
        total_runs=("runs_batsman", "sum"),
        balls_faced=("batsman", "count"),
    ).reset_index()
    g["strike_rate"] = round(g["total_runs"] * 100.0 / g["balls_faced"], 2)
    return g.sort_values("total_runs", ascending=False).head(30)

def kpi_partnership_analysis(df):
    g = df.groupby(["batsman", "non_striker", "match_id", "inning", "match_type"]).agg(
        partnership_runs=("runs_total", "sum"), balls=("batsman", "count")).reset_index()
    return g.sort_values("partnership_runs", ascending=False).head(20)

def kpi_dot_ball_pct(df):
    g = df.groupby("bowler").agg(
        balls_bowled=("bowler", "count"),
        dot_balls=("runs_total", lambda x: (x == 0).sum()),
    ).reset_index()
    g = g[g["balls_bowled"] >= 10]
    g["dot_ball_pct"] = round(g["dot_balls"] * 100.0 / g["balls_bowled"], 2)
    return g.sort_values("dot_ball_pct", ascending=False).head(20)

def kpi_boundary_pct_per_phase(df):
    df = df.copy()
    df["phase"] = df["over_num"].apply(lambda o: "Powerplay" if o < 6 else ("Death" if o >= 16 else "Middle"))
    df["is_boundary"] = df["runs_batsman"].isin([4, 6]).astype(int)
    g = df.groupby(["phase", "match_type"]).agg(
        total_balls=("batsman", "count"),
        boundaries=("is_boundary", "sum"),
    ).reset_index()
    g["boundary_pct"] = round(g["boundaries"] * 100.0 / g["total_balls"], 2)
    return g

def kpi_avg_runs_per_wicket(df):
    wk = df[df["wicket_kind"].notna()]
    scores = df.groupby(["match_id", "inning", "match_type"]).agg(total_runs=("runs_total", "sum")).reset_index()
    wickets = wk.groupby(["match_id", "inning", "match_type"]).agg(total_wickets=("wicket_kind", "count")).reset_index()
    g = scores.merge(wickets, on=["match_id", "inning", "match_type"])
    g["runs_per_wicket"] = round(g["total_runs"] / g["total_wickets"], 2)
    agg = g.groupby(["inning", "match_type"]).agg(avg_runs_per_wicket=("runs_per_wicket", "mean")).reset_index()
    agg["avg_runs_per_wicket"] = round(agg["avg_runs_per_wicket"], 2)
    return agg

def kpi_pressure_index_per_over(df):
    """Pressure = wickets × 10 + (6 − runs_in_over). Per over per match."""
    g = df.groupby(["match_id", "inning", "over_num", "match_type"]).agg(
        runs=("runs_total", "sum"),
        wickets=("wicket_kind", lambda x: x.notna().sum()),
    ).reset_index()
    g["pressure_index"] = g["wickets"] * 10 + (6 - g["runs"]).clip(lower=0)
    return g

def kpi_run_rate_progression(df):
    g = df.groupby(["over_num", "match_type"]).agg(
        total_runs=("runs_total", "sum"),
        total_overs=("match_id", "nunique"),  # approximate
    ).reset_index()
    g["avg_runs_per_over"] = round(g["total_runs"] / g["total_overs"], 2)
    # Cumulative avg run rate
    g = g.sort_values("over_num")
    g["cumulative_runs"] = g.groupby("match_type")["total_runs"].cumsum()
    g["cumulative_overs"] = g.groupby("match_type")["total_overs"].cumsum()
    g["avg_run_rate"] = round(g["cumulative_runs"] / g["cumulative_overs"], 2)
    return g[["match_type", "over_num", "avg_runs_per_over", "avg_run_rate"]]

def kpi_extras_analysis(df):
    g = df.groupby(["inning", "match_type"]).agg(
        total_extras=("runs_extras", "sum"),
        matches=("match_id", "nunique"),
    ).reset_index()
    g["avg_extras_per_match"] = round(g["total_extras"] / g["matches"], 2)
    g = g.rename(columns={"inning": "team"})
    return g.sort_values("avg_extras_per_match", ascending=False)

def kpi_home_away_win_pct(df, matches):
    # Simple approximation: if venue country matches team, it's "home"
    m = matches[matches["winner"] != ""].copy()
    # T20 WC 2026 is in India/Sri Lanka — teams playing there are "away" unless India/SL
    m["home_away"] = m.apply(
        lambda r: "Home" if r["winner"] in r["venue"] or
                  (r["winner"] in ["India", "Sri Lanka"]) else "Away", axis=1)
    g = m.groupby(["winner", "match_type", "home_away"]).agg(wins=("match_id", "count")).reset_index()
    g = g.rename(columns={"winner": "team"})
    return g.sort_values("wins", ascending=False).head(30)

def kpi_head_to_head(matches):
    m = matches[matches["winner"] != ""].copy()
    m["team_pair"] = m.apply(lambda r: tuple(sorted([r["team1"], r["team2"]])), axis=1)
    g = m.groupby("team_pair").agg(matches_played=("match_id", "count")).reset_index()
    g["team1"] = g["team_pair"].apply(lambda x: x[0])
    g["team2"] = g["team_pair"].apply(lambda x: x[1])
    # Count wins per team
    for _, row in g.iterrows():
        t1, t2 = row["team1"], row["team2"]
        subset = m[m["team_pair"] == row["team_pair"]]
        g.loc[g["team_pair"] == row["team_pair"], "team1_wins"] = (subset["winner"] == t1).sum()
        g.loc[g["team_pair"] == row["team_pair"], "team2_wins"] = (subset["winner"] == t2).sum()
        g.loc[g["team_pair"] == row["team_pair"], "no_result"] = 0
    g["team1_wins"] = g["team1_wins"].astype(int)
    g["team2_wins"] = g["team2_wins"].astype(int)
    g["no_result"] = g["no_result"].astype(int)
    return g[["team1", "team2", "matches_played", "team1_wins", "team2_wins", "no_result"]].sort_values("matches_played", ascending=False).head(30)

def kpi_player_consistency(df):
    innings_scores = df.groupby(["batsman", "match_id", "inning", "match_type"]).agg(
        innings_score=("runs_batsman", "sum")).reset_index()
    g = innings_scores.groupby(["batsman", "match_type"]).agg(
        innings=("innings_score", "count"),
        avg_score=("innings_score", "mean"),
        score_stddev=("innings_score", "std"),
    ).reset_index()
    g = g[g["innings"] >= 3].fillna({"score_stddev": 0})
    g["avg_score"] = round(g["avg_score"], 2)
    g["score_stddev"] = round(g["score_stddev"], 2)
    return g.sort_values("score_stddev").head(20)

def kpi_best_bowling_spells(df):
    df = df.copy()
    df["over_group"] = (df["over_num"] // 5).astype(int)
    wk = df.copy()
    wk["is_wicket"] = wk["wicket_kind"].notna() & ~wk["wicket_kind"].isin(["run out", "retired hurt"])
    g = wk.groupby(["bowler", "match_id", "inning", "match_type", "over_group"]).agg(
        spell_start_over=("over_num", "min"),
        spell_end_over=("over_num", "max"),
        runs_conceded=("runs_total", "sum"),
        wickets=("is_wicket", "sum"),
        balls=("bowler", "count"),
    ).reset_index()
    g = g[g["wickets"] >= 3]
    return g.sort_values(["wickets", "runs_conceded"], ascending=[False, True]).head(20)

def kpi_win_contribution(df, matches):
    merged = df.merge(matches[["match_id", "winner"]], on="match_id", suffixes=("", "_match"))
    winner_col = "winner_match" if "winner_match" in merged.columns else "winner"
    merged["in_winning_team"] = merged["inning"] == merged[winner_col]
    g = merged.groupby(["batsman", "match_type"]).agg(
        matches=("match_id", "nunique"),
        total_runs=("runs_batsman", "sum"),
    ).reset_index()
    # Calculate winning runs separately
    winning = merged[merged["in_winning_team"]].groupby(["batsman", "match_type"]).agg(
        winning_runs=("runs_batsman", "sum")).reset_index()
    g = g.merge(winning, on=["batsman", "match_type"], how="left").fillna({"winning_runs": 0})
    g["winning_runs"] = g["winning_runs"].astype(int)
    g = g[g["total_runs"] >= 30]
    g["win_contribution_pct"] = round(g["winning_runs"] * 100.0 / g["total_runs"].replace(0, np.nan), 2)
    return g.sort_values("win_contribution_pct", ascending=False).head(20)


# ─── Main ─────────────────────────────────────────────────────────────────────

def save(name: str, df: pd.DataFrame):
    path = OUT_DIR / f"{name}.parquet"
    df.to_parquet(path, index=False)
    print(f"  OK  {name}: {len(df)} rows")


def main():
    print("━━━ Lightweight KPI Computation (Python-only) ━━━")
    print(f"  Source: {RAW_DIR}")
    print(f"  Output: {OUT_DIR}")
    print()

    df = load_all_deliveries()
    matches = load_matches(df)
    print()

    save("top_run_scorers", kpi_top_run_scorers(df))
    save("top_wicket_takers", kpi_top_wicket_takers(df))
    save("best_batting_average", kpi_best_batting_average(df))
    save("best_bowling_average", kpi_best_bowling_average(df))
    save("best_strike_rate", kpi_best_strike_rate(df))
    save("best_economy_rate", kpi_best_economy_rate(df))
    save("highest_individual_scores", kpi_highest_individual_scores(df))
    save("most_sixes", kpi_most_sixes(df))
    save("most_fours", kpi_most_fours(df))
    save("powerplay_run_rate", kpi_powerplay_run_rate(df))
    save("death_over_economy", kpi_death_over_economy(df))
    save("win_by_toss_decision", kpi_win_by_toss_decision(df, matches))
    save("win_batting_first", kpi_win_batting_first(df, matches))
    save("avg_score_by_venue", kpi_avg_score_by_venue(df))
    save("highest_team_totals", kpi_highest_team_totals(df))
    save("lowest_successful_chases", kpi_lowest_successful_chases(df, matches))
    save("most_wins_by_team", kpi_most_wins_by_team(matches))
    save("player_by_match_type", kpi_player_by_match_type(df))
    save("partnership_analysis", kpi_partnership_analysis(df))
    save("dot_ball_pct", kpi_dot_ball_pct(df))
    save("boundary_pct_per_phase", kpi_boundary_pct_per_phase(df))
    save("avg_runs_per_wicket", kpi_avg_runs_per_wicket(df))
    save("pressure_index_per_over", kpi_pressure_index_per_over(df))
    save("run_rate_progression", kpi_run_rate_progression(df))
    save("extras_analysis", kpi_extras_analysis(df))
    save("home_away_win_pct", kpi_home_away_win_pct(df, matches))
    save("head_to_head", kpi_head_to_head(matches))
    save("player_consistency", kpi_player_consistency(df))
    save("best_bowling_spells", kpi_best_bowling_spells(df))
    save("win_contribution", kpi_win_contribution(df, matches))

    print(f"\n✅ All 30 KPIs computed and saved to {OUT_DIR}")


if __name__ == "__main__":
    main()


