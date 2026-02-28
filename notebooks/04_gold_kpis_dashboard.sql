-- Notebook: 04 - Gold KPIs Dashboard Queries
-- Power BI / Streamlit-ready SQL queries against Gold Delta tables
-- Optimized for dashboard performance with aggregations pre-computed

-- ─── Live Scorecard (refresh every 5s) ───────────────────────────────────────
-- Reads from Gold live_kpis table

SELECT
  match_id,
  window_start,
  window_end,
  window_runs,
  balls_bowled,
  wickets_in_window,
  ROUND(run_rate, 2)              AS current_run_rate,
  ROUND(batting_strike_rate, 2)   AS strike_rate,
  ROUND(bowler_economy, 2)        AS economy_rate,
  current_batsman,
  current_bowler,
  computed_at
FROM delta.`/tmp/cricket-delta/gold/live_kpis`
WHERE computed_at >= NOW() - INTERVAL 1 MINUTE
ORDER BY computed_at DESC
LIMIT 1;

-- ─── Run Rate Trend (last 10 overs) ──────────────────────────────────────────
SELECT
  window_start,
  ROUND(run_rate, 2) AS run_rate,
  wickets_in_window
FROM delta.`/tmp/cricket-delta/gold/live_kpis`
WHERE match_id = :match_id
ORDER BY window_start DESC
LIMIT 10;

-- ─── Top 10 Batsmen Dashboard ────────────────────────────────────────────────
SELECT batsman, total_runs, balls_faced, matches
FROM delta.`/tmp/cricket-delta/gold/batch_kpis/top_run_scorers`
ORDER BY total_runs DESC
LIMIT 10;

-- ─── Top 10 Bowlers Dashboard ────────────────────────────────────────────────
SELECT bowler, wickets, balls_bowled, matches
FROM delta.`/tmp/cricket-delta/gold/batch_kpis/top_wicket_takers`
ORDER BY wickets DESC
LIMIT 10;

-- ─── Pressure Index Over Time ────────────────────────────────────────────────
SELECT over_num, pressure_index, pressure_level, runs_in_over, wickets_in_over
FROM delta.`/tmp/cricket-delta/gold/batch_kpis/pressure_index_per_over`
WHERE match_id = :match_id AND inning = :inning
ORDER BY over_num;

-- ─── Run Rate Progression Chart ──────────────────────────────────────────────
SELECT match_type, over_num, avg_runs_per_over, avg_run_rate
FROM delta.`/tmp/cricket-delta/gold/batch_kpis/run_rate_progression`
WHERE match_type IN ('T20', 'ODI')
ORDER BY match_type, over_num;

-- ─── Batting by Phase ────────────────────────────────────────────────────────
SELECT match_type, phase, total_balls, boundaries, boundary_pct
FROM delta.`/tmp/cricket-delta/gold/batch_kpis/boundary_pct_per_phase`
ORDER BY match_type, phase;

-- ─── Win % by Toss Decision ──────────────────────────────────────────────────
SELECT toss_decision, match_type, total_matches, win_pct
FROM delta.`/tmp/cricket-delta/gold/batch_kpis/win_by_toss_decision`
ORDER BY match_type, win_pct DESC;

-- ─── Head-to-Head Summary ────────────────────────────────────────────────────
SELECT team1, team2, matches_played, team1_wins, team2_wins, no_result
FROM delta.`/tmp/cricket-delta/gold/batch_kpis/head_to_head`
WHERE match_type = :match_type
ORDER BY matches_played DESC
LIMIT 20;

-- ─── Top Partnerships ────────────────────────────────────────────────────────
SELECT batsman, non_striker, partnership_runs, balls_faced
FROM delta.`/tmp/cricket-delta/gold/batch_kpis/partnership_analysis`
ORDER BY partnership_runs DESC
LIMIT 20;
