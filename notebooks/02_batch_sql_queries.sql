-- Notebook: 02 - Batch SQL Queries
-- All 30+ KPI SQL queries from GoldBatchKPIs as standalone SQL
-- Assumes tables: silver_deliveries, silver_matches
-- Run in Spark SQL or Databricks notebook

-- ─── KPI 1: Top 10 run scorers all time ─────────────────────────────────────
SELECT batsman, SUM(runs_batsman) AS total_runs,
       COUNT(*) AS balls_faced,
       COUNT(DISTINCT match_id) AS matches
FROM silver_deliveries
GROUP BY batsman
ORDER BY total_runs DESC
LIMIT 10;

-- ─── KPI 2: Top 10 wicket takers all time ───────────────────────────────────
SELECT bowler,
       SUM(CASE WHEN wicket_kind IS NOT NULL
                 AND wicket_kind NOT IN ('run out','retired hurt','obstructing the field')
                THEN 1 ELSE 0 END) AS wickets,
       COUNT(*) AS balls_bowled,
       COUNT(DISTINCT match_id) AS matches
FROM silver_deliveries
GROUP BY bowler
ORDER BY wickets DESC
LIMIT 10;

-- ─── KPI 3: Best batting average (min 20 innings) ───────────────────────────
SELECT batsman,
       SUM(runs_batsman) AS total_runs,
       COUNT(DISTINCT match_id || '_' || inning) AS innings,
       ROUND(SUM(runs_batsman) /
         NULLIF(COUNT(DISTINCT CASE WHEN wicket_player_out = batsman
                THEN match_id || '_' || inning END), 0), 2) AS batting_average
FROM silver_deliveries
GROUP BY batsman
HAVING COUNT(DISTINCT match_id || '_' || inning) >= 20
ORDER BY batting_average DESC
LIMIT 20;

-- ─── KPI 4: Best bowling average (min 50 wickets) ───────────────────────────
SELECT bowler,
       SUM(runs_total) AS runs_conceded,
       SUM(CASE WHEN wicket_kind IS NOT NULL
                 AND wicket_kind NOT IN ('run out','retired hurt','obstructing the field')
                THEN 1 ELSE 0 END) AS wickets,
       ROUND(SUM(runs_total) / NULLIF(
         SUM(CASE WHEN wicket_kind IS NOT NULL
                   AND wicket_kind NOT IN ('run out','retired hurt','obstructing the field')
                  THEN 1 ELSE 0 END), 0), 2) AS bowling_average
FROM silver_deliveries
GROUP BY bowler
HAVING SUM(CASE WHEN wicket_kind IS NOT NULL
                 AND wicket_kind NOT IN ('run out','retired hurt','obstructing the field')
                THEN 1 ELSE 0 END) >= 50
ORDER BY bowling_average ASC
LIMIT 20;

-- ─── KPI 5: Best strike rate (min 500 balls faced) ──────────────────────────
SELECT batsman,
       SUM(runs_batsman) AS total_runs,
       COUNT(*) AS balls_faced,
       ROUND(SUM(runs_batsman) * 100.0 / COUNT(*), 2) AS strike_rate
FROM silver_deliveries
GROUP BY batsman
HAVING COUNT(*) >= 500
ORDER BY strike_rate DESC
LIMIT 20;

-- ─── KPI 6: Best economy rate (min 100 overs) ───────────────────────────────
SELECT bowler,
       SUM(runs_total) AS runs_conceded,
       ROUND(COUNT(*) / 6.0, 1) AS overs_bowled,
       ROUND(SUM(runs_total) * 6.0 / COUNT(*), 2) AS economy_rate
FROM silver_deliveries
GROUP BY bowler
HAVING ROUND(COUNT(*) / 6.0, 1) >= 100
ORDER BY economy_rate ASC
LIMIT 20;

-- ─── KPI 7: Highest individual scores ───────────────────────────────────────
SELECT batsman, match_id, match_type,
       SUM(runs_batsman) AS score,
       COUNT(*) AS balls_faced
FROM silver_deliveries
GROUP BY batsman, match_id, match_type
ORDER BY score DESC
LIMIT 20;

-- ─── KPI 8: Most sixes hit ───────────────────────────────────────────────────
SELECT batsman,
       SUM(CASE WHEN runs_batsman = 6 THEN 1 ELSE 0 END) AS sixes
FROM silver_deliveries
GROUP BY batsman
ORDER BY sixes DESC
LIMIT 20;

-- ─── KPI 9: Most fours hit ───────────────────────────────────────────────────
SELECT batsman,
       SUM(CASE WHEN runs_batsman = 4 THEN 1 ELSE 0 END) AS fours
FROM silver_deliveries
GROUP BY batsman
ORDER BY fours DESC
LIMIT 20;

-- ─── KPI 10: Best powerplay run rate by team (overs 0-5) ────────────────────
SELECT inning AS team, match_type,
       ROUND(SUM(runs_total) * 6.0 / COUNT(*), 2) AS powerplay_run_rate,
       COUNT(DISTINCT match_id) AS matches
FROM silver_deliveries
WHERE over_num < 6
GROUP BY inning, match_type
ORDER BY powerplay_run_rate DESC
LIMIT 20;

-- ─── KPI 11: Best death over economy by bowler (overs 15-19 in T20) ─────────
SELECT bowler, match_type,
       ROUND(SUM(runs_total) * 6.0 / COUNT(*), 2) AS death_economy,
       COUNT(DISTINCT match_id) AS matches,
       ROUND(COUNT(*) / 6.0, 1) AS overs_bowled
FROM silver_deliveries
WHERE over_num >= 15 AND match_type = 'T20'
GROUP BY bowler, match_type
HAVING ROUND(COUNT(*) / 6.0, 1) >= 10
ORDER BY death_economy ASC
LIMIT 20;

-- ─── KPI 12: Win % by toss decision (bat/field) ──────────────────────────────
SELECT toss_decision, match_type,
       COUNT(DISTINCT match_id) AS total_matches,
       SUM(CASE WHEN toss_winner = winner THEN 1 ELSE 0 END) AS toss_winner_wins,
       ROUND(SUM(CASE WHEN toss_winner = winner THEN 1 ELSE 0 END) * 100.0 / COUNT(DISTINCT match_id), 2) AS win_pct
FROM silver_matches
WHERE toss_decision IS NOT NULL AND winner IS NOT NULL
GROUP BY toss_decision, match_type
ORDER BY win_pct DESC;

-- ─── KPI 13: Win % batting first vs chasing ──────────────────────────────────
SELECT match_type,
       SUM(CASE WHEN (toss_decision = 'bat' AND toss_winner = winner)
                  OR (toss_decision = 'field' AND toss_winner != winner AND winner IS NOT NULL)
                THEN 1 ELSE 0 END) AS batting_first_wins,
       COUNT(DISTINCT match_id) AS total_matches,
       ROUND(SUM(CASE WHEN (toss_decision = 'bat' AND toss_winner = winner)
                        OR (toss_decision = 'field' AND toss_winner != winner AND winner IS NOT NULL)
                      THEN 1 ELSE 0 END) * 100.0 / COUNT(DISTINCT match_id), 2) AS batting_first_win_pct
FROM silver_matches
WHERE winner IS NOT NULL
GROUP BY match_type;

-- ─── KPI 14: Average score by venue ──────────────────────────────────────────
SELECT venue, match_type, inning,
       ROUND(AVG(innings_total), 2) AS avg_score
FROM (
  SELECT venue, match_type, match_id, inning, SUM(runs_total) AS innings_total
  FROM silver_deliveries
  GROUP BY venue, match_type, match_id, inning
) t
GROUP BY venue, match_type, inning
ORDER BY avg_score DESC
LIMIT 30;

-- ─── KPI 15: Highest team totals ─────────────────────────────────────────────
SELECT inning AS team, match_id, match_type, venue,
       SUM(runs_total) AS team_total
FROM silver_deliveries
GROUP BY inning, match_id, match_type, venue
ORDER BY team_total DESC
LIMIT 20;

-- ─── KPI 16: Lowest successful chases ────────────────────────────────────────
SELECT d.inning AS team, d.match_id, m.match_type, m.venue,
       SUM(d.runs_total) AS chase_total
FROM silver_deliveries d
JOIN silver_matches m ON d.match_id = m.match_id
WHERE d.inning NOT LIKE '%1st%'
  AND m.winner = d.inning
GROUP BY d.inning, d.match_id, m.match_type, m.venue
ORDER BY chase_total ASC
LIMIT 20;

-- ─── KPI 17: Most matches won by team ────────────────────────────────────────
SELECT winner AS team, match_type, COUNT(*) AS wins
FROM silver_matches
WHERE winner IS NOT NULL
GROUP BY winner, match_type
ORDER BY wins DESC
LIMIT 20;

-- ─── KPI 18: Player performance by match type (T20/ODI/Test) ─────────────────
SELECT batsman, match_type,
       SUM(runs_batsman) AS total_runs,
       COUNT(*) AS balls_faced,
       ROUND(SUM(runs_batsman) * 100.0 / COUNT(*), 2) AS strike_rate,
       COUNT(DISTINCT match_id) AS matches
FROM silver_deliveries
GROUP BY batsman, match_type
ORDER BY total_runs DESC
LIMIT 30;

-- ─── KPI 19: Partnership analysis (top partnerships) ─────────────────────────
SELECT batsman, non_striker, match_id, inning,
       SUM(runs_total) AS partnership_runs,
       COUNT(*) AS balls_faced
FROM silver_deliveries
GROUP BY batsman, non_striker, match_id, inning
ORDER BY partnership_runs DESC
LIMIT 20;

-- ─── KPI 20: Dot ball % by bowler ────────────────────────────────────────────
SELECT bowler,
       COUNT(*) AS balls_bowled,
       SUM(CASE WHEN runs_total = 0 THEN 1 ELSE 0 END) AS dot_balls,
       ROUND(SUM(CASE WHEN runs_total = 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS dot_ball_pct
FROM silver_deliveries
GROUP BY bowler
HAVING COUNT(*) >= 100
ORDER BY dot_ball_pct DESC
LIMIT 20;

-- ─── KPI 21: Boundary % per over phase (powerplay/middle/death) ──────────────
SELECT match_type,
       CASE WHEN over_num < 6  THEN 'Powerplay'
            WHEN over_num < 15 THEN 'Middle'
            ELSE 'Death'
       END AS phase,
       COUNT(*) AS total_balls,
       SUM(CASE WHEN runs_batsman IN (4,6) THEN 1 ELSE 0 END) AS boundaries,
       ROUND(SUM(CASE WHEN runs_batsman IN (4,6) THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS boundary_pct
FROM silver_deliveries
GROUP BY match_type,
         CASE WHEN over_num < 6 THEN 'Powerplay' WHEN over_num < 15 THEN 'Middle' ELSE 'Death' END
ORDER BY match_type, phase;

-- ─── KPI 22: Average runs per wicket by innings ───────────────────────────────
SELECT match_type, inning,
       ROUND(SUM(runs_total) / NULLIF(SUM(CASE WHEN wicket_kind IS NOT NULL THEN 1 ELSE 0 END), 0), 2) AS avg_runs_per_wicket
FROM silver_deliveries
GROUP BY match_type, inning
ORDER BY match_type, inning;

-- ─── KPI 23: Pressure index per over ─────────────────────────────────────────
SELECT match_id, inning, over_num,
       SUM(runs_total) AS runs_in_over,
       SUM(CASE WHEN wicket_kind IS NOT NULL THEN 1 ELSE 0 END) AS wickets_in_over,
       ROUND(SUM(runs_total) * 6.0 / COUNT(*), 2) AS over_run_rate,
       ROUND(SUM(CASE WHEN wicket_kind IS NOT NULL THEN 1 ELSE 0 END) * 10.0 + (6 - SUM(runs_total)), 2) AS pressure_index
FROM silver_deliveries
GROUP BY match_id, inning, over_num
ORDER BY match_id, inning, over_num;

-- ─── KPI 24: Run rate progression (over by over average) ─────────────────────
SELECT match_type, over_num,
       ROUND(AVG(over_runs), 2) AS avg_runs_per_over,
       ROUND(AVG(over_runs) * 6.0 / 6, 2) AS avg_run_rate
FROM (
  SELECT match_type, match_id, inning, over_num, SUM(runs_total) AS over_runs
  FROM silver_deliveries
  GROUP BY match_type, match_id, inning, over_num
) t
GROUP BY match_type, over_num
ORDER BY match_type, over_num;

-- ─── KPI 25: Extras analysis by team ─────────────────────────────────────────
SELECT inning AS team, match_type,
       SUM(runs_extras) AS total_extras,
       COUNT(DISTINCT match_id) AS matches,
       ROUND(SUM(runs_extras) * 1.0 / COUNT(DISTINCT match_id), 2) AS avg_extras_per_match
FROM silver_deliveries
GROUP BY inning, match_type
ORDER BY avg_extras_per_match DESC
LIMIT 20;

-- ─── KPI 26: Home vs away win percentage ─────────────────────────────────────
SELECT winner, venue, match_type,
       COUNT(*) AS wins,
       CASE WHEN venue LIKE CONCAT('%', SPLIT(winner, ' ')[0], '%') THEN 'Home' ELSE 'Away' END AS home_away
FROM silver_matches
WHERE winner IS NOT NULL
GROUP BY winner, venue, match_type,
         CASE WHEN venue LIKE CONCAT('%', SPLIT(winner, ' ')[0], '%') THEN 'Home' ELSE 'Away' END
ORDER BY wins DESC
LIMIT 30;

-- ─── KPI 27: Head-to-head team records ───────────────────────────────────────
SELECT team1, team2, match_type,
       COUNT(*) AS matches_played,
       SUM(CASE WHEN winner = team1 THEN 1 ELSE 0 END) AS team1_wins,
       SUM(CASE WHEN winner = team2 THEN 1 ELSE 0 END) AS team2_wins,
       SUM(CASE WHEN winner IS NULL THEN 1 ELSE 0 END) AS no_result
FROM silver_matches
GROUP BY team1, team2, match_type
ORDER BY matches_played DESC
LIMIT 30;

-- ─── KPI 28: Player consistency (std dev of scores) ──────────────────────────
SELECT batsman, match_type,
       COUNT(DISTINCT match_id || '_' || inning) AS innings,
       ROUND(AVG(innings_score), 2) AS avg_score,
       ROUND(STDDEV(innings_score), 2) AS score_stddev
FROM (
  SELECT batsman, match_id, inning, match_type, SUM(runs_batsman) AS innings_score
  FROM silver_deliveries
  GROUP BY batsman, match_id, inning, match_type
) t
GROUP BY batsman, match_type
HAVING COUNT(DISTINCT match_id || '_' || inning) >= 20
ORDER BY score_stddev ASC
LIMIT 20;

-- ─── KPI 29: Best bowling spells (5-over window) ─────────────────────────────
SELECT bowler, match_id, inning, match_type,
       MIN(over_num) AS spell_start_over,
       MAX(over_num) AS spell_end_over,
       SUM(runs_total) AS runs_conceded,
       SUM(CASE WHEN wicket_kind IS NOT NULL
                 AND wicket_kind NOT IN ('run out','retired hurt')
                THEN 1 ELSE 0 END) AS wickets,
       COUNT(*) AS balls
FROM silver_deliveries
GROUP BY bowler, match_id, inning, match_type, FLOOR(over_num / 5)
HAVING SUM(CASE WHEN wicket_kind IS NOT NULL
                 AND wicket_kind NOT IN ('run out','retired hurt')
                THEN 1 ELSE 0 END) >= 3
ORDER BY wickets DESC, runs_conceded ASC
LIMIT 20;

-- ─── KPI 30: Match-winning contribution index ────────────────────────────────
SELECT d.batsman, d.match_type,
       COUNT(DISTINCT d.match_id) AS matches,
       SUM(CASE WHEN m.winner = d.inning THEN d.runs_batsman ELSE 0 END) AS winning_runs,
       SUM(d.runs_batsman) AS total_runs,
       ROUND(SUM(CASE WHEN m.winner = d.inning THEN d.runs_batsman ELSE 0 END) * 100.0 /
             NULLIF(SUM(d.runs_batsman), 0), 2) AS win_contribution_pct
FROM silver_deliveries d
JOIN silver_matches m ON d.match_id = m.match_id
GROUP BY d.batsman, d.match_type
HAVING SUM(d.runs_batsman) >= 500
ORDER BY win_contribution_pct DESC
LIMIT 20;
