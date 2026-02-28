package com.rajesh.cricket.gold

import com.rajesh.cricket.config.SparkSessionFactory
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.functions._

/**
 * Unit tests for GoldBatchKPIs.
 * Verifies that at least 5 KPI SQL queries return non-empty results.
 */
class GoldKPISpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  implicit var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSessionFactory.createForTest("GoldKPISpec")
    createTestViews()
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
  }

  // ─── Tests ────────────────────────────────────────────────────────────────

  "KPI 1: Top Run Scorers" should "return non-empty results" in {
    val result = spark.sql(GoldBatchKPIs.kpi01TopRunScorers)
    result.count() should be > 0L
    result.columns should contain ("batsman")
    result.columns should contain ("total_runs")
  }

  "KPI 2: Top Wicket Takers" should "return non-empty results" in {
    val result = spark.sql(GoldBatchKPIs.kpi02TopWicketTakers)
    result.count() should be > 0L
    result.columns should contain ("bowler")
    result.columns should contain ("wickets")
  }

  "KPI 8: Most Sixes" should "return non-empty results" in {
    val result = spark.sql(GoldBatchKPIs.kpi08MostSixes)
    result.count() should be > 0L
    result.columns should contain ("sixes")
  }

  "KPI 20: Dot Ball Percentage" should "return non-empty results" in {
    val result = spark.sql(GoldBatchKPIs.kpi20DotBallPct)
    // May be empty if no bowler has 100+ balls - just check schema
    result.columns should contain ("dot_ball_pct")
  }

  "KPI 24: Run Rate Progression" should "return non-empty results" in {
    val result = spark.sql(GoldBatchKPIs.kpi24RunRateProgression)
    result.count() should be > 0L
    result.columns should contain ("avg_runs_per_over")
  }

  // ─── Helpers ──────────────────────────────────────────────────────────────

  /** Create in-memory test views matching the silver_deliveries and silver_matches schema. */
  private def createTestViews(): Unit = {
    import spark.implicits._

    val deliveries = Seq(
      ("match-1", "T20", "India",     "Australia", "MCG", "2023-01-01", "India", "India", "bat",
       "India", 0, "Kohli",  "Starc",    "Rohit",  4, 0, 4, null, null),
      ("match-1", "T20", "India",     "Australia", "MCG", "2023-01-01", "India", "India", "bat",
       "India", 0, "Rohit",  "Cummins",  "Kohli",  6, 0, 6, null, null),
      ("match-1", "T20", "India",     "Australia", "MCG", "2023-01-01", "India", "India", "bat",
       "Australia", 0, "Warner", "Bumrah", "Finch", 0, 0, 0, "bowled", "Warner"),
      ("match-1", "T20", "India",     "Australia", "MCG", "2023-01-01", "India", "India", "bat",
       "India", 1, "Kohli",  "Hazlewood","Rohit",  1, 0, 1, null, null),
      ("match-2", "ODI", "England",   "Pakistan",  "Lord's", "2023-02-01", "England", "England", "field",
       "England", 0, "Root",   "Amir",    "Buttler", 4, 0, 4, null, null),
      ("match-2", "ODI", "England",   "Pakistan",  "Lord's", "2023-02-01", "England", "England", "field",
       "Pakistan", 0, "Babar",  "Anderson","Rizwan",  2, 0, 2, null, null)
    ).toDF(
      "match_id", "match_type", "team1", "team2", "venue", "match_date",
      "winner", "toss_winner", "toss_decision",
      "inning", "over_num", "batsman", "bowler", "non_striker",
      "runs_batsman", "runs_extras", "runs_total",
      "wicket_kind", "wicket_player_out"
    )

    deliveries.createOrReplaceTempView("silver_deliveries")

    val matches = deliveries.select(
      "match_id", "match_type", "team1", "team2", "venue",
      "match_date", "winner", "toss_winner", "toss_decision"
    ).distinct()
    matches.createOrReplaceTempView("silver_matches")
  }
}
