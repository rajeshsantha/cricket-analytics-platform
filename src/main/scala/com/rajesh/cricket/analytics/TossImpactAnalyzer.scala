package com.rajesh.cricket.analytics

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
 * Analyzes the impact of toss decisions on match outcomes.
 * Groups by toss_decision, venue, and match_type to calculate win probability.
 */
object TossImpactAnalyzer {

  /**
   * Analyze toss impact and return win probability by toss decision, venue, and match type.
   *
   * @param matchesDf  DataFrame with match-level data (from silver_matches view)
   *                   Expected columns: match_id, match_type, venue, winner, toss_winner, toss_decision
   * @return           DataFrame with toss impact statistics
   */
  def analyze(matchesDf: DataFrame)(implicit spark: SparkSession): DataFrame = {
    matchesDf
      .filter(col("toss_decision").isNotNull)
      .filter(col("winner").isNotNull)
      .groupBy("toss_decision", "venue", "match_type")
      .agg(
        count("*").as("total_matches"),
        sum(when(col("toss_winner") === col("winner"), 1).otherwise(0)).as("toss_winner_wins"),
        sum(when(col("toss_winner") =!= col("winner"), 1).otherwise(0)).as("toss_winner_losses")
      )
      .withColumn(
        "win_probability",
        round(col("toss_winner_wins") * 100.0 / col("total_matches"), 2)
      )
      .filter(col("total_matches") >= 5) // filter out venues with too few matches
      .orderBy(col("win_probability").desc)
  }

  /**
   * Summary: win % by toss decision across all venues (bat vs field).
   *
   * @param matchesDf  Silver matches DataFrame
   * @return           Summarized DataFrame
   */
  def summarize(matchesDf: DataFrame)(implicit spark: SparkSession): DataFrame = {
    matchesDf
      .filter(col("toss_decision").isNotNull)
      .filter(col("winner").isNotNull)
      .groupBy("toss_decision", "match_type")
      .agg(
        count("*").as("total_matches"),
        sum(when(col("toss_winner") === col("winner"), 1).otherwise(0)).as("wins_after_toss"),
        round(
          sum(when(col("toss_winner") === col("winner"), 1).otherwise(0)) * 100.0 / count("*"),
          2
        ).as("win_pct")
      )
      .orderBy("match_type", "toss_decision")
  }
}
