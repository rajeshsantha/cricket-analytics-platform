/*
 * Cricket Analytics Platform
 * Author: Rajesh Santha
 *
 * Gold streaming layer — produces real-time KPIs from live ball-by-ball data.
 */
package com.rajesh.cricket.gold

import com.rajesh.cricket.config.AppConfig
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.logging.log4j.LogManager

/**
 * Gold Streaming KPIs Job: reads from Silver Delta as a stream, computes stateful
 * aggregations using window functions, and writes live KPIs to Gold Delta.
 *
 * KPIs computed:
 *  - Running run rate per match (5-min window, 1-min slide)
 *  - Wickets in last 5 overs
 *  - Current partnership runs
 *  - Batsman strike rate (live)
 *  - Bowler economy rate (live)
 */
object GoldStreamingKPIs {

  private val logger = LogManager.getLogger(getClass)

  /**
   * Start the Gold streaming KPI pipeline.
   *
   * @param spark  SparkSession with Delta extensions
   * @return       StreamingQuery handle
   */
  def run(spark: SparkSession): StreamingQuery = {
    logger.info("GoldStreamingKPIs: starting aggregation stream from Silver Delta")

    val silverDf = readFromSilver(spark)
    val kpiDf    = computeKPIs(silverDf)
    writeToGold(kpiDf)
  }

  /**
   * Read from Silver Delta as a streaming source.
   *
   * @param spark  SparkSession
   * @return       Streaming DataFrame from Silver Delta
   */
  def readFromSilver(spark: SparkSession): DataFrame = {
    spark.readStream
      .format("delta")
      .load(AppConfig.deltaSilverLiveBalls)
  }

  /**
   * Compute live KPIs using windowed aggregations.
   *
   * @param df  Silver streaming DataFrame
   * @return    Aggregated KPI DataFrame
   */
  def computeKPIs(df: DataFrame): DataFrame = {
    // Running run rate: total runs in 5-min sliding window
    df.withWatermark("event_time", "10 minutes")
      .groupBy(
        col("match_id"),
        window(col("event_time"), "5 minutes", "1 minute")
      )
      .agg(
        sum("runs_total").as("window_runs"),
        count("*").as("balls_bowled"),
        sum(col("is_wicket").cast("int")).as("wickets_in_window"),
        // Run rate = runs per over (6 balls per over)
        (sum("runs_total") * 6.0 / count("*")).as("run_rate"),
        // Batsman strike rate: runs / balls * 100
        (sum("runs_batsman") * 100.0 / count("*")).as("batting_strike_rate"),
        // Bowler economy: runs conceded per over
        (sum("runs_total") * 6.0 / count("*")).as("bowler_economy"),
        first("batsman").as("current_batsman"),
        first("bowler").as("current_bowler")
      )
      .select(
        col("match_id"),
        col("window.start").as("window_start"),
        col("window.end").as("window_end"),
        col("window_runs"),
        col("balls_bowled"),
        col("wickets_in_window"),
        col("run_rate"),
        col("batting_strike_rate"),
        col("bowler_economy"),
        col("current_batsman"),
        col("current_bowler"),
        current_timestamp().as("computed_at")
      )
  }

  /**
   * Write KPI DataFrame to Gold Delta with update output mode.
   *
   * @param df  KPI DataFrame
   * @return    StreamingQuery
   */
  private def writeToGold(df: DataFrame): StreamingQuery = {
    val goldPath       = AppConfig.deltaGoldLiveKpis
    val checkpointPath = s"${AppConfig.checkpointBase}/gold/live_kpis"

    df.writeStream
      .format("delta")
      .outputMode("append")
      .option("checkpointLocation", checkpointPath)
      .option("path", goldPath)
      .start()
  }
}
