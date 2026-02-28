package com.rajesh.cricket.analytics

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

/**
 * Provides reusable window functions for cricket analytics.
 * Supports both batch and streaming DataFrames.
 */
object WindowFunctions {

  /**
   * Calculate rolling average runs over the last 6 balls (1 over).
   *
   * @param df  DataFrame with columns: match_id, inning, runs_total, over_num, ball_num
   * @return    DataFrame with rolling_avg_runs column added
   */
  def rollingAvgRuns(df: DataFrame): DataFrame = {
    val windowSpec = Window
      .partitionBy("match_id", "inning")
      .orderBy("over_num", "ball_num")
      .rowsBetween(-5, 0) // last 6 balls

    df.withColumn("rolling_avg_runs", avg("runs_total").over(windowSpec))
  }

  /**
   * Calculate cumulative runs per innings.
   *
   * @param df  DataFrame with match_id, inning, runs_total, over_num, ball_num
   * @return    DataFrame with cumulative_runs column
   */
  def cumulativeRuns(df: DataFrame): DataFrame = {
    val windowSpec = Window
      .partitionBy("match_id", "inning")
      .orderBy("over_num", "ball_num")
      .rowsBetween(Window.unboundedPreceding, 0)

    df.withColumn("cumulative_runs", sum("runs_total").over(windowSpec))
  }

  /**
   * Calculate over-by-over run rate (runs scored in each over).
   *
   * @param df  DataFrame with match_id, inning, over_num, runs_total
   * @return    DataFrame with over_run_rate column
   */
  def overByOverRunRate(df: DataFrame): DataFrame = {
    df.groupBy("match_id", "inning", "over_num")
      .agg(
        sum("runs_total").as("over_runs"),
        count("*").as("balls_in_over")
      )
      .withColumn("over_run_rate",
        round(col("over_runs") * 6.0 / col("balls_in_over"), 2)
      )
  }

  /**
   * Add lag/lead columns to detect momentum shifts (previous over runs vs current).
   *
   * @param df  DataFrame with match_id, inning, over_num, over_runs
   * @return    DataFrame with prev_over_runs and momentum_shift columns
   */
  def momentumShifts(df: DataFrame): DataFrame = {
    val windowSpec = Window
      .partitionBy("match_id", "inning")
      .orderBy("over_num")

    df.withColumn("prev_over_runs", lag("over_runs", 1).over(windowSpec))
      .withColumn("next_over_runs", lead("over_runs", 1).over(windowSpec))
      .withColumn("momentum_shift",
        when(col("over_runs") > col("prev_over_runs"), "accelerating")
          .when(col("over_runs") < col("prev_over_runs"), "decelerating")
          .otherwise("steady")
      )
  }

  /**
   * Rank batsmen by total runs within each match.
   *
   * @param df  DataFrame with match_id, batsman, runs_batsman
   * @return    DataFrame with batsman_rank column (1 = top scorer in match)
   */
  def rankBatsmenInMatch(df: DataFrame): DataFrame = {
    val windowSpec = Window
      .partitionBy("match_id")
      .orderBy(col("innings_runs").desc)

    val batsmanTotals = df.groupBy("match_id", "batsman")
      .agg(sum("runs_batsman").as("innings_runs"))

    batsmanTotals.withColumn("batsman_rank", rank().over(windowSpec))
  }
}
