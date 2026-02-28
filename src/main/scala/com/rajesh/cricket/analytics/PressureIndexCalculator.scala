package com.rajesh.cricket.analytics

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

/**
 * Calculates the cricket Pressure Index to quantify match pressure at any point.
 *
 * Formula:
 *   pressure_index = (required_run_rate / current_run_rate) * (1 + wickets_lost/10) * (balls_remaining/total_balls)
 *
 * Ranges: 0-1 = Low pressure, 1-2 = Medium pressure, 2+ = High pressure
 */
object PressureIndexCalculator {

  /**
   * UDF to compute pressure index given key match state variables.
   *
   * @param requiredRR    Required run rate to win
   * @param currentRR     Current run rate
   * @param wicketsLost   Wickets already fallen
   * @param ballsRemaining Balls remaining in innings
   * @param totalBalls    Total balls in the innings (120 for T20, 300 for ODI)
   * @return              Pressure index (Double)
   */
  val pressureIndexUDF: UserDefinedFunction = udf(
    (requiredRR: Double, currentRR: Double, wicketsLost: Int, ballsRemaining: Int, totalBalls: Int) => {
      if (currentRR <= 0 || totalBalls <= 0) 0.0
      else {
        val rrRatio        = requiredRR / math.max(currentRR, 0.1)
        val wicketFactor   = 1.0 + (wicketsLost.toDouble / 10.0)
        val ballsFactor    = ballsRemaining.toDouble / totalBalls.toDouble
        val pressureIndex  = rrRatio * wicketFactor * ballsFactor
        math.round(pressureIndex * 100.0) / 100.0
      }
    }
  )

  /**
   * Apply the pressure index calculation to a batch DataFrame.
   * Expects columns: required_rr, current_rr, wickets_lost, balls_remaining, total_balls
   *
   * @param df  DataFrame with match state columns
   * @return    DataFrame with pressure_index and pressure_level columns added
   */
  def applyToBatch(df: DataFrame): DataFrame = {
    df.withColumn(
      "pressure_index",
      pressureIndexUDF(
        col("required_rr"),
        col("current_rr"),
        col("wickets_lost"),
        col("balls_remaining"),
        col("total_balls")
      )
    ).withColumn(
      "pressure_level",
      when(col("pressure_index") < 1.0, "Low")
        .when(col("pressure_index") < 2.0, "Medium")
        .otherwise("High")
    )
  }

  /**
   * Compute per-over pressure index from raw Silver deliveries.
   *
   * @param df    Silver deliveries DataFrame
   * @param spark SparkSession
   * @return      DataFrame with over-level pressure index
   */
  def computePerOver(df: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    // Calculate over-by-over cumulative state
    val overStats = df.groupBy("match_id", "inning", "over_num")
      .agg(
        sum("runs_total").as("over_runs"),
        sum(col("wicket_kind").isNotNull.cast("int")).as("wickets_in_over")
      )

    overStats
      .withColumn("pressure_index",
        pressureIndexUDF(
          lit(8.0),   // approximate required RR
          col("over_runs").cast("double"),
          col("wickets_in_over"),
          (20 - col("over_num")) * lit(6),
          lit(120)
        )
      )
      .withColumn("pressure_level",
        when(col("pressure_index") < 1.0, "Low")
          .when(col("pressure_index") < 2.0, "Medium")
          .otherwise("High")
      )
  }
}
