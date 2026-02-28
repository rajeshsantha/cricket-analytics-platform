package com.rajesh.cricket.silver

import com.rajesh.cricket.config.AppConfig
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.logging.log4j.LogManager

/**
 * Silver Batch Job: reads from Bronze Delta batch, explodes Cricsheet deliveries,
 * flattens match info and delivery info into one row per ball, applies data quality
 * checks, and writes to Silver Delta partitioned by match_type.
 */
object SilverBatchJob {

  private val logger = LogManager.getLogger(getClass)

  /**
   * Run the Silver batch transformation pipeline.
   *
   * @param spark  SparkSession
   */
  def run(implicit spark: SparkSession): Unit = {
    logger.info("SilverBatchJob: starting transformation from Bronze to Silver")

    val bronzeDf    = readFromBronze
    val flattenedDf = flatten(bronzeDf)
    val cleanDf     = applyDataQuality(flattenedDf)

    writeToSilver(cleanDf)
    logger.info("SilverBatchJob: completed successfully")
  }

  /**
   * Read from the Bronze Delta batch table.
   *
   * @param spark  SparkSession
   * @return       Bronze DataFrame
   */
  def readFromBronze(implicit spark: SparkSession): DataFrame = {
    spark.read.format("delta").load(AppConfig.deltaBronzeDeliveries)
  }

  /**
   * Flatten Cricsheet nested structure into one row per delivery.
   * Extracts match info + delivery info from the innings/overs/deliveries hierarchy.
   *
   * @param df  Raw Bronze DataFrame
   * @return    Flattened Silver DataFrame
   */
  def flatten(df: DataFrame): DataFrame = {
    // Cricsheet structure: info (teams, venue, toss, outcome) + innings[]
    df.withColumn("match_type",   col("info.match_type"))
      .withColumn("team1",        col("info.teams").getItem(0))
      .withColumn("team2",        col("info.teams").getItem(1))
      .withColumn("venue",        col("info.venue"))
      .withColumn("match_date",   col("info.dates").getItem(0))
      .withColumn("winner",       col("info.outcome.winner"))
      .withColumn("toss_winner",  col("info.toss.winner"))
      .withColumn("toss_decision", col("info.toss.decision"))
      // Explode innings
      .withColumn("innings_data", explode(col("innings")))
      .withColumn("inning_name",  col("innings_data.team"))
      // Explode overs within each innings
      .withColumn("over_data",    explode(col("innings_data.overs")))
      .withColumn("over_num",     col("over_data.over"))
      // Explode deliveries within each over
      .withColumn("delivery",     explode(col("over_data.deliveries")))
      // Extract delivery fields
      .select(
        col("source_file").as("match_id"),
        col("match_type"),
        col("team1"),
        col("team2"),
        col("venue"),
        col("match_date"),
        col("winner"),
        col("toss_winner"),
        col("toss_decision"),
        col("inning_name").as("inning"),
        col("over_num"),
        col("delivery.batter").as("batsman"),
        col("delivery.bowler").as("bowler"),
        col("delivery.non_striker").as("non_striker"),
        col("delivery.runs.batter").cast("int").as("runs_batsman"),
        col("delivery.runs.extras").cast("int").as("runs_extras"),
        col("delivery.runs.total").cast("int").as("runs_total"),
        col("delivery.wickets").getItem(0).getField("kind").as("wicket_kind"),
        col("delivery.wickets").getItem(0).getField("player_out").as("wicket_player_out"),
        col("bronze_ingestion_time")
      )
  }

  /**
   * Apply data quality checks: filter out nulls and validate run ranges.
   *
   * @param df  Flattened DataFrame
   * @return    Cleaned DataFrame
   */
  def applyDataQuality(df: DataFrame): DataFrame = {
    df.filter(col("batsman").isNotNull)
      .filter(col("bowler").isNotNull)
      .filter(col("runs_total") >= 0 && col("runs_total") <= 36)
      .filter(col("over_num") >= 0)
      .na.fill(0, Seq("runs_batsman", "runs_extras", "runs_total"))
  }

  /**
   * Write cleaned DataFrame to Silver Delta, partitioned by match_type.
   *
   * @param df     Silver DataFrame
   * @param spark  SparkSession
   */
  private def writeToSilver(df: DataFrame)(implicit spark: SparkSession): Unit = {
    df.write
      .format("delta")
      .mode("append")
      .partitionBy("match_type")
      .save(AppConfig.deltaSilverDeliveries)

    logger.info(s"SilverBatchJob: written to ${AppConfig.deltaSilverDeliveries}")
  }
}
