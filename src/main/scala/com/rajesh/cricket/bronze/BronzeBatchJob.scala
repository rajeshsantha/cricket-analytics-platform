/*
 * Cricket Analytics Platform
 * Author: Rajesh Santha
 *
 * Bronze batch layer — reads raw Cricsheet JSON and writes to Bronze Delta table.
 */
package com.rajesh.cricket.bronze

import com.rajesh.cricket.config.AppConfig
import com.rajesh.cricket.ingestion.batch.CricsheetIngestionJob
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.logging.log4j.LogManager

/**
 * Bronze Batch Job: reads Cricsheet raw files and writes them to the Bronze Delta layer.
 * Partitions by match_type and date for efficient downstream queries.
 */
object BronzeBatchJob {

  private val logger = LogManager.getLogger(getClass)

  /**
   * Run the Bronze batch ingestion pipeline.
   *
   * @param dataPath  Path to the Cricsheet data directory
   * @param spark     SparkSession
   */
  def run(dataPath: String)(implicit spark: SparkSession): Unit = {
    logger.info(s"BronzeBatchJob: starting ingestion from $dataPath")

    // Use the Cricsheet ingestion job to read raw files
    val rawDf = CricsheetIngestionJob.readCricsheetFiles(dataPath)

    // Add processing metadata columns
    val enrichedDf = rawDf
      .withColumn("bronze_ingestion_time", current_timestamp())
      .withColumn("batch_date", to_date(current_timestamp()))

    writeToDelta(enrichedDf)
    logger.info("BronzeBatchJob: completed successfully")
  }

  /**
   * Write a DataFrame to the Bronze Delta table.
   * Partitioned by match_type and batch_date.
   *
   * @param df     Enriched raw DataFrame
   * @param spark  SparkSession
   */
  def writeToDelta(df: DataFrame)(implicit spark: SparkSession): Unit = {
    val bronzePath = AppConfig.deltaBronzeDeliveries

    // info.players  → struct keyed by team name  (e.g. "Chennai Super Kings")
    // info.registry → struct keyed by player name (e.g. "V Kohli")
    // Both contain spaces in field names which Delta forbids.
    // Rebuild info without those two sub-fields; all analytics columns are preserved.
    val cleanDf = df
      .withColumn("info", struct(
        col("info.balls_per_over"),
        col("info.city"),
        col("info.dates"),
        col("info.event"),
        col("info.gender"),
        col("info.match_type"),
        col("info.officials"),
        col("info.outcome"),
        col("info.overs"),
        col("info.player_of_match"),
        col("info.season"),
        col("info.team_type"),
        col("info.teams"),
        col("info.toss"),
        col("info.venue")
        // info.players and info.registry intentionally excluded:
        // their keys are team/player names containing spaces, which
        // Delta Lake forbids as column names.
      ))
      .withColumn("match_type", col("info.match_type"))
      .withColumn("season",     col("info.season"))

    cleanDf.write
      .format("delta")
      .mode("append")
      .partitionBy("match_type", "season")
      .save(bronzePath)

    logger.info(s"BronzeBatchJob: written to Delta at $bronzePath")
  }
}
