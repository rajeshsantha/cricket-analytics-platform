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

    df.write
      .format("delta")
      .mode("append")
      .partitionBy("match_type", "batch_date")
      .save(bronzePath)

    logger.info(s"BronzeBatchJob: written to Delta at $bronzePath")
  }
}
