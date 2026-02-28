package com.rajesh.cricket.ingestion.batch

import com.rajesh.cricket.config.AppConfig
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.logging.log4j.LogManager

/**
 * Reads Cricsheet JSON/YAML files from a local directory and loads them
 * into the Bronze Delta table for raw storage.
 */
object CricsheetIngestionJob {

  private val logger = LogManager.getLogger(getClass)

  /**
   * Ingest Cricsheet match data files into the Bronze Delta table.
   *
   * @param dataPath  Path to the directory containing Cricsheet JSON files
   * @param spark     SparkSession
   */
  def run(dataPath: String)(implicit spark: SparkSession): Unit = {
    logger.info(s"Starting Cricsheet ingestion from: $dataPath")

    val rawDf = readCricsheetFiles(dataPath)
    logger.info(s"Read ${rawDf.count()} records from Cricsheet files")

    writeToBronze(rawDf)
    logger.info("Cricsheet ingestion to Bronze complete")
  }

  /**
   * Read all JSON files from the Cricsheet data directory.
   * Each file represents one match.
   *
   * @param dataPath  Path to Cricsheet JSON files
   * @param spark     SparkSession
   * @return          Raw DataFrame with file content
   */
  def readCricsheetFiles(dataPath: String)(implicit spark: SparkSession): DataFrame = {
    spark.read
      .option("multiLine", "true")
      .json(s"$dataPath/*.json")
      .withColumn("ingestion_time", current_timestamp())
      .withColumn("source_file",    input_file_name())
  }

  /**
   * Write the raw DataFrame to the Bronze Delta table.
   * Partitioned by match_type for efficient querying.
   *
   * @param df     Raw Cricsheet DataFrame
   * @param spark  SparkSession
   */
  private def writeToBronze(df: DataFrame)(implicit spark: SparkSession): Unit = {
    val bronzePath = AppConfig.deltaBronzeDeliveries

    // match_type lives inside the nested `info` struct (info.match_type).
    // Promote it to a top-level column so Delta can use it as a partition key.
    val partitioned = df
      .withColumn("match_type", col("info.match_type"))
      .withColumn("season",     col("info.season"))

    partitioned.write
      .format("delta")
      .mode("append")
      .partitionBy("match_type", "season")
      .save(bronzePath)

    logger.info(s"Written to Bronze Delta: $bronzePath")
  }
}
