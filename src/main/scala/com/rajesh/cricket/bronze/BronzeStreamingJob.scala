package com.rajesh.cricket.bronze

import com.rajesh.cricket.config.AppConfig
import com.rajesh.cricket.ingestion.streaming.KafkaStreamReader
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.logging.log4j.LogManager

/**
 * Bronze Streaming Job: reads raw JSON events from Kafka and writes them
 * to the Bronze Delta layer with ingestion metadata.
 *
 * Output mode: append (no aggregations at bronze level)
 * Checkpoint: ensures exactly-once delivery
 */
object BronzeStreamingJob {

  private val logger = LogManager.getLogger(getClass)

  /**
   * Start the Bronze streaming pipeline.
   *
   * @param spark  SparkSession with Kafka and Delta extensions
   * @return       StreamingQuery handle for monitoring/awaiting termination
   */
  def run(spark: SparkSession): StreamingQuery = {
    logger.info("BronzeStreamingJob: starting streaming from Kafka to Delta Bronze")

    val kafkaDf = KafkaStreamReader.readLiveBalls(spark)
    val enrichedDf = enrich(kafkaDf)
    writeToDeltal(enrichedDf, spark)
  }

  /**
   * Enrich the raw Kafka DataFrame with ingestion metadata.
   *
   * @param df  Raw Kafka streaming DataFrame (key, value)
   * @return    Enriched DataFrame
   */
  def enrich(df: DataFrame): DataFrame = {
    df.withColumn("ingestion_time", current_timestamp())
      .withColumn("raw_json", col("value"))
      .drop("key", "value")
  }

  /**
   * Write the enriched DataFrame to Bronze Delta in append mode.
   *
   * @param df     Enriched streaming DataFrame
   * @param spark  SparkSession
   * @return       StreamingQuery
   */
  private def writeToDeltal(df: DataFrame, spark: SparkSession): StreamingQuery = {
    val bronzePath     = AppConfig.deltaBronzeLiveBalls
    val checkpointPath = s"${AppConfig.checkpointBase}/bronze/live_balls"

    df.writeStream
      .format("delta")
      .outputMode("append")
      .option("checkpointLocation", checkpointPath)
      .option("path", bronzePath)
      .start()
  }
}
