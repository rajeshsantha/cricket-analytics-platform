/*
 * Cricket Analytics Platform
 * Author: Rajesh Santha
 *
 * Silver streaming layer — transforms Bronze streaming data into clean, typed records.
 */
package com.rajesh.cricket.silver

import com.rajesh.cricket.config.AppConfig
import com.rajesh.cricket.utils.SchemaUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.logging.log4j.LogManager

/**
 * Silver Streaming Job: reads from Bronze Delta as a stream, applies watermarking
 * for late data tolerance, parses JSON, flattens nested fields, and writes to Silver Delta.
 *
 * Watermark: 10 minutes on event_time
 * Output mode: append
 */
object SilverStreamingJob {

  private val logger = LogManager.getLogger(getClass)

  /**
   * Start the Silver streaming pipeline.
   *
   * @param spark  SparkSession with Delta extensions
   * @return       StreamingQuery handle
   */
  def run(spark: SparkSession): StreamingQuery = {
    logger.info("SilverStreamingJob: starting stream from Bronze Delta")

    val bronzeDf    = readFromBronze(spark)
    val flattenedDf = flatten(bronzeDf)
    writeToSilver(flattenedDf)
  }

  /**
   * Read from Bronze Delta table as a stream.
   *
   * @param spark  SparkSession
   * @return       Streaming DataFrame from Bronze Delta
   */
  def readFromBronze(spark: SparkSession): DataFrame = {
    spark.readStream
      .format("delta")
      .load(AppConfig.deltaBronzeLiveBalls)
  }

  /**
   * Parse raw JSON, apply watermark, flatten nested structs, and cast types.
   *
   * @param df  Bronze streaming DataFrame
   * @return    Flattened Silver DataFrame
   */
  def flatten(df: DataFrame): DataFrame = {
    val schema = SchemaUtils.liveBallSchema

    df.withColumn("parsed", from_json(col("raw_json"), schema))
      // Apply watermark for late data tolerance (10-minute window)
      .withColumn("event_time", to_timestamp(col("parsed.eventTime")))
      .withWatermark("event_time", "10 minutes")
      // Flatten nested fields
      .select(
        col("parsed.matchId").cast("string").as("match_id"),
        col("parsed.inning").cast("string").as("inning"),
        col("parsed.over").cast("int").as("over_num"),
        col("parsed.ball").cast("int").as("ball_num"),
        col("parsed.batsman").cast("string").as("batsman"),
        col("parsed.bowler").cast("string").as("bowler"),
        col("parsed.runs.batsman").cast("int").as("runs_batsman"),
        col("parsed.runs.extras").cast("int").as("runs_extras"),
        col("parsed.runs.total").cast("int").as("runs_total"),
        col("parsed.wicket.kind").cast("string").as("wicket_kind"),
        col("parsed.wicket.player_out").cast("string").as("wicket_player_out"),
        when(col("parsed.wicket.kind").isNotNull, true).otherwise(false).as("is_wicket"),
        col("event_time"),
        col("ingestion_time")
      )
  }

  /**
   * Write the flattened DataFrame to Silver Delta.
   *
   * @param df  Flattened streaming DataFrame
   * @return    StreamingQuery
   */
  private def writeToSilver(df: DataFrame): StreamingQuery = {
    val silverPath     = AppConfig.deltaSilverLiveBalls
    val checkpointPath = s"${AppConfig.checkpointBase}/silver/live_balls"

    df.writeStream
      .format("delta")
      .outputMode("append")
      .option("checkpointLocation", checkpointPath)
      .option("path", silverPath)
      .start()
  }
}
