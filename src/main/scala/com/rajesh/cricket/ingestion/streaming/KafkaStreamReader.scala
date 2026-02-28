package com.rajesh.cricket.ingestion.streaming

import com.rajesh.cricket.config.AppConfig
import org.apache.spark.sql.{DataFrame, SparkSession}

/** Reads live ball events from Kafka as a Spark Structured Streaming source. */
object KafkaStreamReader {

  /**
   * Create a streaming DataFrame that reads from the cricket-live-balls Kafka topic.
   *
   * @param spark            SparkSession with Kafka support
   * @param bootstrapServers Kafka bootstrap server address
   * @param startingOffsets  Kafka starting offsets: "earliest", "latest", or JSON
   * @return                 Streaming DataFrame with columns: key, value (as String), topic, partition, offset, timestamp
   */
  def readLiveBalls(
    spark: SparkSession,
    bootstrapServers: String = AppConfig.kafkaBootstrapServers,
    startingOffsets: String  = "latest"
  ): DataFrame = {
    import spark.implicits._

    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("subscribe",               AppConfig.kafkaTopicLiveBalls)
      .option("startingOffsets",         startingOffsets)
      .option("failOnDataLoss",          "false")
      .load()
      .selectExpr("CAST(key AS STRING) AS key", "CAST(value AS STRING) AS value")
  }

  /**
   * Create a streaming DataFrame that reads from all cricket Kafka topics.
   *
   * @param spark            SparkSession
   * @param bootstrapServers Kafka bootstrap server address
   * @return                 Streaming DataFrame
   */
  def readAllTopics(
    spark: SparkSession,
    bootstrapServers: String = AppConfig.kafkaBootstrapServers
  ): DataFrame = {
    val topics = Seq(
      AppConfig.kafkaTopicLiveBalls,
      AppConfig.kafkaTopicLiveMatches
    ).mkString(",")

    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("subscribe",       topics)
      .option("startingOffsets", "latest")
      .option("failOnDataLoss",  "false")
      .load()
      .selectExpr("CAST(key AS STRING) AS key", "CAST(value AS STRING) AS value", "topic")
  }
}
