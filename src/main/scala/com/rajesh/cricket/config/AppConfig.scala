package com.rajesh.cricket.config

import com.typesafe.config.{Config, ConfigFactory}

/** Loads and provides typed access to all application configuration. */
object AppConfig {

  private val config: Config = ConfigFactory.load("application")

  // Kafka configuration
  val kafkaBootstrapServers: String = config.getString("kafka.bootstrap-servers")
  val kafkaTopicLiveBalls: String   = config.getString("kafka.topics.live-balls")
  val kafkaTopicLiveMatches: String = config.getString("kafka.topics.live-matches")
  val kafkaTopicBatch: String       = config.getString("kafka.topics.cricsheet-batch")
  val kafkaConsumerGroup: String    = config.getString("kafka.consumer-group")

  // Delta Lake configuration
  val deltaBasePath: String            = config.getString("delta.base-path")
  val deltaBronzeLiveBalls: String     = config.getString("delta.bronze.live-balls")
  val deltaBronzeDeliveries: String    = config.getString("delta.bronze.batch-deliveries")
  val deltaSilverLiveBalls: String     = config.getString("delta.silver.live-balls")
  val deltaSilverDeliveries: String    = config.getString("delta.silver.deliveries")
  val deltaGoldLiveKpis: String        = config.getString("delta.gold.live-kpis")
  val deltaGoldBatchKpis: String       = config.getString("delta.gold.batch-kpis")

  // Spark configuration
  val sparkMaster: String  = config.getString("spark.master")
  val sparkAppName: String = config.getString("spark.app-name")
  val checkpointBase: String = config.getString("spark.checkpoint-base")

  // CricAPI configuration
  val cricApiConfig: CricApiConfig = CricApiConfig(
    baseUrl             = config.getString("cricapi.base-url"),
    apiKey              = if (config.hasPath("cricapi.api-key")) config.getString("cricapi.api-key") else "",
    pollIntervalSeconds = config.getInt("cricapi.poll-interval-seconds"),
    matchId             = if (config.hasPath("cricapi.match-id")) config.getString("cricapi.match-id") else ""
  )
}
