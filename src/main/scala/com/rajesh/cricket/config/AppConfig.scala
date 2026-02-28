package com.rajesh.cricket.config

import com.typesafe.config.{Config, ConfigFactory}

/** Loads and provides typed access to all application configuration. */
object AppConfig {

  private val config: Config = ConfigFactory.load("application")

  // Spark configuration (needed at startup — keep eager)
  val sparkMaster: String    = config.getString("spark.master")
  val sparkAppName: String   = config.getString("spark.app-name")
  val checkpointBase: String = config.getString("spark.checkpoint-base")

  // Delta Lake configuration (needed for all modes — keep eager)
  val deltaBasePath: String         = config.getString("delta.base-path")
  val deltaBronzeLiveBalls: String  = config.getString("delta.bronze.live-balls")
  val deltaBronzeDeliveries: String = config.getString("delta.bronze.batch-deliveries")
  val deltaSilverLiveBalls: String  = config.getString("delta.silver.live-balls")
  val deltaSilverDeliveries: String = config.getString("delta.silver.deliveries")
  val deltaGoldLiveKpis: String     = config.getString("delta.gold.live-kpis")
  val deltaGoldBatchKpis: String    = config.getString("delta.gold.batch-kpis")

  // Kafka configuration (only needed for streaming — lazy to avoid init failure in batch mode)
  lazy val kafkaBootstrapServers: String = config.getString("kafka.bootstrap-servers")
  lazy val kafkaTopicLiveBalls: String   = config.getString("kafka.topics.live-balls")
  lazy val kafkaTopicLiveMatches: String = config.getString("kafka.topics.live-matches")
  lazy val kafkaTopicBatch: String       = config.getString("kafka.topics.cricsheet-batch")
  lazy val kafkaConsumerGroup: String    = config.getString("kafka.consumer-group")

  // CricAPI configuration (only needed for poller mode — lazy)
  lazy val cricApiConfig: CricApiConfig = CricApiConfig(
    baseUrl             = config.getString("cricapi.base-url"),
    apiKey              = if (config.hasPath("cricapi.api-key")) config.getString("cricapi.api-key") else "",
    pollIntervalSeconds = config.getInt("cricapi.poll-interval-seconds"),
    matchId             = if (config.hasPath("cricapi.match-id")) config.getString("cricapi.match-id") else ""
  )
}
