package com.rajesh.cricket.config

import org.apache.spark.sql.SparkSession

/** Factory for creating SparkSession with Delta Lake extensions. */
object SparkSessionFactory {

  /**
   * Create a SparkSession configured for Delta Lake.
   *
   * @param appName   Spark application name
   * @param master    Spark master URL (e.g., "local[*]" for dev or cluster URL)
   * @return          Configured SparkSession
   */
  def create(
    appName: String = AppConfig.sparkAppName,
    master: String  = AppConfig.sparkMaster
  ): SparkSession = {
    SparkSession.builder()
      .appName(appName)
      .master(master)
      // Enable Delta Lake SQL extensions
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      // Use Delta catalog as the default Spark catalog
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      // Performance tuning for local dev
      .config("spark.sql.shuffle.partitions", "8")
      .config("spark.driver.memory", "2g")
      // Streaming config
      .config("spark.streaming.stopGracefullyOnShutdown", "true")
      .getOrCreate()
  }

  /**
   * Create a SparkSession suitable for unit tests (no UI, minimal resources).
   */
  def createForTest(appName: String = "CricketAnalytics-Test"): SparkSession = {
    SparkSession.builder()
      .appName(appName)
      .master("local[2]")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
  }
}
