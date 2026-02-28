package com.rajesh.cricket

import com.rajesh.cricket.bronze.{BronzeBatchJob, BronzeStreamingJob}
import com.rajesh.cricket.config.{AppConfig, SparkSessionFactory}
import com.rajesh.cricket.gold.{GoldBatchKPIs, GoldStreamingKPIs}
import com.rajesh.cricket.ingestion.batch.CricsheetIngestionJob
import com.rajesh.cricket.ingestion.streaming.{CricApiPoller, KafkaProducer}
import com.rajesh.cricket.silver.{SilverBatchJob, SilverStreamingJob}
import org.apache.logging.log4j.LogManager

/**
 * Main entry point for the Cricket Analytics Platform.
 *
 * Usage:
 *   --mode batch      [--data-path /path/to/cricsheet]
 *   --mode streaming
 *   --mode poller
 *   --mode gold
 */
object Main {

  private val logger = LogManager.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val argMap = parseArgs(args)
    val mode   = argMap.getOrElse("mode", "batch")

    logger.info(s"Cricket Analytics Platform starting in mode: $mode")

    mode match {
      case "batch"     => runBatch(argMap)
      case "streaming" => runStreaming()
      case "poller"    => runPoller()
      case "gold"      => runGoldOnly()
      case unknown =>
        logger.error(s"Unknown mode: $unknown. Use: batch | streaming | poller | gold")
        System.exit(1)
    }
  }

  /** Run the full batch pipeline: Ingest → Bronze → Silver → Gold */
  private def runBatch(argMap: Map[String, String]): Unit = {
    val dataPath = argMap.getOrElse("data-path", "/tmp/cricsheet-data")
    logger.info(s"Running BATCH pipeline with data path: $dataPath")

    implicit val spark = SparkSessionFactory.create()
    try {
      CricsheetIngestionJob.run(dataPath)
      BronzeBatchJob.run(dataPath)
      SilverBatchJob.run
      GoldBatchKPIs.run
      logger.info("Batch pipeline completed successfully")
    } finally {
      spark.stop()
    }
  }

  /** Run all streaming jobs in parallel threads: Bronze → Silver → Gold */
  private def runStreaming(): Unit = {
    logger.info("Running STREAMING pipeline")

    val spark = SparkSessionFactory.create()

    // Start each streaming job in its own thread
    val bronzeThread = new Thread(() => {
      val query = BronzeStreamingJob.run(spark)
      query.awaitTermination()
    }, "bronze-streaming-thread")

    val silverThread = new Thread(() => {
      Thread.sleep(5000L) // wait for bronze to start
      val query = SilverStreamingJob.run(spark)
      query.awaitTermination()
    }, "silver-streaming-thread")

    val goldThread = new Thread(() => {
      Thread.sleep(10000L) // wait for silver to start
      val query = GoldStreamingKPIs.run(spark)
      query.awaitTermination()
    }, "gold-streaming-thread")

    bronzeThread.setDaemon(false)
    silverThread.setDaemon(false)
    goldThread.setDaemon(false)

    bronzeThread.start()
    silverThread.start()
    goldThread.start()

    // Register shutdown hook for graceful stop
    Runtime.getRuntime.addShutdownHook(new Thread(() => {
      logger.info("Shutdown hook triggered, stopping Spark...")
      spark.stop()
    }))

    bronzeThread.join()
    silverThread.join()
    goldThread.join()
  }

  /** Run the CricAPI poller standalone */
  private def runPoller(): Unit = {
    val config   = AppConfig.cricApiConfig
    val producer = new KafkaProducer(AppConfig.kafkaBootstrapServers)
    val poller   = new CricApiPoller(config, producer)

    logger.info(s"Starting CricAPI poller for matchId=${config.matchId}")

    Runtime.getRuntime.addShutdownHook(new Thread(() => {
      logger.info("Shutting down poller...")
      producer.close()
    }))

    poller.startPolling(config.matchId)
  }

  /** Run only the Gold KPI batch job */
  private def runGoldOnly(): Unit = {
    logger.info("Running GOLD KPI batch job only")
    implicit val spark = SparkSessionFactory.create()
    try {
      GoldBatchKPIs.run
    } finally {
      spark.stop()
    }
  }

  /** Parse command-line arguments into a key-value map. */
  private def parseArgs(args: Array[String]): Map[String, String] = {
    args.grouped(2).collect {
      case Array(key, value) if key.startsWith("--") => key.stripPrefix("--") -> value
    }.toMap
  }
}
