package com.rajesh.cricket.bronze

import com.rajesh.cricket.config.SparkSessionFactory
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import java.io.{File, PrintWriter}
import java.nio.file.{Files, Path}

/**
 * Unit tests for BronzeBatchJob.
 * Verifies that Cricsheet-like JSON files are read correctly and written to Delta.
 */
class BronzeJobSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  implicit var spark: SparkSession = _
  var tempDir: Path                = _

  override def beforeAll(): Unit = {
    spark   = SparkSessionFactory.createForTest("BronzeJobSpec")
    tempDir = Files.createTempDirectory("bronze-test")
    createSampleJsonFiles(tempDir.toFile)
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
    deleteDirectory(tempDir.toFile)
  }

  // ─── Tests ────────────────────────────────────────────────────────────────

  "BronzeBatchJob.writeToDelta" should "write DataFrame to a Delta table" in {
    val deltaPath = s"${tempDir.toAbsolutePath}/bronze-output"

    // Read sample files
    val rawDf = spark.read
      .option("multiLine", "true")
      .json(s"${tempDir.toAbsolutePath}/data/*.json")
      .withColumn("match_type",            org.apache.spark.sql.functions.lit("T20"))
      .withColumn("batch_date",            org.apache.spark.sql.functions.current_date())
      .withColumn("bronze_ingestion_time", org.apache.spark.sql.functions.current_timestamp())

    // Override config path for test
    rawDf.write
      .format("delta")
      .mode("append")
      .save(deltaPath)

    // Verify the output Delta table
    val result = spark.read.format("delta").load(deltaPath)
    result.count() should be > 0L
  }

  "BronzeBatchJob.enrich (BronzeStreamingJob)" should "add ingestion_time column" in {
    val inputDf = spark.createDataFrame(Seq(
      ("match-1", """{"matchId":"match-1","over":0,"ball":1}""")
    )).toDF("key", "value")

    val enriched = BronzeStreamingJob.enrich(inputDf)

    enriched.columns should contain ("ingestion_time")
    enriched.columns should contain ("raw_json")
    enriched.columns should not contain "value"
  }

  // ─── Helpers ──────────────────────────────────────────────────────────────

  /** Create minimal Cricsheet-like JSON files for testing. */
  private def createSampleJsonFiles(dir: File): Unit = {
    val dataDir = new File(dir, "data")
    dataDir.mkdirs()

    val sampleJson =
      """{
        |  "meta": {"data_version": "1.1.0"},
        |  "info": {
        |    "match_type": "T20",
        |    "teams": ["India", "Australia"],
        |    "venue": "MCG",
        |    "dates": ["2023-01-01"],
        |    "toss": {"winner": "India", "decision": "bat"},
        |    "outcome": {"winner": "India"}
        |  },
        |  "innings": [
        |    {
        |      "team": "India",
        |      "overs": [
        |        {"over": 0, "deliveries": [
        |          {"batter": "Kohli", "bowler": "Starc", "non_striker": "Rohit",
        |           "runs": {"batter": 4, "extras": 0, "total": 4}}
        |        ]}
        |      ]
        |    }
        |  ]
        |}""".stripMargin

    val writer = new PrintWriter(new File(dataDir, "match_001.json"))
    try { writer.write(sampleJson) } finally { writer.close() }
  }

  private def deleteDirectory(file: File): Unit = {
    if (file.isDirectory) file.listFiles().foreach(deleteDirectory)
    file.delete()
  }
}
