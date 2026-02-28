package com.rajesh.cricket.silver

import com.rajesh.cricket.config.SparkSessionFactory
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.functions._

/**
 * Unit tests for SilverBatchJob.
 * Verifies the JSON flattening logic and data quality checks.
 */
class SilverJobSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  implicit var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSessionFactory.createForTest("SilverJobSpec")
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
  }

  "SilverBatchJob.applyDataQuality" should "filter out rows with null batsman" in {
    val ss = spark; import ss.implicits._

    val df = Seq(
      ("Kohli",  "Starc", 4, 0, 4),
      (null,     "Cummins", 0, 1, 1),
      ("Rohit",  "Hazlewood", 1, 0, 1)
    ).toDF("batsman", "bowler", "runs_batsman", "runs_extras", "runs_total")
     .withColumn("over_num", lit(0))
     .withColumn("wicket_kind", lit(null).cast("string"))

    val result = SilverBatchJob.applyDataQuality(df)

    result.count() shouldBe 2
    result.filter(col("batsman").isNull).count() shouldBe 0
  }

  it should "filter out rows with null bowler" in {
    val ss = spark; import ss.implicits._

    val df = Seq(
      ("Kohli", "Starc",   4, 0, 4),
      ("Rohit", null,      1, 0, 1)
    ).toDF("batsman", "bowler", "runs_batsman", "runs_extras", "runs_total")
     .withColumn("over_num", lit(0))
     .withColumn("wicket_kind", lit(null).cast("string"))

    val result = SilverBatchJob.applyDataQuality(df)

    result.count() shouldBe 1
  }

  it should "filter out rows with invalid run totals (> 36)" in {
    val ss = spark; import ss.implicits._

    val df = Seq(
      ("Kohli", "Starc",  4, 0,  4),
      ("Rohit", "Cummins", 0, 0, 37) // invalid - impossible ball total
    ).toDF("batsman", "bowler", "runs_batsman", "runs_extras", "runs_total")
     .withColumn("over_num", lit(0))
     .withColumn("wicket_kind", lit(null).cast("string"))

    val result = SilverBatchJob.applyDataQuality(df)

    result.count() shouldBe 1
  }

  it should "keep valid rows intact" in {
    val ss = spark; import ss.implicits._

    val df = Seq(
      ("Kohli",  "Starc",    4, 0, 4),
      ("Rohit",  "Cummins",  6, 0, 6),
      ("Dhoni",  "Hazlewood",1, 0, 1)
    ).toDF("batsman", "bowler", "runs_batsman", "runs_extras", "runs_total")
     .withColumn("over_num", lit(5))
     .withColumn("wicket_kind", lit(null).cast("string"))

    val result = SilverBatchJob.applyDataQuality(df)

    result.count() shouldBe 3
  }
}
