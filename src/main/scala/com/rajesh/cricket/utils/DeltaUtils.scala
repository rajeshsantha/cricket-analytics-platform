package com.rajesh.cricket.utils

import org.apache.spark.sql.SparkSession

/** Utility helpers for Delta Lake table management. */
object DeltaUtils {

  /**
   * Run OPTIMIZE on a Delta table to compact small files.
   *
   * @param path   Delta table path
   * @param spark  SparkSession
   */
  def optimizeTable(path: String)(implicit spark: SparkSession): Unit = {
    spark.sql(s"OPTIMIZE delta.`$path`")
  }

  /**
   * Run VACUUM on a Delta table to remove old data files.
   *
   * @param path            Delta table path
   * @param retentionHours  Minimum retention period in hours (default 168 = 7 days)
   * @param spark           SparkSession
   */
  def vacuumTable(path: String, retentionHours: Int = 168)(implicit spark: SparkSession): Unit = {
    spark.sql(s"VACUUM delta.`$path` RETAIN $retentionHours HOURS")
  }

  /**
   * Perform a MERGE (upsert) into a Delta table.
   *
   * @param targetPath   Delta table path (target)
   * @param sourceDf     Source DataFrame alias name used in the merge condition
   * @param mergeCondition  SQL condition string (e.g., "target.id = source.id")
   * @param spark        SparkSession
   */
  def mergeIntoTable(
    targetPath: String,
    sourceTable: String,
    mergeCondition: String
  )(implicit spark: SparkSession): Unit = {
    spark.sql(
      s"""
         |MERGE INTO delta.`$targetPath` AS target
         |USING $sourceTable AS source
         |ON $mergeCondition
         |WHEN MATCHED THEN UPDATE SET *
         |WHEN NOT MATCHED THEN INSERT *
         |""".stripMargin
    )
  }

  /**
   * Return basic statistics about a Delta table.
   *
   * @param path   Delta table path
   * @param spark  SparkSession
   * @return       Tuple of (row count, last modified timestamp as string)
   */
  def getTableStats(path: String)(implicit spark: SparkSession): (Long, String) = {
    val df = spark.read.format("delta").load(path)
    val rowCount = df.count()
    val lastModified = spark.sql(
      s"DESCRIBE DETAIL delta.`$path`"
    ).select("lastModified").collect().headOption.map(_.toString).getOrElse("unknown")
    (rowCount, lastModified)
  }
}
