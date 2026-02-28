package com.rajesh.cricket.utils

import org.apache.spark.sql.types._

/** Centralized Spark StructType schema definitions for all data sources. */
object SchemaUtils {

  /** Schema for a live ball event received from CricAPI via Kafka. */
  val liveBallSchema: StructType = StructType(Seq(
    StructField("matchId",      StringType,  nullable = false),
    StructField("inning",       StringType,  nullable = true),
    StructField("over",         IntegerType, nullable = true),
    StructField("ball",         IntegerType, nullable = true),
    StructField("batsman",      StringType,  nullable = true),
    StructField("bowler",       StringType,  nullable = true),
    StructField("runs", StructType(Seq(
      StructField("batsman",    IntegerType, nullable = true),
      StructField("extras",     IntegerType, nullable = true),
      StructField("total",      IntegerType, nullable = true)
    )), nullable = true),
    StructField("wicket", StructType(Seq(
      StructField("kind",       StringType,  nullable = true),
      StructField("player_out", StringType,  nullable = true)
    )), nullable = true),
    StructField("eventTime",    StringType,  nullable = true)
  ))

  /** Schema for a Cricsheet delivery (ball) record. */
  val cricsheetDeliverySchema: StructType = StructType(Seq(
    StructField("matchId",         StringType,  nullable = false),
    StructField("inning",          IntegerType, nullable = true),
    StructField("over",            IntegerType, nullable = true),
    StructField("ball",            IntegerType, nullable = true),
    StructField("batsman",         StringType,  nullable = true),
    StructField("bowler",          StringType,  nullable = true),
    StructField("non_striker",     StringType,  nullable = true),
    StructField("runs_batsman",    IntegerType, nullable = true),
    StructField("runs_extras",     IntegerType, nullable = true),
    StructField("runs_total",      IntegerType, nullable = true),
    StructField("wicket_kind",     StringType,  nullable = true),
    StructField("wicket_player_out", StringType, nullable = true)
  ))

  /** Schema for a Cricsheet match info record. */
  val cricsheetMatchSchema: StructType = StructType(Seq(
    StructField("matchId",       StringType, nullable = false),
    StructField("team1",         StringType, nullable = true),
    StructField("team2",         StringType, nullable = true),
    StructField("venue",         StringType, nullable = true),
    StructField("date",          StringType, nullable = true),
    StructField("match_type",    StringType, nullable = true),
    StructField("winner",        StringType, nullable = true),
    StructField("toss_winner",   StringType, nullable = true),
    StructField("toss_decision", StringType, nullable = true)
  ))

  /**
   * Load a JSON schema definition from the resources/schemas directory.
   *
   * @param fileName  The schema file name (e.g., "cricapi_live_ball_schema.json")
   * @return          Schema as a String
   */
  def loadSchemaJson(fileName: String): String = {
    val stream = getClass.getResourceAsStream(s"/schemas/$fileName")
    if (stream == null) throw new IllegalArgumentException(s"Schema file not found: $fileName")
    scala.io.Source.fromInputStream(stream).mkString
  }
}
