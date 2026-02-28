// Notebook: 03 - Streaming Demo
// Demonstrates watermarking, stateful aggregation, and windowing concepts
// Run as: spark-shell -i notebooks/03_streaming_demo.scala

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._

val spark = SparkSession.builder()
  .appName("Streaming Demo")
  .master("local[*]")
  .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
  .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
  .config("spark.sql.shuffle.partitions", "4")
  .getOrCreate()

import spark.implicits._

// ─── Concept 1: Watermarking ──────────────────────────────────────────────────
// Watermarking tells Spark how late data can arrive.
// With "10 minutes" watermark: any event older than (max_event_time - 10min) is dropped.
//
// This allows Spark to clean up state for completed windows while still
// tolerating out-of-order data delivery from Kafka.

println("""
=== WATERMARKING CONCEPT ===
A watermark of "10 minutes" means:
  - Spark tracks the maximum event_time seen so far
  - Data arriving with event_time < (max_seen - 10min) is considered "too late"
  - Late data is silently dropped
  - Allows Spark to garbage-collect old window state
""")

// ─── Concept 2: Windowed Aggregation ─────────────────────────────────────────
// Window functions aggregate data within a time-bounded window.
// Tumbling window: non-overlapping, e.g., window("5 minutes") 
// Sliding window: overlapping, e.g., window("5 minutes", "1 minute")
//
// Example: Run rate per match in a 5-minute sliding window

println("""
=== WINDOWED AGGREGATION EXAMPLE ===
groupBy(match_id, window(event_time, "5 minutes", "1 minute"))
  .agg(sum("runs_total") / count("*") * 6)
  = Live run rate every 1 minute, using 5-minute rolling window
""")

// ─── Concept 3: Stateful Streaming ───────────────────────────────────────────
// Spark maintains state across micro-batches for aggregations.
// Without watermark: state grows forever (memory leak risk)
// With watermark: state is bounded and regularly cleaned up

println("""
=== STATEFUL STREAMING ===
- Bronze layer: stateless (just write raw JSON)
- Silver layer: stateless transformation (parse + flatten)
- Gold layer: STATEFUL (aggregations across time windows)

The key is: watermark + window = bounded, fault-tolerant stateful aggregation
""")

// ─── Demo: Simulated Ball Stream (using rate source) ─────────────────────────
// This demonstrates the streaming pipeline structure without needing real Kafka

val simulatedStream = spark.readStream
  .format("rate")
  .option("rowsPerSecond", "6") // 6 balls per second = 1 over/second
  .load()
  .withColumn("match_id",   lit("demo-match-001"))
  .withColumn("runs_total", (rand() * 6).cast("int"))
  .withColumn("is_wicket",  (rand() < 0.1))
  .withColumnRenamed("timestamp", "event_time")

// Apply watermark and windowed aggregation
val runRate = simulatedStream
  .withWatermark("event_time", "30 seconds")
  .groupBy(
    col("match_id"),
    window(col("event_time"), "30 seconds", "5 seconds")
  )
  .agg(
    sum("runs_total").as("window_runs"),
    count("*").as("balls"),
    sum(col("is_wicket").cast("int")).as("wickets"),
    (sum("runs_total") * 6.0 / count("*")).as("run_rate")
  )
  .select(
    col("match_id"),
    col("window.start").as("window_start"),
    col("window_runs"),
    col("balls"),
    col("wickets"),
    round(col("run_rate"), 2).as("run_rate")
  )

println("Starting simulated streaming run rate demo (30 seconds)...")
val query = runRate.writeStream
  .format("console")
  .outputMode("update")
  .option("truncate", "false")
  .start()

query.awaitTermination(30000) // run for 30 seconds
query.stop()

println("Streaming demo complete!")
spark.stop()
