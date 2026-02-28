// Notebook: 01 - Cricsheet EDA (Exploratory Data Analysis)
// Run as a Spark script: spark-shell -i notebooks/01_cricsheet_eda.scala

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

val spark = SparkSession.builder()
  .appName("Cricsheet EDA")
  .master("local[*]")
  .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
  .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
  .getOrCreate()

import spark.implicits._

// ─── 1. Load Cricsheet data ───────────────────────────────────────────────────
val dataPath = sys.env.getOrElse("CRICSHEET_PATH", "/tmp/cricsheet-data")
println(s"Loading data from: $dataPath")

val rawDf = spark.read
  .option("multiLine", "true")
  .json(s"$dataPath/*.json")

// ─── 2. Show schema ──────────────────────────────────────────────────────────
println("=== Schema ===")
rawDf.printSchema()

// ─── 3. Row counts ───────────────────────────────────────────────────────────
println(s"Total match files: ${rawDf.count()}")

// ─── 4. Match type distribution ──────────────────────────────────────────────
println("=== Match Type Distribution ===")
rawDf.groupBy("info.match_type")
  .count()
  .orderBy(desc("count"))
  .show()

// ─── 5. Explode deliveries for ball-level analysis ───────────────────────────
val deliveries = rawDf
  .withColumn("match_type", col("info.match_type"))
  .withColumn("team1",      col("info.teams").getItem(0))
  .withColumn("team2",      col("info.teams").getItem(1))
  .withColumn("venue",      col("info.venue"))
  .withColumn("innings_data", explode(col("innings")))
  .withColumn("over_data",    explode(col("innings_data.overs")))
  .withColumn("delivery",     explode(col("over_data.deliveries")))
  .withColumn("over_num",     col("over_data.over"))
  .withColumn("batsman",      col("delivery.batter"))
  .withColumn("bowler",       col("delivery.bowler"))
  .withColumn("runs_total",   col("delivery.runs.total"))

println(s"Total deliveries: ${deliveries.count()}")

// ─── 6. Check nulls on key columns ───────────────────────────────────────────
println("=== Null counts on key columns ===")
val keyCols = Seq("batsman", "bowler", "runs_total", "over_num")
keyCols.foreach { c =>
  val nullCount = deliveries.filter(col(c).isNull).count()
  println(s"  $c: $nullCount nulls")
}

// ─── 7. Distribution of runs per ball ────────────────────────────────────────
println("=== Distribution of Runs per Ball ===")
deliveries.groupBy("runs_total")
  .count()
  .orderBy("runs_total")
  .show()

// ─── 8. Top 10 venues by match count ─────────────────────────────────────────
println("=== Top 10 Venues ===")
rawDf.groupBy("info.venue")
  .count()
  .orderBy(desc("count"))
  .limit(10)
  .show(truncate = false)

// ─── 9. Sample deliveries ────────────────────────────────────────────────────
println("=== Sample Deliveries ===")
deliveries.select("team1", "team2", "batsman", "bowler", "over_num", "runs_total")
  .show(20, truncate = false)

spark.stop()
