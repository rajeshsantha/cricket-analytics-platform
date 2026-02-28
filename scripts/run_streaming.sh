#!/bin/bash
# Run streaming pipeline: Bronze → Silver → Gold
# Uses spark-submit from the project-local Spark installation

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

# Load .env
if [ -f "$PROJECT_DIR/.env" ]; then
  export $(grep -v '^#' "$PROJECT_DIR/.env" | xargs)
fi

# Locate spark-submit: prefer project-local Spark, fall back to PATH
if [ -f "$PROJECT_DIR/spark/current/bin/spark-submit" ]; then
  SPARK_SUBMIT="$PROJECT_DIR/spark/current/bin/spark-submit"
elif command -v spark-submit &> /dev/null; then
  SPARK_SUBMIT="spark-submit"
else
  echo "ERROR: spark-submit not found."
  echo "Run: bash scripts/setup_mac.sh"
  exit 1
fi

echo "Using spark-submit: $SPARK_SUBMIT"

JAR="$PROJECT_DIR/target/cricket-analytics-platform-1.0.0-SNAPSHOT-jar-with-dependencies.jar"

if [ ! -f "$JAR" ]; then
  echo "JAR not found. Building project..."
  cd "$PROJECT_DIR" && mvn clean package -DskipTests
fi

echo ""
echo "Starting streaming pipeline (Bronze → Silver → Gold)..."
echo "Kafka broker : ${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"
echo "Delta path   : ${DELTA_BASE_PATH:-/tmp/cricket-delta}"
echo ""

"$SPARK_SUBMIT" \
  --class com.rajesh.cricket.Main \
  --master "local[*]" \
  --driver-memory 2g \
  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
  --conf "spark.streaming.stopGracefullyOnShutdown=true" \
  --conf "spark.sql.shuffle.partitions=4" \
  "$JAR" --mode streaming
