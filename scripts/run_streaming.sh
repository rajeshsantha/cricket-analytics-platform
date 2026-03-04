#!/bin/bash
# Run streaming pipeline — no host spark-submit needed, runs fat JAR with java

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

# Load environment variables
if [ -f "$PROJECT_DIR/.env" ]; then
  export $(grep -v '^#' "$PROJECT_DIR/.env" | xargs)
fi

JAR="$PROJECT_DIR/target/cricket-analytics-platform-1.0.0-SNAPSHOT-jar-with-dependencies.jar"

if [ ! -f "$JAR" ]; then
  echo "JAR not found. Building project..."
  cd "$PROJECT_DIR" && mvn clean package -DskipTests
fi

echo "Starting streaming pipeline (Bronze → Silver → Gold)..."
echo "Kafka: ${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"
echo "Delta base: ${DELTA_BASE_PATH:-/tmp/cricket-delta}"

java \
  -Xmx2g \
  -Dspark.master="${SPARK_MASTER:-local[*]}" \
  -Dspark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  -Dspark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  -cp "$JAR" \
  com.rajesh.cricket.Main --mode streaming
