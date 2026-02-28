#!/bin/bash
# Run batch ingestion pipeline (Cricsheet → Bronze → Silver → Gold)

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
CRICSHEET_PATH="${1:-/tmp/cricsheet-data}"

# Load environment variables
if [ -f "$PROJECT_DIR/.env" ]; then
  export $(grep -v '^#' "$PROJECT_DIR/.env" | xargs)
fi

JAR="$PROJECT_DIR/target/cricket-analytics-platform-1.0.0-SNAPSHOT-jar-with-dependencies.jar"

if [ ! -f "$JAR" ]; then
  echo "JAR not found. Building project..."
  cd "$PROJECT_DIR" && mvn clean package -DskipTests
fi

echo "Starting batch pipeline with data path: $CRICSHEET_PATH"
spark-submit \
  --class com.rajesh.cricket.Main \
  --master "${SPARK_MASTER:-local[*]}" \
  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
  --conf "spark.driver.memory=4g" \
  --conf "spark.executor.memory=4g" \
  "$JAR" --mode batch --data-path "$CRICSHEET_PATH"
