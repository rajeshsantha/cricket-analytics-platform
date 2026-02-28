#!/bin/bash
# Run batch pipeline: Cricsheet → Bronze → Silver → Gold KPIs

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

if [ -f "$PROJECT_DIR/.env" ]; then
  export $(grep -v '^#' "$PROJECT_DIR/.env" | xargs)
fi

CRICSHEET_DATA_PATH="${1:-$PROJECT_DIR/data/cricsheet}"

# Locate spark-submit
if [ -f "$PROJECT_DIR/spark/current/bin/spark-submit" ]; then
  SPARK_SUBMIT="$PROJECT_DIR/spark/current/bin/spark-submit"
elif command -v spark-submit &> /dev/null; then
  SPARK_SUBMIT="spark-submit"
else
  echo "ERROR: spark-submit not found. Run: bash scripts/setup_mac.sh"
  exit 1
fi

JAR="$PROJECT_DIR/target/cricket-analytics-platform-1.0.0-SNAPSHOT-jar-with-dependencies.jar"

if [ ! -f "$JAR" ]; then
  echo "JAR not found. Building..."
  cd "$PROJECT_DIR" && mvn clean package -DskipTests
fi

echo "Running batch pipeline..."
echo "Cricsheet data path: $CRICSHEET_DATA_PATH"
echo ""

"$SPARK_SUBMIT" \
  --class com.rajesh.cricket.Main \
  --master "local[*]" \
  --driver-memory 4g \
  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
  --conf "spark.sql.shuffle.partitions=8" \
  "$JAR" --mode batch --data-path "$CRICSHEET_DATA_PATH"
