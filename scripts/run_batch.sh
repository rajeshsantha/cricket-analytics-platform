#!/bin/bash
# Run batch pipeline (Cricsheet → Bronze → Silver → Gold KPIs)

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

if [ -f "$PROJECT_DIR/.env" ]; then
  export $(grep -v '^#' "$PROJECT_DIR/.env" | xargs)
fi

CRICSHEET_DATA_PATH="${1:-$PROJECT_DIR/data/cricsheet}"
JAR="$PROJECT_DIR/target/cricket-analytics-platform-1.0.0-SNAPSHOT-jar-with-dependencies.jar"

if [ ! -f "$JAR" ]; then
  echo "JAR not found. Building..."
  cd "$PROJECT_DIR" && mvn clean package -DskipTests
fi

echo "Running batch pipeline on: $CRICSHEET_DATA_PATH"

java \
  -Xmx4g \
  -Dspark.master="${SPARK_MASTER:-local[*]}" \
  -Dspark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  -Dspark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  -cp "$JAR" \
  com.rajesh.cricket.Main --mode batch --data-path "$CRICSHEET_DATA_PATH"
