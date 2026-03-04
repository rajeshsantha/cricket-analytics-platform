#!/bin/bash
# Delete and recreate all Kafka topics (dev reset) — uses docker exec

KAFKA_CONTAINER="${KAFKA_CONTAINER:-cricket-kafka}"
KAFKA_INTERNAL_HOST="localhost:9092"

echo "=== Resetting Kafka topics ==="

echo "WARNING: This will delete and recreate all Kafka topics!"
read -p "Are you sure? (y/N): " confirm
if [[ "$confirm" != "y" && "$confirm" != "Y" ]]; then
  echo "Aborted."
  exit 0
fi

delete_topic() {
  local topic=$1
  echo "Deleting topic: $topic"
  docker exec "$KAFKA_CONTAINER" kafka-topics \
    --bootstrap-server "$KAFKA_INTERNAL_HOST" \
    --delete --if-exists \
    --topic "$topic" 2>/dev/null || true
}

delete_topic "cricket-live-balls"
delete_topic "cricket-live-matches"
delete_topic "cricket-batch"

echo "Waiting for deletions to propagate..."
sleep 5

echo "Recreating topics..."
bash "$(dirname "$0")/create_kafka_topics.sh"

echo "Reset complete."
