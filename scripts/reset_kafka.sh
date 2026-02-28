#!/bin/bash
# Delete and recreate all Kafka topics — dev reset, uses docker exec

KAFKA_CONTAINER="${KAFKA_CONTAINER:-cricket-kafka}"
KAFKA_INTERNAL_HOST="localhost:9092"

echo "=== Resetting Kafka topics ==="

for topic in cricket-live-balls cricket-live-matches cricket-batch; do
  echo "Deleting: $topic"
  docker exec "$KAFKA_CONTAINER" kafka-topics \
    --bootstrap-server "$KAFKA_INTERNAL_HOST" \
    --delete --topic "$topic" 2>/dev/null || true
done

echo "Waiting 5s for deletions to propagate..."
sleep 5

echo "Recreating topics..."
bash "$(dirname "$0")/create_kafka_topics.sh"

echo "Reset complete."
