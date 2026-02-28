#!/bin/bash
# Reset Kafka topics (delete and recreate) - use in development only

KAFKA_HOST="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"
TOPICS=("cricket-live-balls" "cricket-live-matches" "cricket-batch")

echo "WARNING: This will delete and recreate all Kafka topics!"
read -p "Are you sure? (y/N): " confirm
if [[ "$confirm" != "y" && "$confirm" != "Y" ]]; then
  echo "Aborted."
  exit 0
fi

for topic in "${TOPICS[@]}"; do
  echo "Deleting topic: $topic"
  kafka-topics.sh --bootstrap-server "$KAFKA_HOST" --delete --topic "$topic" 2>/dev/null || true
done

sleep 3

echo "Recreating topics..."
bash "$(dirname "$0")/create_kafka_topics.sh"

echo "Kafka topics reset complete."
