#!/bin/bash
# Create Kafka topics via docker exec — no host Kafka installation required

KAFKA_CONTAINER="${KAFKA_CONTAINER:-cricket-kafka}"
KAFKA_INTERNAL_HOST="localhost:9092"
PARTITIONS=3
REPLICATION_FACTOR=1

echo "Waiting for Kafka broker inside container '$KAFKA_CONTAINER'..."
until docker exec "$KAFKA_CONTAINER" kafka-topics \
    --bootstrap-server "$KAFKA_INTERNAL_HOST" --list > /dev/null 2>&1; do
  echo "  Kafka not ready yet, retrying in 3s..."
  sleep 3
done
echo "Kafka is ready."

create_topic() {
  local topic=$1
  echo "Creating topic: $topic"
  docker exec "$KAFKA_CONTAINER" kafka-topics \
    --bootstrap-server "$KAFKA_INTERNAL_HOST" \
    --create --if-not-exists \
    --topic "$topic" \
    --partitions $PARTITIONS \
    --replication-factor $REPLICATION_FACTOR
}

create_topic "cricket-live-balls"
create_topic "cricket-live-matches"
create_topic "cricket-batch"

echo ""
echo "Topics available:"
docker exec "$KAFKA_CONTAINER" kafka-topics \
  --bootstrap-server "$KAFKA_INTERNAL_HOST" --list
