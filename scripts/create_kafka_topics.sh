#!/bin/bash
# Create Kafka topics for cricket analytics platform

KAFKA_HOST="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"
PARTITIONS=3
REPLICATION_FACTOR=1

echo "Creating Kafka topics on $KAFKA_HOST..."

kafka-topics.sh --bootstrap-server "$KAFKA_HOST" \
  --create --if-not-exists \
  --topic cricket-live-balls \
  --partitions $PARTITIONS \
  --replication-factor $REPLICATION_FACTOR

kafka-topics.sh --bootstrap-server "$KAFKA_HOST" \
  --create --if-not-exists \
  --topic cricket-live-matches \
  --partitions $PARTITIONS \
  --replication-factor $REPLICATION_FACTOR

kafka-topics.sh --bootstrap-server "$KAFKA_HOST" \
  --create --if-not-exists \
  --topic cricket-batch \
  --partitions $PARTITIONS \
  --replication-factor $REPLICATION_FACTOR

echo "Topics created:"
kafka-topics.sh --bootstrap-server "$KAFKA_HOST" --list
