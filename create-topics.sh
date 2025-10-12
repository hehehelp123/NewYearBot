#!/bin/bash

echo "Waiting for Kafka to be reachable..."

# Loop until the kafka-topics command is successful
until kafka-topics --bootstrap-server kafka:29092 --list > /dev/null 2>&1; do
  echo "Kafka is not yet available, waiting 5 seconds..."
  sleep 5
done

echo "Kafka is ready! Creating topics..."

# Create topics
kafka-topics --create --if-not-exists --topic user.user.created --bootstrap-server kafka:29092 --partitions 1 --replication-factor 1
kafka-topics --create --if-not-exists --topic ticket.ticket.created --bootstrap-server kafka:29092 --partitions 1 --replication-factor 1
kafka-topics --create --if-not-exists --topic selenium.upload.success --bootstrap-server kafka:29092 --partitions 1 --replication-factor 1

echo "Kafka topics created successfully."