#!/bin/bash
echo "Waiting for Kafka to be reachable..."
until kafka-topics --bootstrap-server kafka:29092 --list > /dev/null 2>&1; do
  echo "Kafka is not yet available, waiting 5 seconds..."
  sleep 5
done
echo "Kafka is ready! Creating topics..."

kafka-topics --create --if-not-exists --topic user.user.created --bootstrap-server kafka:29092 --partitions 1 --replication-factor 1
kafka-topics --create --if-not-exists --topic selenium.upload.success --bootstrap-server kafka:29092 --partitions 1 --replication-factor 1
kafka-topics --create --if-not-exists --topic ticket.create.requested --bootstrap-server kafka:29092 --partitions 1 --replication-factor 1
kafka-topics --create --if-not-exists --topic ticket.list.request --bootstrap-server kafka:29092 --partitions 1 --replication-factor 1
kafka-topics --create --if-not-exists --topic ticket.download.request --bootstrap-server kafka:29092 --partitions 1 --replication-factor 1
kafka-topics --create --if-not-exists --topic ticket.delete.request --bootstrap-server kafka:29092 --partitions 1 --replication-factor 1
kafka-topics --create --if-not-exists --topic ticket.created --bootstrap-server kafka:29092 --partitions 1 --replication-factor 1
kafka-topics --create --if-not-exists --topic ticket.list.retrieved --bootstrap-server kafka:29092 --partitions 1 --replication-factor 1
kafka-topics --create --if-not-exists --topic notification.send --bootstrap-server kafka:29092 --partitions 1 --replication-factor 1
kafka-topics --create --if-not-exists --topic notification.schedule --bootstrap-server kafka:29092 --partitions 1 --replication-factor 1
kafka-topics --create --if-not-exists --topic notification.schedule.document --bootstrap-server kafka:29092 --partitions 1 --replication-factor 1
kafka-topics --create --if-not-exists --topic notification.send.document --bootstrap-server kafka:29092 --partitions 1 --replication-factor 1
kafka-topics --create --if-not-exists --topic notification.send.tickets --bootstrap-server kafka:29092 --partitions 1 --replication-factor 1

echo "Kafka topics created successfully."