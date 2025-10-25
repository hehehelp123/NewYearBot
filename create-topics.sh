#!/bin/bash
echo "Waiting for Kafka to be reachable..."
until kafka-topics --bootstrap-server kafka:29092 --list > /dev/null 2>&1; do
  echo "Kafka is not yet available, waiting 5 seconds..."
  sleep 5
done
echo "Kafka is ready! Creating topics..."

# --- User Service Topics ---
kafka-topics --create --if-not-exists --topic user.user.create --bootstrap-server kafka:29092 --partitions 1 --replication-factor 1
kafka-topics --create --if-not-exists --topic user.user.allow_request --bootstrap-server kafka:29092 --partitions 1 --replication-factor 1
kafka-topics --create --if-not-exists --topic user.user.disallow_request --bootstrap-server kafka:29092 --partitions 1 --replication-factor 1
kafka-topics --create --if-not-exists --topic user.user.list_request --bootstrap-server kafka:29092 --partitions 1 --replication-factor 1
kafka-topics --create --if-not-exists --topic user.user.allowed --bootstrap-server kafka:29092 --partitions 1 --replication-factor 1
kafka-topics --create --if-not-exists --topic user.user.disallowed --bootstrap-server kafka:29092 --partitions 1 --replication-factor 1
kafka-topics --create --if-not-exists --topic user.user.list_response --bootstrap-server kafka:29092 --partitions 1 --replication-factor 1

# --- Ticket Service Topics ---
kafka-topics --create --if-not-exists --topic ticket.create.requested --bootstrap-server kafka:29092 --partitions 1 --replication-factor 1
kafka-topics --create --if-not-exists --topic ticket.list.request --bootstrap-server kafka:29092 --partitions 1 --replication-factor 1
kafka-topics --create --if-not-exists --topic ticket.download.request --bootstrap-server kafka:29092 --partitions 1 --replication-factor 1
kafka-topics --create --if-not-exists --topic ticket.delete.request --bootstrap-server kafka:29092 --partitions 1 --replication-factor 1
kafka-topics --create --if-not-exists --topic ticket.created --bootstrap-server kafka:29092 --partitions 1 --replication-factor 1
kafka-topics --create --if-not-exists --topic ticket.list.retrieved --bootstrap-server kafka:29092 --partitions 1 --replication-factor 1

# --- Notification Service Topics ---
kafka-topics --create --if-not-exists --topic notification.send --bootstrap-server kafka:29092 --partitions 1 --replication-factor 1
kafka-topics --create --if-not-exists --topic notification.schedule --bootstrap-server kafka:29092 --partitions 1 --replication-factor 1
kafka-topics --create --if-not-exists --topic notification.schedule.document --bootstrap-server kafka:29092 --partitions 1 --replication-factor 1
kafka-topics --create --if-not-exists --topic notification.send.document --bootstrap-server kafka:29092 --partitions 1 --replication-factor 1
kafka-topics --create --if-not-exists --topic notification.send.tickets --bootstrap-server kafka:29092 --partitions 1 --replication-factor 1

# --- Wishlist Service Topics ---
kafka-topics --create --if-not-exists --topic wishlist.wishlist.create --bootstrap-server kafka:29092 --partitions 1 --replication-factor 1
kafka-topics --create --if-not-exists --topic wishlist.item.add --bootstrap-server kafka:29092 --partitions 1 --replication-factor 1
kafka-topics --create --if-not-exists --topic wishlist.item.delete --bootstrap-server kafka:29092 --partitions 1 --replication-factor 1
kafka-topics --create --if-not-exists --topic wishlist.item.book --bootstrap-server kafka:29092 --partitions 1 --replication-factor 1
kafka-topics --create --if-not-exists --topic wishlist.item.unbook --bootstrap-server kafka:29092 --partitions 1 --replication-factor 1
kafka-topics --create --if-not-exists --topic wishlist.view.owner --bootstrap-server kafka:29092 --partitions 1 --replication-factor 1
kafka-topics --create --if-not-exists --topic wishlist.view.viewer --bootstrap-server kafka:29092 --partitions 1 --replication-factor 1
kafka-topics --create --if-not-exists --topic wishlist.view.owner_success --bootstrap-server kafka:29092 --partitions 1 --replication-factor 1
kafka-topics --create --if-not-exists --topic wishlist.view.owner_failed --bootstrap-server kafka:29092 --partitions 1 --replication-factor 1
kafka-topics --create --if-not-exists --topic wishlist.view.viewer_success --bootstrap-server kafka:29092 --partitions 1 --replication-factor 1
kafka-topics --create --if-not-exists --topic wishlist.view.viewer_failed --bootstrap-server kafka:29092 --partitions 1 --replication-factor 1
kafka-topics --create --if-not-exists --topic wishlist.scrape.request --bootstrap-server kafka:29092 --partitions 1 --replication-factor 1
kafka-topics --create --if-not-exists --topic wishlist.scrape.success --bootstrap-server kafka:29092 --partitions 1 --replication-factor 1
kafka-topics --create --if-not-exists --topic wishlist.scrape.failed --bootstrap-server kafka:29092 --partitions 1 --replication-factor 1
kafka-topics --create --if-not-exists --topic wishlist.view.all --bootstrap-server kafka:29092 --partitions 1 --replication-factor 1

# --- Music Service Topics ---
kafka-topics --create --if-not-exists --topic music.download.request --bootstrap-server kafka:29092 --partitions 1 --replication-factor 1
kafka-topics --create --if-not-exists --topic music.download.success --bootstrap-server kafka:29092 --partitions 1 --replication-factor 1
kafka-topics --create --if-not-exists --topic music.download.failed --bootstrap-server kafka:29092 --partitions 1 --replication-factor 1
kafka-topics --create --if-not-exists --topic music.upload.request --bootstrap-server kafka:29092 --partitions 1 --replication-factor 1
kafka-topics --create --if-not-exists --topic music.upload.success --bootstrap-server kafka:29092 --partitions 1 --replication-factor 1
kafka-topics --create --if-not-exists --topic music.upload.failed --bootstrap-server kafka:29092 --partitions 1 --replication-factor 1
kafka-topics --create --if-not-exists --topic music.sync.request --bootstrap-server kafka:29092 --partitions 1 --replication-factor 1
kafka-topics --create --if-not-exists --topic music.sync.completed --bootstrap-server kafka:29092 --partitions 1 --replication-factor 1

# --- Other/Legacy Topics ---
kafka-topics --create --if-not-exists --topic selenium.upload.success --bootstrap-server kafka:29092 --partitions 1 --replication-factor 1

echo "All topics created successfully."