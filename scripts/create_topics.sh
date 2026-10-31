#!/bin/bash

docker exec kafka kafka-topics --create \
  --topic transactions.raw \
  --bootstrap-server localhost:9092 \
  --partitions 6 \
  --replication-factor 1 \
  --if-not-exists

docker exec kafka kafka-topics --create \
  --topic transactions.deadletter \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1 \
  --if-not-exists