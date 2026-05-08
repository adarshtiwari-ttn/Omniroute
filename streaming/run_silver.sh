#!/bin/bash

set -e

MASTER_IP=$(hostname -I | awk '{print $1}')
KAFKA_SERVER="${MASTER_IP}:9092"

spark-submit \
  --deploy-mode client \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  --conf spark.executor.memory=2g \
  --conf spark.driver.memory=1g \
  --conf spark.executor.cores=2 \
  --conf spark.yarn.maxAppAttempts=1 \
  s3://ttn-de-bootcamp-gold-us-east-1/poc-bootcamp-group1-gold/streaming/telemetry_silver_cleaner.py \
  --logical_date 2026-05-08 \
  --kafka_bootstrap "$KAFKA_SERVER" \
  --input_topic omniroute.telemetry.bronze \
  --output_topic omniroute.telemetry.silver \
  --checkpoint_path s3://ttn-de-bootcamp-gold-us-east-1/poc-bootcamp-group1-gold/streaming/checkpoints/silver/ \
  --bad_path s3://ttn-de-bootcamp-bronze-us-east-1/poc-bootcamp-group1-bronze/bad_data/telemetry_stream/ \
  --trigger_seconds 30