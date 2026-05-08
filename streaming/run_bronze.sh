#!/bin/bash

set -e

MASTER_IP=$(hostname -I | awk '{print $1}')
KAFKA_SERVER="${MASTER_IP}:9092"

spark-submit \
  --deploy-mode client \
  --packages io.delta:delta-spark_2.12:3.2.0 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --conf spark.executor.memory=2g \
  --conf spark.driver.memory=1g \
  --conf spark.executor.cores=2 \
  --conf spark.yarn.maxAppAttempts=1 \
  s3://ttn-de-bootcamp-gold-us-east-1/poc-bootcamp-group1-gold/streaming/telemetry_bronze_producer.py \
  --logical_date 2026-05-08 \
  --kafka_bootstrap "$KAFKA_SERVER" \
  --asset_history_path s3://ttn-de-bootcamp-gold-us-east-1/poc-bootcamp-group1-gold/asset_history_scd2/ \
  --output_topic omniroute.telemetry.bronze \
  --refresh_interval_seconds 300 \
  --messages_per_vehicle 5 \
  --sleep_ms 5