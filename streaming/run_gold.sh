#!/bin/bash

set -e

MASTER_IP=$(hostname -I | awk '{print $1}')
KAFKA_SERVER="${MASTER_IP}:9092"
REDIS_HOST="${MASTER_IP}"

spark-submit \
  --deploy-mode client \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,io.delta:delta-spark_2.12:3.2.0,org.postgresql:postgresql:42.7.3 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --conf spark.executor.memory=2g \
  --conf spark.driver.memory=1g \
  --conf spark.executor.cores=2 \
  --conf spark.yarn.maxAppAttempts=1 \
  s3://ttn-de-bootcamp-gold-us-east-1/poc-bootcamp-group1-gold/streaming/telemetry_gold_violations_status.py \
  --logical_date 2026-05-08 \
  --kafka_bootstrap "$KAFKA_SERVER" \
  --input_topic omniroute.telemetry.silver \
  --restricted_zones_path s3://ttn-de-bootcamp-bronze-us-east-1/poc-bootcamp-group1-bronze/static/restricted_zones.json \
  --driver_violation_events_path s3://ttn-de-bootcamp-gold-us-east-1/poc-bootcamp-group1-gold/driver_violation_events/ \
  --driver_safety_status_path s3://ttn-de-bootcamp-gold-us-east-1/poc-bootcamp-group1-gold/driver_safety_status/ \
  --checkpoint_path s3://ttn-de-bootcamp-gold-us-east-1/poc-bootcamp-group1-gold/streaming/checkpoints/gold/ \
  --redis_host "$REDIS_HOST" \
  --redis_port 6379 \
  --pg_host "<POSTGRES_HOST>" \
  --pg_port 5432 \
  --pg_database report \
  --pg_user postgres \
  --pg_password "<POSTGRES_PASSWORD>" \
  --pg_driver_events_table public.driver_safety_events \
  --pg_driver_safety_table public.driver_safety_status \
  --trigger_seconds 30