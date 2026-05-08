import argparse
import json
import os
import random
import time
from datetime import datetime, timedelta, timezone

from kafka import KafkaProducer
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


DEFAULT_ASSET_HISTORY_PATH = (
    "s3://ttn-de-bootcamp-gold-us-east-1/"
    "poc-bootcamp-group1-gold/asset_history_scd2/"
)

DEFAULT_OUTPUT_TOPIC = "omniroute.telemetry.bronze"

REFRESH_INTERVAL_SECONDS = 300
DEFAULT_MESSAGES_PER_VEHICLE = 5
DEFAULT_SLEEP_MS = 5


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument("--logical_date", default="2026-05-08")
    parser.add_argument("--kafka_bootstrap", default=os.environ.get("KAFKA_SERVER"))
    parser.add_argument("--asset_history_path", default=DEFAULT_ASSET_HISTORY_PATH)
    parser.add_argument("--output_topic", default=DEFAULT_OUTPUT_TOPIC)

    parser.add_argument("--refresh_interval_seconds", type=int, default=REFRESH_INTERVAL_SECONDS)
    parser.add_argument("--messages_per_vehicle", type=int, default=DEFAULT_MESSAGES_PER_VEHICLE)
    parser.add_argument("--sleep_ms", type=int, default=DEFAULT_SLEEP_MS)

    parser.add_argument("--bad_record_probability", type=float, default=0.02)
    parser.add_argument("--speed_min", type=int, default=25)
    parser.add_argument("--speed_max", type=int, default=200)

    args = parser.parse_args()

    if not args.kafka_bootstrap:
        raise ValueError("Kafka bootstrap server is required. Pass --kafka_bootstrap or set KAFKA_SERVER env variable.")

    return args


def bad_or_value(value, bad_values, prob):
    if random.random() < prob:
        return random.choice(bad_values)
    return value


def load_active_pairs(spark, asset_history_path):
    asset_df = (
        spark.read
        .format("delta")
        .load(asset_history_path)
        .filter(F.col("status") == F.lit("IN-TRANSIT"))
        .select("vin", "driver_id", "daily_rate")
        .withColumn("vin", F.upper(F.trim(F.col("vin"))))
        .withColumn("driver_id", F.upper(F.trim(F.col("driver_id"))))
        .withColumn("daily_rate", F.col("daily_rate").cast("double"))
        .filter(
            F.col("vin").isNotNull() &
            F.col("driver_id").isNotNull() &
            F.col("daily_rate").isNotNull() &
            (F.col("daily_rate") > 0)
        )
        .dropDuplicates(["vin"])
    )

    return [
        {
            "vin": row["vin"],
            "driver_id": row["driver_id"],
            "daily_rate": row["daily_rate"],
        }
        for row in asset_df.collect()
    ]


def build_event(row, event_time, args):
    bad_vins = ["", None, "BADVIN", "123456789999", "VIN@@123"]
    bad_drivers = ["", None, "DRV-X", "DRIVER_1", "12345"]
    bad_speeds = [-10, 251, 999, None, "fast"]
    bad_lats = [-100, 100, None, "lat"]
    bad_longs = [-200, 200, None, "long"]

    speed = random.randint(args.speed_min, args.speed_max)
    lat = random.uniform(-90, 90)
    lon = random.uniform(-180, 180)

    return {
        "vin": bad_or_value(row["vin"], bad_vins, args.bad_record_probability),
        "driver_id": bad_or_value(row["driver_id"], bad_drivers, args.bad_record_probability),
        "speed": bad_or_value(speed, bad_speeds, args.bad_record_probability),
        "lat": bad_or_value(round(lat, 6), bad_lats, args.bad_record_probability),
        "long": bad_or_value(round(lon, 6), bad_longs, args.bad_record_probability),
        "event_timestamp": event_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "base_rate": float(row["daily_rate"]),
    }


def main():
    args = parse_args()

    spark = (
        SparkSession.builder
        .appName("OmniRouteTelemetryBronzeProducer")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )

    producer = KafkaProducer(
        bootstrap_servers=args.kafka_bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda v: str(v).encode("utf-8"),
        linger_ms=5,
        retries=3,
    )

    logical_start = datetime.strptime(args.logical_date, "%Y-%m-%d").replace(
        tzinfo=timezone.utc
    )

    active_pairs = []
    last_refresh_time = 0
    event_counter = 0
    total_sent = 0

    try:
        while True:
            current_time = time.time()

            if not active_pairs or current_time - last_refresh_time >= args.refresh_interval_seconds:
                active_pairs = load_active_pairs(
                    spark=spark,
                    asset_history_path=args.asset_history_path,
                )
                last_refresh_time = current_time

                print(f"Loaded {len(active_pairs)} active VIN-driver pairs from asset_history_scd2")

                if not active_pairs:
                    print("No active pairs found. Waiting before retry...")
                    time.sleep(args.refresh_interval_seconds)
                    continue

            for row in active_pairs:
                for _ in range(args.messages_per_vehicle):
                    event_time = logical_start + timedelta(seconds=(event_counter % 86400))

                    event = build_event(
                        row=row,
                        event_time=event_time,
                        args=args,
                    )

                    producer.send(
                        args.output_topic,
                        key=row["vin"],
                        value=event,
                    )

                    event_counter += 1
                    total_sent += 1

                    if args.sleep_ms > 0:
                        time.sleep(args.sleep_ms / 1000)

            producer.flush()

            print(
                f"Produced {total_sent} total telemetry messages "
                f"to {args.output_topic}"
            )

    finally:
        producer.flush()
        producer.close()
        spark.stop()


if __name__ == "__main__":
    main()