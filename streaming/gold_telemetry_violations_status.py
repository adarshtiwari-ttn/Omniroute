import argparse
import os

import redis
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    TimestampType,
)


STATUS_ACTIVE = "ACTIVE"
STATUS_SUSPENDED = "SUSPENDED"

SPEED_LIMIT = 110
STRIKE_LIMIT = 10
PENALTY_RATE = 0.05

DEFAULT_INPUT_TOPIC = "omniroute.telemetry.silver"

DEFAULT_RESTRICTED_ZONES_PATH = (
    "s3://ttn-de-bootcamp-bronze-us-east-1/"
    "poc-bootcamp-group1-bronze/static/restricted_zones.json"
)

DEFAULT_DRIVER_EVENTS_PATH = (
    "s3://ttn-de-bootcamp-gold-us-east-1/"
    "poc-bootcamp-group1-gold/driver_violation_events/"
)

DEFAULT_DRIVER_STATUS_PATH = (
    "s3://ttn-de-bootcamp-gold-us-east-1/"
    "poc-bootcamp-group1-gold/driver_safety_status/"
)

DEFAULT_CHECKPOINT_PATH = (
    "s3://ttn-de-bootcamp-gold-us-east-1/"
    "poc-bootcamp-group1-gold/streaming/checkpoints/gold/"
)


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument("--kafka_bootstrap", default=os.environ.get("KAFKA_SERVER"))
    parser.add_argument("--input_topic", default=DEFAULT_INPUT_TOPIC)

    parser.add_argument("--restricted_zones_path", default=DEFAULT_RESTRICTED_ZONES_PATH)
    parser.add_argument("--driver_violation_events_path", default=DEFAULT_DRIVER_EVENTS_PATH)
    parser.add_argument("--driver_safety_status_path", default=DEFAULT_DRIVER_STATUS_PATH)
    parser.add_argument("--checkpoint_path", default=DEFAULT_CHECKPOINT_PATH)

    parser.add_argument("--redis_host", default=os.environ.get("REDIS_HOST"))
    parser.add_argument("--redis_port", default=6379, type=int)

    parser.add_argument("--pg_host", required=True)
    parser.add_argument("--pg_port", default="5432")
    parser.add_argument("--pg_database", default="report")
    parser.add_argument("--pg_user", default="postgres")
    parser.add_argument("--pg_password", required=True)

    parser.add_argument("--pg_driver_events_table", default="public.driver_safety_events")
    parser.add_argument("--pg_driver_safety_table", default="public.driver_safety_status")

    parser.add_argument("--trigger_seconds", type=int, default=30)

    args = parser.parse_args()

    if not args.kafka_bootstrap:
        raise ValueError("Kafka bootstrap server is required. Pass --kafka_bootstrap or set KAFKA_SERVER.")

    if not args.redis_host:
        raise ValueError("Redis host is required. Pass --redis_host or set REDIS_HOST.")

    return args


def get_pg_url(args):
    return f"jdbc:postgresql://{args.pg_host}:{args.pg_port}/{args.pg_database}"


def get_pg_properties(args):
    return {
        "user": args.pg_user,
        "password": args.pg_password,
        "driver": "org.postgresql.Driver",
    }


def get_redis_client(redis_host, redis_port):
    return redis.Redis(
        host=redis_host,
        port=redis_port,
        decode_responses=True,
    )


def build_status_snapshot_for_month(spark, redis_client, month_key, year, month, date):
    keys = redis_client.keys(f"driver_safety:*:{month_key}")

    rows = []

    for key in keys:
        data = redis_client.hgetall(key)

        if not data:
            continue

        rows.append({
            "driver_id": data.get("driver_id"),
            "month": data.get("month"),
            "zone_sk": data.get("zone_sk"),
            "base_rate": float(data.get("base_rate", 0)),
            "strike_count": int(data.get("strike_count", 0)),
            "current_adjusted_rate": float(data.get("current_adjusted_rate", 0)),
            "status": data.get("status", STATUS_ACTIVE),
            "year": year,
            "month_num": month,
            "date": date,
        })

    if not rows:
        return None

    return (
        spark.createDataFrame(rows)
        .withColumn("driver_id", F.col("driver_id").cast("string"))
        .withColumn("month", F.col("month").cast("string"))
        .withColumn("zone_sk", F.col("zone_sk").cast("string"))
        .withColumn("base_rate", F.col("base_rate").cast("double"))
        .withColumn("strike_count", F.col("strike_count").cast("int"))
        .withColumn("current_adjusted_rate", F.col("current_adjusted_rate").cast("double"))
        .withColumn("status", F.col("status").cast("string"))
        .withColumn("year", F.col("year").cast("int"))
        .withColumn("month_num", F.col("month_num").cast("int"))
        .withColumn("date", F.col("date").cast("int"))
    )


def write_events_delta(batch_df, path):
    (
        batch_df
        .write
        .format("delta")
        .mode("append")
        .partitionBy("year", "month", "date")
        .save(path)
    )


def write_events_postgres(batch_df, args):
    pg_df = (
        batch_df
        .select(
            "driver_id",
            "vin",
            "event_timestamp",
            "zone_sk",
            "speed",
            "lat",
            "long",
            "is_speed_violation",
            "is_restricted_zone_breach",
        )
        .dropDuplicates(["vin", "event_timestamp"])
    )

    if pg_df.rdd.isEmpty():
        return

    (
        pg_df
        .write
        .mode("append")
        .jdbc(
            url=get_pg_url(args),
            table=args.pg_driver_events_table,
            properties=get_pg_properties(args),
        )
    )


def write_status_delta(status_df, status_path, year, month, date):
    (
        status_df
        .write
        .format("delta")
        .mode("overwrite")
        .option(
            "replaceWhere",
            f"year = {year} AND month_num = {month} AND date = {date}"
        )
        .partitionBy("year", "month_num", "date")
        .save(status_path)
    )


def write_status_postgres(status_df, args):
    pg_df = (
        status_df
        .select(
            "driver_id",
            "month",
            "zone_sk",
            "base_rate",
            "strike_count",
            "current_adjusted_rate",
            "status",
        )
        .dropDuplicates(["driver_id", "month"])
    )

    if pg_df.rdd.isEmpty():
        return

    (
        pg_df
        .write
        .mode("append")
        .jdbc(
            url=get_pg_url(args),
            table=args.pg_driver_safety_table,
            properties=get_pg_properties(args),
        )
    )


def process_violation_batch(batch_df, batch_id, args):
    if batch_df.rdd.isEmpty():
        print(f"Gold batch_id={batch_id}, violation_count=0")
        return

    batch_df = batch_df.cache()

    violation_count = batch_df.count()
    print(f"Gold batch_id={batch_id}, violation_count={violation_count}")

    redis_client = get_redis_client(
        args.redis_host,
        args.redis_port,
    )

    write_events_delta(
        batch_df,
        args.driver_violation_events_path,
    )

    write_events_postgres(
        batch_df,
        args,
    )

    rows = (
        batch_df
        .select(
            "driver_id",
            "vin",
            "event_timestamp",
            "base_rate",
            "zone_sk",
            "year",
            "month",
            "date",
        )
        .collect()
    )

    touched_months = {}

    for row in rows:
        driver_id = row["driver_id"]
        vin = row["vin"]
        event_timestamp = row["event_timestamp"]
        base_rate = float(row["base_rate"])
        zone_sk = row["zone_sk"]

        year = int(row["year"])
        month = int(row["month"])
        date = int(row["date"])
        month_key = f"{year}-{str(month).zfill(2)}"

        touched_months[month_key] = {
            "year": year,
            "month": month,
            "date": date,
        }

        event_id = f"{driver_id}:{vin}:{event_timestamp}"

        if not redis_client.setnx(f"processed_event:{event_id}", "1"):
            continue

        redis_client.expire(
            f"processed_event:{event_id}",
            60 * 60 * 24 * 60,
        )

        key = f"driver_safety:{driver_id}:{month_key}"

        existing = redis_client.hgetall(key)

        if existing:
            strike_count = int(existing.get("strike_count", 0))
            status = existing.get("status", STATUS_ACTIVE)
            base_rate = float(existing.get("base_rate", base_rate))
        else:
            strike_count = 0
            status = STATUS_ACTIVE

        if status != STATUS_SUSPENDED:
            strike_count += 1

            if strike_count >= STRIKE_LIMIT:
                status = STATUS_SUSPENDED

        if status == STATUS_SUSPENDED:
            current_adjusted_rate = 0.0
        else:
            current_adjusted_rate = round(base_rate * (1 - PENALTY_RATE), 2)

        redis_client.hset(
            key,
            mapping={
                "driver_id": driver_id,
                "month": month_key,
                "zone_sk": zone_sk if zone_sk is not None else "",
                "base_rate": base_rate,
                "strike_count": strike_count,
                "current_adjusted_rate": current_adjusted_rate,
                "status": status,
            }
        )

    spark = batch_df.sparkSession

    for month_key, date_parts in touched_months.items():
        status_df = build_status_snapshot_for_month(
            spark=spark,
            redis_client=redis_client,
            month_key=month_key,
            year=date_parts["year"],
            month=date_parts["month"],
            date=date_parts["date"],
        )

        if status_df is None:
            continue

        write_status_delta(
            status_df,
            args.driver_safety_status_path,
            date_parts["year"],
            date_parts["month"],
            date_parts["date"],
        )

        write_status_postgres(
            status_df,
            args,
        )

    batch_df.unpersist()


def main():
    args = parse_args()

    spark = (
        SparkSession.builder
        .appName("OmniRouteTelemetryGoldViolationsStatus")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

    schema = StructType([
        StructField("vin", StringType(), True),
        StructField("driver_id", StringType(), True),
        StructField("speed", IntegerType(), True),
        StructField("lat", DoubleType(), True),
        StructField("long", DoubleType(), True),
        StructField("event_timestamp", TimestampType(), True),
        StructField("base_rate", DoubleType(), True),
    ])

    zones_df = (
        spark.read
        .json(args.restricted_zones_path)
        .select(
            "zone_name",
            F.col("min_lat").cast("double").alias("min_lat"),
            F.col("max_lat").cast("double").alias("max_lat"),
            F.col("min_long").cast("double").alias("min_long"),
            F.col("max_long").cast("double").alias("max_long"),
        )
    )

    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", args.kafka_bootstrap)
        .option("subscribe", args.input_topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    telemetry_df = (
        raw_df
        .select(
            F.from_json(
                F.col("value").cast("string"),
                schema
            ).alias("data")
        )
        .select("data.*")
        .filter(
            F.col("vin").isNotNull() &
            F.col("driver_id").isNotNull() &
            F.col("speed").isNotNull() &
            F.col("lat").isNotNull() &
            F.col("long").isNotNull() &
            F.col("event_timestamp").isNotNull() &
            F.col("base_rate").isNotNull()
        )
        .withWatermark("event_timestamp", "10 minutes")
    )

    joined_df = telemetry_df.join(
        F.broadcast(zones_df),
        (
            (telemetry_df["lat"] >= zones_df["min_lat"]) &
            (telemetry_df["lat"] <= zones_df["max_lat"]) &
            (telemetry_df["long"] >= zones_df["min_long"]) &
            (telemetry_df["long"] <= zones_df["max_long"])
        ),
        "left"
    )

    violation_df = (
        joined_df
        .withColumn("is_speed_violation", F.col("speed") > F.lit(SPEED_LIMIT))
        .withColumn("is_restricted_zone_breach", F.col("zone_name").isNotNull())
        .filter(F.col("is_speed_violation") | F.col("is_restricted_zone_breach"))
        .withColumn("year", F.year("event_timestamp").cast("int"))
        .withColumn("month", F.month("event_timestamp").cast("int"))
        .withColumn("date", F.dayofmonth("event_timestamp").cast("int"))
        .select(
            "driver_id",
            "vin",
            "event_timestamp",
            F.col("zone_name").alias("zone_sk"),
            "speed",
            "lat",
            "long",
            "is_speed_violation",
            "is_restricted_zone_breach",
            "base_rate",
            "year",
            "month",
            "date",
        )
        .dropDuplicates(["vin", "event_timestamp"])
    )

    query = (
        violation_df
        .writeStream
        .foreachBatch(
            lambda batch_df, batch_id: process_violation_batch(
                batch_df=batch_df,
                batch_id=batch_id,
                args=args,
            )
        )
        .option("checkpointLocation", args.checkpoint_path.rstrip("/") + "/violations_status")
        .trigger(processingTime=f"{args.trigger_seconds} seconds")
        .start()
    )

    query.awaitTermination()

    spark.stop()


if __name__ == "__main__":
    main()