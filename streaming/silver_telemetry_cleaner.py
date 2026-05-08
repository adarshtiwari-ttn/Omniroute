import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
)


DEFAULT_INPUT_TOPIC = "omniroute.telemetry.bronze"
DEFAULT_OUTPUT_TOPIC = "omniroute.telemetry.silver"

DEFAULT_CHECKPOINT_PATH = (
    "s3://ttn-de-bootcamp-gold-us-east-1/"
    "poc-bootcamp-group1-gold/streaming/checkpoints/silver/"
)

DEFAULT_BAD_PATH = (
    "s3://ttn-de-bootcamp-bronze-us-east-1/"
    "poc-bootcamp-group1-bronze/bad_data/telemetry_stream/"
)


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument("--kafka_bootstrap", default=os.environ.get("KAFKA_SERVER"))
    parser.add_argument("--input_topic", default=DEFAULT_INPUT_TOPIC)
    parser.add_argument("--output_topic", default=DEFAULT_OUTPUT_TOPIC)
    parser.add_argument("--checkpoint_path", default=DEFAULT_CHECKPOINT_PATH)
    parser.add_argument("--bad_path", default=DEFAULT_BAD_PATH)
    parser.add_argument("--trigger_seconds", type=int, default=30)

    args = parser.parse_args()

    if not args.kafka_bootstrap:
        raise ValueError(
            "Kafka bootstrap server is required. "
            "Pass --kafka_bootstrap or set KAFKA_SERVER env variable."
        )

    return args


def main():
    args = parse_args()

    spark = (
        SparkSession.builder
        .appName("OmniRouteTelemetrySilverCleaner")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )

    schema = StructType([
        StructField("vin", StringType(), True),
        StructField("driver_id", StringType(), True),
        StructField("speed", StringType(), True),
        StructField("lat", StringType(), True),
        StructField("long", StringType(), True),
        StructField("event_timestamp", StringType(), True),
        StructField("base_rate", StringType(), True),
    ])

    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", args.kafka_bootstrap)
        .option("subscribe", args.input_topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed_df = (
        raw_df
        .select(
            F.col("key").cast("string").alias("kafka_key"),
            F.col("value").cast("string").alias("raw_json"),
            F.from_json(F.col("value").cast("string"), schema).alias("data")
        )
        .select("kafka_key", "raw_json", "data.*")
        .withColumn("vin", F.upper(F.trim(F.col("vin"))))
        .withColumn("driver_id", F.upper(F.trim(F.col("driver_id"))))
        .withColumn("speed_int", F.col("speed").cast(IntegerType()))
        .withColumn("lat_double", F.col("lat").cast(DoubleType()))
        .withColumn("long_double", F.col("long").cast(DoubleType()))
        .withColumn("base_rate_double", F.col("base_rate").cast(DoubleType()))
        .withColumn("event_ts", F.to_timestamp("event_timestamp", "yyyy-MM-dd'T'HH:mm:ss'Z'"))
        .withColumn("event_date", F.to_date("event_ts"))
        .withColumn("year", F.year("event_ts").cast("int"))
        .withColumn("month", F.month("event_ts").cast("int"))
        .withColumn("date", F.dayofmonth("event_ts").cast("int"))
    )

    valid_condition = (
        F.col("vin").rlike("^(?=.*[A-Z])(?=.*[0-9])[A-Z0-9]{8}$") &
        F.col("driver_id").rlike("^DRV_[0-9]+$") &
        F.col("speed_int").between(0, 250) &
        F.col("lat_double").between(-90, 90) &
        F.col("long_double").between(-180, 180) &
        F.col("event_ts").isNotNull() &
        F.col("base_rate_double").isNotNull() &
        (F.col("base_rate_double") > 0)
    )

    valid_df = (
        parsed_df
        .filter(valid_condition)
        .select(
            "vin",
            "driver_id",
            F.col("speed_int").alias("speed"),
            F.col("lat_double").alias("lat"),
            F.col("long_double").alias("long"),
            F.col("event_ts").alias("event_timestamp"),
            F.col("base_rate_double").alias("base_rate"),
        )
        .withColumn(
            "value",
            F.to_json(
                F.struct(
                    "vin",
                    "driver_id",
                    "speed",
                    "lat",
                    "long",
                    "event_timestamp",
                    "base_rate",
                )
            )
        )
        .withColumn("key", F.col("vin").cast("string"))
        .select("key", "value")
    )

    bad_df = (
        parsed_df
        .filter(~valid_condition)
        .select(
            "kafka_key",
            "raw_json",
            "vin",
            "driver_id",
            "speed",
            "lat",
            "long",
            "event_timestamp",
            "base_rate",
            "event_date",
            "year",
            "month",
            "date",
        )
    )

    good_query = (
        valid_df.writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", args.kafka_bootstrap)
        .option("topic", args.output_topic)
        .option("checkpointLocation", args.checkpoint_path.rstrip("/") + "/good_to_kafka")
        .trigger(processingTime=f"{args.trigger_seconds} seconds")
        .start()
    )

    bad_query = (
        bad_df.writeStream
        .format("parquet")
        .option("path", args.bad_path.rstrip("/") + "/")
        .option("checkpointLocation", args.checkpoint_path.rstrip("/") + "/bad_to_s3")
        .partitionBy("year", "month", "date")
        .trigger(processingTime=f"{args.trigger_seconds} seconds")
        .start()
    )

    good_query.awaitTermination()
    bad_query.awaitTermination()

    spark.stop()


if __name__ == "__main__":
    main()