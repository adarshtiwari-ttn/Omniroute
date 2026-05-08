import logging
import sys
from datetime import datetime

from pyspark.sql import functions as F
from delta.tables import DeltaTable

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.conf import SparkConf


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger(__name__)


SILVER_VEHICLE_REGISTRY = "s3://ttn-de-bootcamp-silver-us-east-1/poc-bootcamp-group1-silver/vehicle_registry/"
GOLD_ASSET_HISTORY      = "s3://ttn-de-bootcamp-gold-us-east-1/poc-bootcamp-group1-gold/asset_history_scd2/"
GOLD_ACTIVE_FLEET       = "s3://ttn-de-bootcamp-gold-us-east-1/poc-bootcamp-group1-gold/active_fleet_snapshot/"

pg_url = "jdbc:postgresql://54.92.140.124:5432/report"

pg_properties = {
    "user": "postgres",
    "password": "postgrespass",
    "driver": "org.postgresql.Driver"
}

pg_table = "staging.active_fleet_snapshot"

STATUS_IN_TRANSIT = "IN-TRANSIT"


def read_asset_history(spark, path: str, year: int, month: int, date: int):
    try:
        df = (
            spark.read
            .format("delta")
            .load(path)
            .filter(
                (F.col("year") == F.lit(year)) &
                (F.col("month") == F.lit(month)) &
                (F.col("date") == F.lit(date))
            )
        )

        log.info(f"Gold asset_history_scd2 rows: {df.count():,}")
        return df

    except Exception as e:
        log.error(f"Failed to read asset_history_scd2: {e}")
        raise


def read_vehicle_registry(spark, path: str, year: int, month: int, date: int):
    try:
        df = spark.read.parquet(path)

        if {"year", "month", "date"}.issubset(set(df.columns)):
            df = df.filter(
                (F.col("year") == F.lit(year)) &
                (F.col("month") == F.lit(month)) &
                (F.col("date") == F.lit(date))
            )

        log.info(f"Silver vehicle_registry rows: {df.count():,}")
        return df

    except Exception as e:
        log.error(f"Failed to read vehicle_registry: {e}")
        raise


def build_snapshot(asset_history_df, vehicle_registry_df, logical_time: str):
    try:
        in_transit_df = (
            asset_history_df
            .filter(F.col("status") == STATUS_IN_TRANSIT)
            .withColumn("vin", F.upper(F.trim(F.col("vin"))))
        )

        registry_df = (
            vehicle_registry_df
            .select("vin", "model")
            .withColumn("vin", F.upper(F.trim(F.col("vin"))))
            .withColumn("model", F.trim(F.col("model")))
            .dropDuplicates(["vin"])
        )

        snapshot_df = (
            in_transit_df
            .join(registry_df, on="vin", how="inner")
            .groupBy("model")
            .agg(F.countDistinct("vin").alias("no_of_active_vehicles"))
            .withColumn("snapshot_time", F.to_timestamp(F.lit(logical_time)))
            .withColumn("snapshot_date", F.to_date(F.col("snapshot_time")))
            .select(
                "model",
                "no_of_active_vehicles",
                "snapshot_time",
                "snapshot_date"
            )
        )

        log.info(f"Snapshot rows: {snapshot_df.count():,}")
        return snapshot_df

    except Exception as e:
        log.error(f"build_snapshot failed: {e}")
        raise


def write_snapshot(spark, df, path: str):
    try:
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

        (
            df.write
            .mode("overwrite")
            .partitionBy("snapshot_date")
            .parquet(path)
        )

        log.info(f"Snapshot written to {path}")

    except Exception as e:
        log.error(f"Failed to write snapshot: {e}")
        raise


def write_to_postgres(df):
    try:
        reporting_df = df.select(
            "model",
            "no_of_active_vehicles",
            "snapshot_time"
        )

        (
            reporting_df
            .write
            .mode("overwrite")
            .jdbc(url=pg_url, table=pg_table, properties=pg_properties)
        )

        log.info(f"Snapshot written to PostgreSQL table {pg_table}")

    except Exception as e:
        log.error(f"PostgreSQL write failed: {e}")
        raise


def main():
    log.info("OmniRoute | Gold | active_fleet_snapshot | Glue Job")

    success = False

    args = getResolvedOptions(sys.argv, ["JOB_NAME", "logical_time"])
    logical_time = args["logical_time"]

    logical_date = datetime.strptime(logical_time[:10], "%Y-%m-%d").date()
    year = logical_date.year
    month = logical_date.month
    date = logical_date.day

    log.info(f"Logical time: {logical_time}")

    conf = SparkConf()
    conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    sc = SparkContext(conf=conf)
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session

    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    spark.sparkContext.setLogLevel("ERROR")

    try:
        asset_history_df = read_asset_history(
            spark,
            GOLD_ASSET_HISTORY,
            year,
            month,
            date
        )

        vehicle_registry_df = read_vehicle_registry(
            spark,
            SILVER_VEHICLE_REGISTRY,
            year,
            month,
            date
        )

        snapshot_df = build_snapshot(
            asset_history_df,
            vehicle_registry_df,
            logical_time
        )

        write_snapshot(spark, snapshot_df, GOLD_ACTIVE_FLEET)

        log.info("Snapshot Schema:")
        snapshot_df.printSchema()

        log.info("Snapshot Sample:")
        snapshot_df.orderBy(F.desc("no_of_active_vehicles")).show(50, truncate=False)

        write_to_postgres(snapshot_df)

        success = True
        log.info("Job completed successfully.")

    except Exception as e:
        log.error(f"Job failed: {e}")
        raise

    finally:
        if success:
            job.commit()
            log.info("Glue job committed.")
        else:
            log.error("Glue job failed. Commit skipped.")

        log.info("OmniRoute | active_fleet_snapshot | Job End")

if __name__ == "__main__":
    main()