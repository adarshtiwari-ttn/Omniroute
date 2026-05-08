import logging
import sys

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import LongType, IntegerType, ShortType, ByteType
from delta.tables import DeltaTable
from datetime import datetime

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


SILVER_VEHICLE_ASSIGNMENT = "s3://ttn-de-bootcamp-silver-us-east-1/poc-bootcamp-group1-silver/vehicle_assignment/"
GOLD_ASSET_HISTORY        = "s3://ttn-de-bootcamp-gold-us-east-1/poc-bootcamp-group1-gold/asset_history_scd2/"

pg_url = "jdbc:postgresql://54.92.140.124:5432/report"

pg_properties = {
    "user": "postgres",
    "password": "postgrespass",
    "driver": "org.postgresql.Driver"
}

pg_table = "staging.asset_history_scd2"

STATUS_IN_TRANSIT = "IN-TRANSIT"
STATUS_ARCHIVED   = "ARCHIVED"

BUSINESS_COLUMNS = [
    "vin",
    "driver_id",
    "start_date",
    "daily_rate",
    "region"
]

GOLD_COLUMNS = [
    "vin",
    "driver_id",
    "start_date",
    "end_date",
    "daily_rate",
    "region",
    "status",    
    "year",
    "month",
    "date"
]


def read_silver_vehicle_assignment(spark, path: str, logical_date: str):
    try:
        df = spark.read.format("delta").load(path)

        if "ingested_date" not in df.columns:
            raise ValueError("Column ingested_date not found in Silver vehicle_assignment")

        ingested_type = df.schema["ingested_date"].dataType

        if isinstance(ingested_type, (LongType, IntegerType, ShortType, ByteType)):
            df = df.filter(
                F.to_date(
                    F.from_unixtime((F.col("ingested_date") / F.lit(1000)).cast("long"))
                ) == F.lit(logical_date)
            )
        else:
            df = df.filter(F.to_date(F.col("ingested_date")) == F.lit(logical_date))

        log.info(f"Silver rows for {logical_date}: {df.count():,}")
        return df

    except Exception as e:
        log.error(f"Failed to read Silver vehicle_assignment: {e}")
        raise


def prepare_incoming_assignments(df):
    try:
        missing = [c for c in BUSINESS_COLUMNS if c not in df.columns]
        if missing:
            raise ValueError(f"Missing required Silver columns: {missing}")

        df = (
            df
            .select(*BUSINESS_COLUMNS)
            .withColumn("vin", F.upper(F.trim(F.col("vin"))))
            .withColumn("driver_id", F.upper(F.trim(F.col("driver_id"))))
            .withColumn("start_date", F.to_date(F.col("start_date")))
            .withColumn("daily_rate", F.col("daily_rate").cast("float"))
            .withColumn("region", F.trim(F.col("region")))
            .filter(
                F.col("vin").isNotNull() &
                F.col("driver_id").isNotNull() &
                F.col("start_date").isNotNull() &
                F.col("daily_rate").isNotNull() &
                F.col("region").isNotNull()
            )
        )

        log.info(f"Prepared incoming rows: {df.count():,}")
        return df

    except Exception as e:
        log.error(f"prepare_incoming_assignments failed: {e}")
        raise


def resolve_conflicts(df):
    try:
        window = (
            Window
            .partitionBy("vin", "start_date")
            .orderBy(
                F.desc("daily_rate"),
                F.asc("driver_id"),
                F.asc("region")
            )
        )

        df = (
            df
            .withColumn("_rank", F.row_number().over(window))
            .filter(F.col("_rank") == 1)
            .drop("_rank")
        )

        log.info(f"Conflict resolution complete. Rows after: {df.count():,}")
        return df

    except Exception as e:
        log.error(f"Conflict resolution failed: {e}")
        raise


def build_scd2_timeline(df, year, month, date):
    try:
        w = Window.partitionBy("vin").orderBy("start_date")

        df = (
            df
            .withColumn("next_start_date", F.lead("start_date").over(w))
            .withColumn("end_date", F.col("next_start_date").cast("date"))
            .withColumn("start_date", F.col("start_date").cast("date"))
            .withColumn("daily_rate", F.col("daily_rate").cast("float"))
            .withColumn(
                "status",
                F.when(F.col("next_start_date").isNull(), F.lit(STATUS_IN_TRANSIT))
                 .otherwise(F.lit(STATUS_ARCHIVED))
            )
            .withColumn("year", F.lit(year).cast("int"))
            .withColumn("month", F.lit(month).cast("int"))
            .withColumn("date", F.lit(date).cast("int"))
            .drop("next_start_date")
            .select(*GOLD_COLUMNS)
        )

        return df

    except Exception as e:
        log.error(f"build_scd2_timeline failed: {e}")
        raise

def initialize_gold_table(incoming_df, path: str, year, month, date):
    try:
        gold_df = build_scd2_timeline(resolve_conflicts(incoming_df), year, month, date)

        gold_df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .partitionBy("year", "month", "date") \
            .save(path)

        log.info(f"Gold table initialized with {gold_df.count():,} records at {path}")

    except Exception as e:
        log.error(f"initialize_gold_table failed: {e}")
        raise

def apply_scd2(spark, incoming_df, path: str, year, month, date):
    try:
        incoming_count = incoming_df.count()
        if incoming_count == 0:
            log.info("No incoming rows. Skipping SCD2 merge.")
            return

        gold_table = DeltaTable.forPath(spark, path)
        gold_df = gold_table.toDF()

        affected_vins = incoming_df.select("vin").distinct()

        existing_events = (
            gold_df
            .join(affected_vins, on="vin", how="inner")
            .select(*BUSINESS_COLUMNS)
        )

        incoming_events = incoming_df.select(*BUSINESS_COLUMNS)

        combined_events = existing_events.unionByName(incoming_events)

        resolved_events = resolve_conflicts(combined_events)
        rebuilt_history = build_scd2_timeline(resolved_events, year, month, date)

        gold_table.alias("gold").merge(
            affected_vins.alias("affected"),
            "gold.vin = affected.vin"
        ).whenMatchedDelete().execute()

        rebuilt_history.write \
            .format("delta") \
            .mode("append") \
            .partitionBy("year", "month", "date") \
            .save(path)

        log.info(f"SCD2 merge complete. Rows written: {rebuilt_history.count():,}")

    except Exception as e:
        log.error(f"SCD2 merge failed: {e}")
        raise


def validate_gold_table(gold_df):
    try:
        duplicate_keys = (
            gold_df
            .groupBy("vin", "start_date")
            .count()
            .filter(F.col("count") > 1)
        )

        if duplicate_keys.limit(1).count() > 0:
            duplicate_keys.show(20, truncate=False)
            raise ValueError("Duplicate vin + start_date found in Gold")

        multiple_active = (
            gold_df
            .filter(F.col("status") == STATUS_IN_TRANSIT)
            .groupBy("vin")
            .count()
            .filter(F.col("count") > 1)
        )

        if multiple_active.limit(1).count() > 0:
            multiple_active.show(20, truncate=False)
            raise ValueError("Multiple IN-TRANSIT rows found for same VIN")

        bad_dates = (
            gold_df
            .filter(
                F.col("end_date").isNotNull() &
                (F.col("end_date") < F.col("start_date"))
            )
        )

        if bad_dates.limit(1).count() > 0:
            bad_dates.show(20, truncate=False)
            raise ValueError("Invalid SCD2 date range found")

        log.info("Gold validation passed.")

    except Exception as e:
        log.error(f"validate_gold_table failed: {e}")
        raise


def write_to_postgres(gold_df):
    try:
        reporting_df = gold_df.select(
            "vin",
            "driver_id",
            "start_date",
            "end_date",
            "daily_rate",
            "region",
            "status"
        )
        reporting_df \
            .repartition(4) \
            .write \
            .mode("overwrite") \
            .jdbc(url=pg_url, table=pg_table, properties=pg_properties)

        log.info(f"Gold data written to PostgreSQL table {pg_table}")

    except Exception as e:
        log.error(f"PostgreSQL write failed: {e}")
        raise


def main():
    log.info("OmniRoute | Silver -> Gold | asset_history_scd2 | Glue Job")

    args = getResolvedOptions(sys.argv, ["JOB_NAME", "logical_date"])
    logical_date = datetime.strptime(args["logical_date"], "%Y-%m-%d").date()
    year = logical_date.year
    month = logical_date.month
    date = logical_date.day
    log.info(f"Logical date for this run: {logical_date}")
    
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
        silver_df = read_silver_vehicle_assignment(
            spark,
            SILVER_VEHICLE_ASSIGNMENT,
            str(logical_date)
        )

        incoming_df = prepare_incoming_assignments(silver_df)

        if DeltaTable.isDeltaTable(spark, GOLD_ASSET_HISTORY):
            log.info("Gold table exists. Applying SCD2 merge.")
            apply_scd2(spark, incoming_df, GOLD_ASSET_HISTORY, year, month, date)
        else:
            log.info("Gold table does not exist. Initializing.")
            initialize_gold_table(incoming_df, GOLD_ASSET_HISTORY, year, month, date)

        gold_df = spark.read.format("delta").load(GOLD_ASSET_HISTORY)

        validate_gold_table(gold_df)

        total = gold_df.count()
        in_transit = gold_df.filter(F.col("status") == STATUS_IN_TRANSIT).count()
        archived = gold_df.filter(F.col("status") == STATUS_ARCHIVED).count()

        log.info("Gold Table Summary")
        log.info(f"Total records : {total:,}")
        log.info(f"IN-TRANSIT    : {in_transit:,}")
        log.info(f"ARCHIVED      : {archived:,}")

        log.info("Gold Schema:")
        gold_df.printSchema()

        log.info("Gold Sample:")
        gold_df.orderBy("vin", "start_date").show(20, truncate=False)

        write_to_postgres(gold_df)

        success = True
        log.info("Job completed successfully.")

    except Exception as e:
        log.error(f"Job failed: {e}")
        raise

    finally:
        job.commit()
        log.info("Glue job committed.")

        log.info("OmniRoute | asset_history_scd2 | Job End")


if __name__ == "__main__":
    main()