import logging
import sys
from datetime import datetime

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DateType,
    DoubleType,
    IntegerType
)
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

pg_url = "jdbc:postgresql://54.92.140.124:5432/report"

pg_properties = {
    "user": "postgres",
    "password": "postgrespass",
    "driver": "org.postgresql.Driver"
}

pg_table = "staging.fuel_efficiency_audit"


SILVER_FUEL_TRANSACTIONS = "s3://ttn-de-bootcamp-silver-us-east-1/poc-bootcamp-group1-silver/fuel_transactions/"
SILVER_VEHICLE_REGISTRY = "s3://ttn-de-bootcamp-silver-us-east-1/poc-bootcamp-group1-silver/vehicle_registry/"
SILVER_MAINTENANCE_SCHEDULES = "s3://ttn-de-bootcamp-silver-us-east-1/poc-bootcamp-group1-silver/maintenance_schedules/"

GOLD_FUEL_EFFICIENCY_AUDIT = "s3://ttn-de-bootcamp-gold-us-east-1/poc-bootcamp-group1-gold/fuel_efficiency_audit/"

EFFICIENCY_THRESHOLD = 0.88

STATUS_FLAGGED = "FLAGGED"
STATUS_OK = "OK"

AUDIT_COLUMNS = [
    "vin",
    "model",
    "audit_date",
    "fuel_liters",
    "distance_km",
    "km_per_liter",
    "baseline_kmpl",
    "threshold_kmpl",
    "status",
    "year",
    "month",
    "date"
]


def empty_audit_df(spark):
    schema = StructType([
        StructField("vin", StringType(), True),
        StructField("model", StringType(), True),
        StructField("audit_date", DateType(), True),
        StructField("fuel_liters", DoubleType(), True),
        StructField("distance_km", DoubleType(), True),
        StructField("km_per_liter", DoubleType(), True),
        StructField("baseline_kmpl", DoubleType(), True),
        StructField("threshold_kmpl", DoubleType(), True),
        StructField("status", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("month", IntegerType(), True),
        StructField("date", IntegerType(), True),
    ])

    return spark.createDataFrame([], schema)


def align_audit_schema(df):
    return (
        df
        .withColumn("vin", F.col("vin").cast("string"))
        .withColumn("model", F.col("model").cast("string"))
        .withColumn("audit_date", F.col("audit_date").cast("date"))
        .withColumn("fuel_liters", F.col("fuel_liters").cast("double"))
        .withColumn("distance_km", F.col("distance_km").cast("double"))
        .withColumn("km_per_liter", F.col("km_per_liter").cast("double"))
        .withColumn("baseline_kmpl", F.col("baseline_kmpl").cast("double"))
        .withColumn("threshold_kmpl", F.col("threshold_kmpl").cast("double"))
        .withColumn("status", F.col("status").cast("string"))
        .withColumn("year", F.col("year").cast("int"))
        .withColumn("month", F.col("month").cast("int"))
        .withColumn("date", F.col("date").cast("int"))
        .select(*AUDIT_COLUMNS)
    )


def read_fuel_transactions(spark, path, year, month, date):
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

        df = (
            df
            .select(
                "transaction_id",
                "vin",
                "fuel_liters",
                "odometer_reading",
                "timestamp"
            )
            .withColumn("vin", F.upper(F.trim(F.col("vin"))))
            .withColumn("fuel_liters", F.col("fuel_liters").cast("double"))
            .withColumn("odometer_reading", F.col("odometer_reading").cast("double"))
            .withColumn("timestamp", F.col("timestamp").cast("timestamp"))
            .filter(
                F.col("transaction_id").isNotNull() &
                F.col("vin").isNotNull() &
                F.col("fuel_liters").isNotNull() &
                F.col("odometer_reading").isNotNull() &
                F.col("timestamp").isNotNull() &
                (F.col("fuel_liters") > 0) &
                (F.col("odometer_reading") > 0)
            )
        )

        log.info(f"Silver fuel_transactions rows for logical date: {df.count():,}")
        return df

    except Exception as e:
        log.error(f"Failed to read silver fuel_transactions: {e}")
        raise


def read_vehicle_registry(spark, path, year, month, date):
    try:
        df = spark.read.parquet(path)

        if {"year", "month", "date"}.issubset(set(df.columns)):
            df = df.filter(
                (F.col("year") == F.lit(year)) &
                (F.col("month") == F.lit(month)) &
                (F.col("date") == F.lit(date))
            )

        required_cols = {"vin", "model", "baseline_kmpl"}
        missing = required_cols - set(df.columns)

        if missing:
            raise ValueError(f"Missing columns in vehicle_registry: {missing}")

        df = (
            df
            .select("vin", "model", "baseline_kmpl")
            .withColumn("vin", F.upper(F.trim(F.col("vin"))))
            .withColumn("model", F.trim(F.col("model")))
            .withColumn("baseline_kmpl", F.col("baseline_kmpl").cast("double"))
            .filter(
                F.col("vin").isNotNull() &
                F.col("model").isNotNull() &
                F.col("baseline_kmpl").isNotNull() &
                (F.col("baseline_kmpl") > 0)
            )
            .dropDuplicates(["vin"])
        )

        log.info(f"Silver vehicle_registry rows for logical date: {df.count():,}")
        return df

    except Exception as e:
        log.error(f"Failed to read silver vehicle_registry: {e}")
        raise


def read_maintenance_schedules(spark, path, run_date):
    try:
        df = spark.read.parquet(path)

        required_cols = {"vin", "service_date"}
        missing = required_cols - set(df.columns)

        if missing:
            raise ValueError(f"Missing columns in maintenance_schedules: {missing}")

        df = (
            df
            .select("vin", "service_date")
            .withColumn("vin", F.upper(F.trim(F.col("vin"))))
            .withColumn("service_date", F.to_date(F.col("service_date")))
            .filter(F.col("service_date") == F.lit(run_date))
            .select("vin")
            .dropDuplicates(["vin"])
        )

        log.info(f"Maintenance VINs for {run_date}: {df.count():,}")
        return df

    except Exception as e:
        log.error(f"Failed to read silver maintenance_schedules: {e}")
        raise


def build_audit(
    spark,
    fuel_df,
    registry_df,
    maintenance_df,
    run_date,
    year,
    month,
    date
):
    try:
        logical_dt = datetime.strptime(run_date, "%Y-%m-%d").date()

        if logical_dt.weekday() >= 5:
            log.info(f"{run_date} is weekend. Fuel efficiency audit output will be empty.")
            return empty_audit_df(spark)

        fuel_df = (
            fuel_df
            .withColumn("audit_date", F.to_date(F.col("timestamp")))
            .filter(F.col("audit_date") == F.lit(run_date))
        )

        before_maintenance = fuel_df.count()

        fuel_df = fuel_df.join(
            maintenance_df,
            on="vin",
            how="left_anti"
        )

        after_maintenance = fuel_df.count()

        log.info(f"Fuel rows removed due to maintenance: {before_maintenance - after_maintenance:,}")

        if after_maintenance == 0:
            log.info("No fuel rows left after maintenance exclusion.")
            return empty_audit_df(spark)

        vin_window = Window.partitionBy("vin", "audit_date").orderBy("timestamp", "transaction_id")

        fuel_df = (
            fuel_df
            .withColumn("prev_odometer", F.lag("odometer_reading").over(vin_window))
            .withColumn(
                "distance_km",
                F.when(
                    F.col("prev_odometer").isNull(),
                    F.col("odometer_reading")
                ).otherwise(
                    F.col("odometer_reading") - F.col("prev_odometer")
                )
            )
            .filter(
                F.col("distance_km").isNotNull() &
                (F.col("distance_km") > 0)
            )
        )

        log.info(f"Fuel rows after per-transaction distance calculation: {fuel_df.count():,}")

        daily_fuel_df = (
            fuel_df
            .groupBy("vin", "audit_date")
            .agg(
                F.round(F.sum("fuel_liters"), 2).cast("double").alias("fuel_liters"),
                F.round(F.sum("distance_km"), 2).cast("double").alias("distance_km"),
                F.countDistinct("transaction_id").alias("transaction_count")
            )
            .filter(
                F.col("fuel_liters").isNotNull() &
                F.col("distance_km").isNotNull() &
                (F.col("fuel_liters") > 0) &
                (F.col("distance_km") > 0)
            )
        )

        log.info(f"Daily vehicle fuel rows after aggregation: {daily_fuel_df.count():,}")

        audit_df = daily_fuel_df.join(
            registry_df,
            on="vin",
            how="inner"
        )

        audit_df = (
            audit_df
            .withColumn(
                "km_per_liter",
                F.round(F.col("distance_km") / F.col("fuel_liters"), 2).cast("double")
            )
            .withColumn(
                "threshold_kmpl",
                F.round(F.col("baseline_kmpl") * F.lit(EFFICIENCY_THRESHOLD), 2).cast("double")
            )
            .withColumn(
                "status",
                F.when(
                    F.col("km_per_liter") < F.col("threshold_kmpl"),
                    F.lit(STATUS_FLAGGED)
                ).otherwise(F.lit(STATUS_OK))
            )
            .withColumn("year", F.lit(year).cast("int"))
            .withColumn("month", F.lit(month).cast("int"))
            .withColumn("date", F.lit(date).cast("int"))
            .select(*AUDIT_COLUMNS)
        )

        audit_df = align_audit_schema(audit_df)

        total = audit_df.count()
        flagged = audit_df.filter(F.col("status") == STATUS_FLAGGED).count()
        ok = audit_df.filter(F.col("status") == STATUS_OK).count()

        log.info(f"Fuel audit complete. Total: {total:,} | FLAGGED: {flagged:,} | OK: {ok:,}")

        return audit_df

    except Exception as e:
        log.error(f"build_audit failed: {e}")
        raise


def validate_audit(df):
    try:
        duplicate_rows = (
            df
            .groupBy("vin", "audit_date")
            .count()
            .filter(F.col("count") > 1)
        )

        if duplicate_rows.limit(1).count() > 0:
            duplicate_rows.show(20, truncate=False)
            raise ValueError("Duplicate vin + audit_date found in fuel_efficiency_audit")

        invalid_status = (
            df
            .filter(~F.col("status").isin([STATUS_FLAGGED, STATUS_OK]))
        )

        if invalid_status.limit(1).count() > 0:
            invalid_status.show(20, truncate=False)
            raise ValueError("Invalid status found in fuel_efficiency_audit")

        invalid_metrics = (
            df
            .filter(
                F.col("km_per_liter").isNull() |
                F.col("baseline_kmpl").isNull() |
                F.col("threshold_kmpl").isNull() |
                (F.col("fuel_liters") <= 0) |
                (F.col("distance_km") <= 0)
            )
        )

        if invalid_metrics.limit(1).count() > 0:
            invalid_metrics.show(20, truncate=False)
            raise ValueError("Invalid metric values found in fuel_efficiency_audit")

        log.info("Fuel efficiency audit validation passed.")

    except Exception as e:
        log.error(f"validate_audit failed: {e}")
        raise


def write_audit(spark, df, path, year, month, date):
    try:
        df = align_audit_schema(df)

        gold_exists = DeltaTable.isDeltaTable(spark, path)

        if gold_exists:
            (
                df.write
                .format("delta")
                .mode("overwrite")
                .option(
                    "replaceWhere",
                    f"year = {year} AND month = {month} AND date = {date}"
                )
                .option("mergeSchema", "false")
                .partitionBy("year", "month", "date")
                .save(path)
            )
        else:
            (
                df.write
                .format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .partitionBy("year", "month", "date")
                .save(path)
            )

        log.info(f"Gold fuel_efficiency_audit written to {path}")

    except Exception as e:
        log.error(f"write_audit failed: {e}")
        raise

def write_to_postgres(df):
    try:
        reporting_df = (
            df
            .select(
                "vin",
                "model",
                "audit_date",
                "km_per_liter",
                "baseline_kmpl",
                "status"
            )
            .withColumn("vin", F.col("vin").cast("string"))
            .withColumn("model", F.col("model").cast("string"))
            .withColumn("audit_date", F.col("audit_date").cast("date"))
            .withColumn("km_per_liter", F.col("km_per_liter").cast("double"))
            .withColumn("baseline_kmpl", F.col("baseline_kmpl").cast("double"))
            .withColumn("status", F.col("status").cast("string"))
        )

        (
            reporting_df
            .write
            .mode("overwrite")
            .jdbc(
                url=pg_url,
                table=pg_table,
                properties=pg_properties
            )
        )

        log.info(f"Fuel efficiency audit written to PostgreSQL table {pg_table}")

    except Exception as e:
        log.error(f"PostgreSQL write failed: {e}")
        raise
    
def main():
    log.info("OmniRoute | Gold | fuel_efficiency_audit | Glue Job")

    success = False

    args = getResolvedOptions(sys.argv, ["JOB_NAME", "logical_date"])

    run_date = args["logical_date"]
    logical_dt = datetime.strptime(run_date, "%Y-%m-%d").date()

    year = logical_dt.year
    month = logical_dt.month
    date = logical_dt.day

    log.info(f"Logical date: {run_date}")

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
        fuel_df = read_fuel_transactions(
            spark,
            SILVER_FUEL_TRANSACTIONS,
            year,
            month,
            date
        )

        registry_df = read_vehicle_registry(
            spark,
            SILVER_VEHICLE_REGISTRY,
            year,
            month,
            date
        )

        maintenance_df = read_maintenance_schedules(
            spark,
            SILVER_MAINTENANCE_SCHEDULES,
            run_date
        )

        audit_df = build_audit(
            spark,
            fuel_df,
            registry_df,
            maintenance_df,
            run_date,
            year,
            month,
            date
        )

        audit_df = align_audit_schema(audit_df)

        if audit_df.limit(1).count() > 0:
            validate_audit(audit_df)
        else:
            log.info("Audit DataFrame is empty. Validation skipped.")

        write_audit(
            spark,
            audit_df,
            GOLD_FUEL_EFFICIENCY_AUDIT,
            year,
            month,
            date
        )

        log.info("Audit schema:")
        audit_df.printSchema()

        write_to_postgres(audit_df)
        
        log.info("Audit sample:")
        audit_df.show(20, truncate=False)

        log.info("Status breakdown:")
        audit_df.groupBy("status").count().show(truncate=False)

        log.info("Flagged vehicles sample:")
        audit_df.filter(F.col("status") == STATUS_FLAGGED) \
            .select(
                "vin",
                "model",
                "audit_date",
                "km_per_liter",
                "baseline_kmpl",
                "threshold_kmpl"
            ) \
            .orderBy(F.asc("km_per_liter")) \
            .show(10, truncate=False)

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

        log.info("OmniRoute | fuel_efficiency_audit | Job End")


if __name__ == "__main__":
    main()