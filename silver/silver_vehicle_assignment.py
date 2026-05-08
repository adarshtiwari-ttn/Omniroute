import re
import json
import boto3
import logging
import sys
from datetime import datetime, timezone

from word2number import w2n

from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    FloatType,
    DateType
)

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from pyspark.conf import SparkConf
from awsgluedq.transforms import EvaluateDataQuality


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger(__name__)


BUCKET_NAME      = "ttn-de-bootcamp-bronze-us-east-1"
BRONZE_PREFIX    = "poc-bootcamp-group1-bronze/vehicle_assignment/"
BAD_DATA_PREFIX  = "poc-bootcamp-group1-bronze/bad_data/vehicle_assignment/"
DQ_REPORT_PREFIX = "poc-bootcamp-group1-bronze/dq_reports/vehicle_assignment/"
SILVER_PATH      = "s3://ttn-de-bootcamp-silver-us-east-1/poc-bootcamp-group1-silver/vehicle_assignment/"

VALID_DAILY_RATE_MIN = 200.0
VALID_DAILY_RATE_MAX = 1000.0

DRIVER_ID_PATTERN = re.compile(r"^DRV_(\d+)$")

VALID_REGIONS = [
    "North", "South", "East", "West",
    "North-East", "North-West", "South-West", "South-East",
]

REGION_MAP = {
    r.upper(): r for r in VALID_REGIONS
}

RAW_COLUMNS = [
    "vin",
    "driver_id",
    "start_timestamp",
    "end_timestamp",
    "daily_rate",
    "region"
]

SILVER_COLUMNS = [
    "vin",
    "driver_id",
    "start_date",
    "end_date",
    "daily_rate",
    "region"
]

VALID_RUN_DATE = datetime.now(timezone.utc).date()


DQ_RULESET = """
    Rules = [
        IsComplete "vin",
        IsComplete "driver_id",
        IsComplete "start_date",
        IsComplete "daily_rate",
        IsComplete "region",
        ColumnValues "vin" matches "[A-Za-z0-9]{8}",
        ColumnValues "driver_id" matches "^DRV_[0-9]+$",
        ColumnValues "daily_rate" >= 200.0,
        ColumnValues "daily_rate" <= 1000.0,
        ColumnValues "region" in [ "North", "South", "East", "West", "North-East", "North-West", "South-East", "South-West" ]
    ]
"""


def _normalize_region(raw):
    if not raw or not raw.strip():
        return ""

    stripped = raw.strip()
    upper = stripped.upper()

    if upper in REGION_MAP:
        return REGION_MAP[upper]

    cleaned = re.sub(r"[^a-zA-Z\-\s]", "", stripped).strip()

    if not cleaned:
        return ""

    cleaned = cleaned.upper()

    for region in VALID_REGIONS:
        region_upper = region.upper()

        ct = 0
        i, j = 0, 0

        while i < len(cleaned) and j < len(region_upper):
            if cleaned[i] == region_upper[j]:
                ct += 1
                i += 1
            j += 1

        if ct / len(region_upper) >= 0.90:
            return region

    return ""


def _words_to_number(text):
    try:
        return w2n.word_to_num(text)
    except ValueError:
        return None


def _parse_daily_rate(raw):
    if not raw or not str(raw).strip():
        return None

    stripped = str(raw).strip()

    try:
        return float(stripped)
    except ValueError:
        pass

    return _words_to_number(stripped)


def _is_null_timestamp(val):
    return (
        val is None
        or str(val).strip() == ""
        or str(val).strip().lower() == "none"
    )


def _pre_validation(vin, driver_id, start_ts, end_ts, daily_rate, region):
    reasons = []

    if not vin or not str(vin).strip():
        reasons.append("vin:null_or_empty")
    elif len(str(vin).strip()) != 8:
        reasons.append("vin:invalid_length")
    elif not re.fullmatch(r"[A-Za-z0-9]{8}", str(vin).strip()):
        reasons.append("vin:invalid_format")

    if not driver_id or not str(driver_id).strip():
        reasons.append("driver_id:null_or_empty")
    else:
        try:
            float(str(driver_id).strip())
            reasons.append("driver_id:column_shift_detected")
        except ValueError:
            pass

    if not start_ts or not str(start_ts).strip():
        reasons.append("start_timestamp:null_or_empty")
    else:
        try:
            float(str(start_ts).strip())
        except ValueError:
            reasons.append("start_timestamp:non_numeric")

    if not _is_null_timestamp(end_ts):
        try:
            float(str(end_ts).strip())
        except ValueError:
            reasons.append("end_timestamp:non_numeric")

    if not daily_rate or not str(daily_rate).strip():
        reasons.append("daily_rate:null_or_empty")
    else:
        try:
            float(str(daily_rate).strip())
        except ValueError:
            if _words_to_number(str(daily_rate).strip()) is None:
                reasons.append("daily_rate:non_numeric")

    if not region or not str(region).strip():
        reasons.append("region:null_or_empty")
    else:
        try:
            float(str(region).strip())
            reasons.append("region:column_shift_detected")
        except ValueError:
            pass

    return " | ".join(reasons)


def _post_validation(vin, driver_id, start_date, end_date, daily_rate, region):
    reasons = []

    if not vin or not str(vin).strip():
        reasons.append("vin:null_or_empty")
    elif not re.fullmatch(r"^(?=.*[A-Z])(?=.*[0-9])[A-Z0-9]{8}$", str(vin).strip()):
        reasons.append("vin:invalid_format")

    if not driver_id or not str(driver_id).strip():
        reasons.append("driver_id:null_or_empty")
    else:
        match = DRIVER_ID_PATTERN.fullmatch(str(driver_id).strip())
        if not match:
            reasons.append("driver_id:invalid_format")

    if start_date is None:
        reasons.append("start_date:null_or_empty")
    elif start_date > VALID_RUN_DATE:
        reasons.append("start_date:future_date")

    if end_date is not None:
        if start_date is not None and end_date < start_date:
            reasons.append("end_date:before_start_date")
        elif end_date > VALID_RUN_DATE:
            reasons.append("end_date:future_date")

    if daily_rate is None:
        reasons.append("daily_rate:null_or_empty")
    else:
        try:
            rate = float(str(daily_rate).strip())
            if rate < VALID_DAILY_RATE_MIN or rate > VALID_DAILY_RATE_MAX:
                reasons.append("daily_rate:out_of_range")
        except (ValueError, AttributeError):
            reasons.append("daily_rate:non_numeric")

    if not region or not str(region).strip():
        reasons.append("region:null_or_empty")
    else:
        if str(region).strip() not in set(VALID_REGIONS):
            reasons.append("region:unrecognized")

    return " | ".join(reasons)


normalize_region_udf = udf(
    lambda r: _normalize_region(r) if r else "",
    StringType()
)

parse_rate_udf = udf(
    lambda r: _parse_daily_rate(r),
    FloatType()
)

pre_validation_udf = udf(
    lambda v, d, s, e, r, rg: _pre_validation(v, d, s, e, r, rg),
    StringType()
)

post_validation_udf = udf(
    lambda v, d, sd, ed, r, rg: _post_validation(v, d, sd, ed, r, rg),
    StringType()
)


def initial_casing(df):
    try:
        df = df.select([F.trim(F.col(c)).alias(c) for c in df.columns])
        df = df.withColumn("vin", F.upper(F.col("vin")))
        df = df.withColumn("driver_id", F.upper(F.col("driver_id")))
        log.info("initial_casing complete.")
        return df
    except Exception as e:
        log.error(f"initial_casing failed : {e}")
        raise


def pre_filter(df):
    try:
        df = df.withColumn(
            "_pre_reason",
            pre_validation_udf(
                F.col("vin"),
                F.col("driver_id"),
                F.col("start_timestamp"),
                F.col("end_timestamp"),
                F.col("daily_rate"),
                F.col("region")
            )
        )

        valid_df = df.filter(F.col("_pre_reason") == "").drop("_pre_reason")

        bad_df = (
            df.filter(F.col("_pre_reason") != "")
              .withColumnRenamed("_pre_reason", "rejection_reason")
        )

        log.info(f"pre_filter : valid: {valid_df.count():,} | quarantine: {bad_df.count():,}")
        return valid_df, bad_df

    except Exception as e:
        log.error(f"pre_filter failed : {e}")
        raise


def apply_transformations(df):
    try:
        df = (
            df
            .withColumn(
                "start_date",
                F.to_date(F.from_unixtime(F.col("start_timestamp").cast("double")))
            )
            .withColumn(
                "end_date",
                F.when(
                    F.col("end_timestamp").isNull()
                    | (F.col("end_timestamp") == "")
                    | (F.lower(F.col("end_timestamp")) == "none"),
                    F.lit(None).cast(DateType())
                ).otherwise(
                    F.to_date(F.from_unixtime(F.col("end_timestamp").cast("double")))
                )
            )
            .withColumn("region", normalize_region_udf(F.col("region")))
            .withColumn("daily_rate", parse_rate_udf(F.col("daily_rate")).cast(FloatType()))
            .drop("start_timestamp", "end_timestamp")
            .select(*SILVER_COLUMNS)
        )

        log.info("apply_transformations complete.")
        return df

    except Exception as e:
        log.error(f"apply_transformations failed : {e}")
        raise


def post_filter(df):
    try:
        df = df.withColumn(
            "_post_reason",
            post_validation_udf(
                F.col("vin"),
                F.col("driver_id"),
                F.col("start_date"),
                F.col("end_date"),
                F.col("daily_rate"),
                F.col("region")
            )
        )

        df_clean = df.filter(F.col("_post_reason") == "").drop("_post_reason")

        df_bad = (
            df.filter(F.col("_post_reason") != "")
              .withColumnRenamed("_post_reason", "rejection_reason")
        )

        log.info(f"post_filter : clean: {df_clean.count():,} | quarantine: {df_bad.count():,}")
        return df_clean, df_bad

    except Exception as e:
        log.error(f"post_filter failed : {e}")
        raise


def add_ingested_date(df, run_date):
    try:
        df = df.withColumn(
            "ingested_date",
            F.to_timestamp(F.lit(f"{run_date} 00:00:00"))
        )
        df = df.withColumn("year", F.year(F.col("ingested_date")))
        df = df.withColumn("month", F.month(F.col("ingested_date")))
        df = df.withColumn("date", F.dayofmonth(F.col("ingested_date")))

        log.info("add_ingested_date complete.")
        return df

    except Exception as e:
        log.error(f"add_ingested_date failed : {e}")
        raise


def run_data_quality(dyf, glue_context, run_date, s3_client):
    try:
        dq_results = EvaluateDataQuality.apply(
            frame=dyf,
            ruleset=DQ_RULESET,
            publishing_options={
                "dataQualityEvaluationContext": "vehicle_assignment_dq",
                "enableDataQualityCloudWatchMetrics": False,
                "enableDataQualityResultsPublishing": True,
            }
        )

        results_df = dq_results.toDF()
        results = results_df.collect()

        dq_report = {
            "dq_run_date": run_date,
            "dq_run_timestamp": datetime.now(timezone.utc).isoformat(),
            "dq_results": [
                {
                    "rule": row["Rule"],
                    "outcome": row["Outcome"],
                    "details": row.asDict().get("FailureReason", "")
                }
                for row in results
            ]
        }

        report_key = f"{DQ_REPORT_PREFIX}run_date={run_date}/dq_vehicle_assignment.json"

        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=report_key,
            Body=json.dumps(dq_report, indent=2),
            ContentType="application/json"
        )

        log.info(f"DQ report written -> s3://{BUCKET_NAME}/{report_key}")

    except Exception as e:
        log.warning(f"Data quality evaluation failed : {e}")


def write_bad_data(df_bad, run_date):
    try:
        bad_count = df_bad.count()

        if bad_count == 0:
            log.info("No bad rows : skipping bad-data write.")
            return

        bad_path = f"s3://{BUCKET_NAME}/{BAD_DATA_PREFIX}run_date={run_date}/"

        (
            df_bad
            .write
            .mode("overwrite")
            .option("header", True)
            .csv(bad_path)
        )

        log.info(f"{bad_count:,} bad rows written -> {bad_path}")

    except Exception as e:
        log.warning(f"write_bad_data failed : {e}. Continuing.")


def audit(df_raw, df_clean, df_bad_pre, df_bad_post):
    try:
        df_clean.cache()

        raw_count = df_raw.count()
        clean_count = df_clean.count()
        bad_pre_count = df_bad_pre.count()
        bad_post_count = df_bad_post.count()

        log.info("-" * 55)
        log.info(f"  Raw rows         : {raw_count:>10,}")
        log.info(f"  Clean rows       : {clean_count:>10,}")
        log.info(f"  Pre-filter bad   : {bad_pre_count:>10,}")
        log.info(f"  Post-filter bad  : {bad_post_count:>10,}")
        log.info(f"  Total quarantine : {bad_pre_count + bad_post_count:>10,}")
        log.info("-" * 55)

        mandatory_cols = ["vin", "driver_id", "start_date", "daily_rate", "region"]

        log.info("Null counts in Silver output:")
        df_clean.select([
            F.count(F.when(F.col(c).isNull(), c)).alias(c)
            for c in mandatory_cols
        ]).show()

        log.info("Silver schema:")
        df_clean.printSchema()

        log.info("Silver sample:")
        df_clean.show(5, truncate=False)

        if bad_pre_count > 0:
            log.info("Pre-filter rejection reasons:")
            df_bad_pre.groupBy("rejection_reason") \
                      .count().orderBy(F.col("count").desc()) \
                      .show(20, truncate=False)

        if bad_post_count > 0:
            log.info("Post-filter rejection reasons:")
            df_bad_post.groupBy("rejection_reason") \
                       .count().orderBy(F.col("count").desc()) \
                       .show(20, truncate=False)

        df_clean.unpersist()

    except Exception as e:
        log.warning(f"audit failed : {e}. Continuing.")


def main():
    log.info("OmniRoute | vehicle_assignment | Bronze -> Silver | Glue Job")

    success = False

    args = getResolvedOptions(sys.argv, ["JOB_NAME", "logical_date"])

    run_date = args["logical_date"]
    logical_dt = datetime.strptime(run_date, "%Y-%m-%d").date()

    global VALID_RUN_DATE
    VALID_RUN_DATE = logical_dt

    run_year = logical_dt.year
    run_month = logical_dt.month
    run_day = logical_dt.day

    conf = SparkConf()
    conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    sc = SparkContext(conf=conf)
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session

    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    spark.sparkContext.setLogLevel("ERROR")

    s3 = boto3.client("s3")

    schema = StructType([
        StructField("vin", StringType(), True),
        StructField("driver_id", StringType(), True),
        StructField("start_timestamp", StringType(), True),
        StructField("end_timestamp", StringType(), True),
        StructField("daily_rate", StringType(), True),
        StructField("region", StringType(), True),
    ])

    try:
        bronze_key = bronze_key = (
            f"poc-bootcamp-group1-bronze/"
            f"vehicle_assignment/"
            f"vehicle_assignment_{run_date}.csv"
        )


        try:
            s3.head_object(Bucket=BUCKET_NAME, Key=bronze_key)
        except Exception:
            raise ValueError(f"No CSV file found in Bronze: s3://{BUCKET_NAME}/{bronze_key}")

        csv_files = [bronze_key]

        paths = [
            f"s3://{BUCKET_NAME}/{key}"
            for key in csv_files
        ]

        dyf = glue_context.create_dynamic_frame.from_options(
            connection_type="s3",
            connection_options={"paths": paths},
            format="csv",
            format_options={"withHeader": True}
        )

        df = dyf.toDF()

        for col_name in schema.fieldNames():
            if col_name in df.columns:
                df = df.withColumn(col_name, F.col(col_name).cast(StringType()))

        missing = set(RAW_COLUMNS) - set(df.columns)

        if missing:
            raise ValueError(f"Missing expected columns: {missing}")

        df_raw = df

        df = initial_casing(df)
        df, df_bad_pre = pre_filter(df)

        df = apply_transformations(df)

        df_clean, df_bad_post = post_filter(df)

        df_clean = add_ingested_date(df_clean, run_date)

        audit(df_raw, df_clean, df_bad_pre, df_bad_post)

        all_bad = df_bad_pre.unionByName(df_bad_post, allowMissingColumns=True)
        write_bad_data(all_bad, run_date)

        dyf_clean = DynamicFrame.fromDF(df_clean, glue_context, "df_clean")

        dyf_clean = dyf_clean.apply_mapping([
            ("vin", "string", "vin", "string"),
            ("driver_id", "string", "driver_id", "string"),
            ("start_date", "date", "start_date", "date"),
            ("end_date", "date", "end_date", "date"),
            ("daily_rate", "float", "daily_rate", "float"),
            ("region", "string", "region", "string"),
            ("ingested_date", "timestamp", "ingested_date", "timestamp"),
            ("year", "int", "year", "int"),
            ("month", "int", "month", "int"),
            ("date", "int", "date", "int"),
        ])

        run_data_quality(dyf_clean, glue_context, run_date, s3)

        final_df = (
            dyf_clean
            .toDF()
            .dropDuplicates([
                "vin",
                "driver_id",
                "start_date"
            ])
        )

        final_df.write \
            .format("delta") \
            .mode("overwrite") \
            .option(
                "replaceWhere",
                f"year = {run_year} AND month = {run_month} AND date = {run_day}"
            ) \
            .partitionBy("year", "month", "date") \
            .save(SILVER_PATH)

        success = True
        log.info(f"Silver vehicle_assignment written -> {SILVER_PATH}")

    except Exception as e:
        log.error(f"Job failed : {e}")
        raise

    finally:
        if success:
            job.commit()
            log.info("Glue job committed.")
        else:
            log.error("Glue job failed. Commit skipped.")

        log.info("OmniRoute | vehicle_assignment | Job End")


if __name__ == "__main__":
    main()