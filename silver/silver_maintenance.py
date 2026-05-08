import re
import json
import sys
import boto3
import logging
from datetime import datetime, timezone
from difflib import get_close_matches
from dateutil import parser as date_parser
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import (
    StructType, StructField, StringType, DateType
)

# Glue imports 
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from awsgluedq.transforms import EvaluateDataQuality

# LOGGING
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger(__name__)


# CONFIG
BUCKET_NAME      = "ttn-de-bootcamp-bronze-us-east-1"
BRONZE_PREFIX    = "poc-bootcamp-group1-bronze/maintenance_schedules/"
BAD_DATA_PREFIX  = "poc-bootcamp-group1-bronze/bad_data/maintenance_schedules/"
DQ_REPORT_PREFIX = "poc-bootcamp-group1-bronze/dq_reports/maintenance_schedules/"
SILVER_PATH      = "s3://ttn-de-bootcamp-silver-us-east-1/poc-bootcamp-group1-silver/maintenance_schedules/"
EXPECTED_COLUMNS = ["vin", "service_date", "service_type"]
SILVER_COLUMN_ORDER = EXPECTED_COLUMNS

# This is populated in main() from LOGICAL_DATE passed by Airflow.
VALID_YEAR = datetime.now(timezone.utc).year


# Domain constants 
SERVICE_TYPE_CUTOFF = 0.75
VALID_SERVICE_TYPES = [
    "Engine overhaul",
    "Oil change",
    "Tire rotation",
    "Wheel alignment",
    "Brake service",
    "Battery service",
    "Air filter replacement",
    "Coolant service",
    "Transmission service",
    "Suspension service",
    "AC service",
    "Fuel system cleaning",
    "General inspection",
    "Electrical system repair",
]

_SERVICE_TYPE_LOWER_MAP = {s.lower(): s for s in VALID_SERVICE_TYPES}

# Glue Data Quality Ruleset
DQ_RULESET = """
    Rules = [
        IsComplete "vin",
        IsComplete "service_date",
        IsComplete "service_type",
        ColumnValues "vin" matches "^[A-Z0-9]{8}$",
        ColumnValues "service_type" in [
            "Engine overhaul",
            "Oil change",
            "Tire rotation",
            "Wheel alignment",
            "Brake service",
            "Battery service",
            "Air filter replacement",
            "Coolant service",
            "Transmission service",
            "Suspension service",
            "AC service",
            "Fuel system cleaning",
            "General inspection",
            "Electrical system repair"
        ]
    ]
"""


# PURE-PYTHON HELPERS
def _normalize_service_date(raw):
    if not raw or not raw.strip():
        return ""
    stripped = raw.strip()

    # YYYYMMDD 
    if re.fullmatch(r"\d{8}", stripped):
        try:
            parsed = datetime.strptime(stripped, "%Y%m%d")
            if parsed.year != VALID_YEAR:
                return ""
            return parsed.strftime("%Y-%m-%d")
        except ValueError:
            return ""

    #  other formats via dateutil
    try:
        parsed = date_parser.parse(stripped, dayfirst=True)
        if parsed.year != VALID_YEAR:
            return ""
        return parsed.strftime("%Y-%m-%d")
    except (ValueError, OverflowError, TypeError):
        return ""


def _normalize_service_type(raw):
    if not raw or not raw.strip():
        return ""
    stripped = raw.strip()

    # exact match
    if stripped in VALID_SERVICE_TYPES:
        return stripped

    # case-insensitive
    lower = stripped.lower()
    if lower in _SERVICE_TYPE_LOWER_MAP:
        return _SERVICE_TYPE_LOWER_MAP[lower]

    # strip special chars then fuzzy match
    cleaned = re.sub(r"[^a-zA-Z\s/]", "", stripped).strip()
    if not cleaned:
        return ""
    matches = get_close_matches(cleaned, VALID_SERVICE_TYPES, n=1, cutoff=SERVICE_TYPE_CUTOFF)
    return matches[0] if matches else ""


def _pre_validation(vin, service_date, service_type) -> str:
    reasons = []

    # VIN
    if not vin or not str(vin).strip():
        reasons.append("vin:null_or_empty")
    elif len(str(vin).strip()) != 8:
        reasons.append("vin:invalid_length")
    elif not re.fullmatch(r"[A-Za-z0-9]{8}", str(vin).strip()):
        reasons.append("vin:invalid_format")

    # service_date - null check + basic parseability 
    if not service_date or not str(service_date).strip():
        reasons.append("service_date:null_or_empty")
    else:
        stripped = str(service_date).strip()
        parsed_date = None
        if re.fullmatch(r"\d{8}", stripped):
            try:
                parsed_date = datetime.strptime(stripped, "%Y%m%d")
            except ValueError:
                pass
        else:
            try:
                parsed_date = date_parser.parse(stripped, dayfirst=True)
            except (ValueError, OverflowError, TypeError):
                pass
        if parsed_date is None:
            reasons.append("service_date:invalid_format")
        elif parsed_date.year != VALID_YEAR:
            reasons.append("service_date:out_of_range")

    # service_type - null check only at pre-validation stage
    if not service_type or not str(service_type).strip():
        reasons.append("service_type:null_or_empty")
    else:
        # Check for column shift - numeric value in service_type
        try:
            float(str(service_type).strip())
            reasons.append("service_type:column_shift_detected")
        except ValueError:
            pass

    return " | ".join(reasons)


def _post_validation(vin, service_date, service_type) -> str:
    reasons = []

    #  VIN 
    if not vin or not str(vin).strip():
        reasons.append("vin:null_or_empty")
    elif not re.fullmatch(r"^(?=.*[A-Z])(?=.*[0-9])[A-Z0-9]{8}$", str(vin).strip()):
        reasons.append("vin:invalid_format")

    # service_date 
    if service_date is None:
        reasons.append("service_date:null_or_empty")

    # service_type 
    if not service_type or not str(service_type).strip():
        reasons.append("service_type:null_or_empty")
    else:
        if str(service_type).strip() not in set(VALID_SERVICE_TYPES):
            reasons.append("service_type:unrecognized")

    return " | ".join(reasons)


# UDF REGISTRATIONS
normalize_date_udf = udf(
    lambda d: _normalize_service_date(d) if d else "",
    StringType()
)

normalize_type_udf = udf(
    lambda t: _normalize_service_type(t) if t else "",
    StringType()
)

pre_validation_udf = udf(
    lambda v, d, t: _pre_validation(v, d, t),
    StringType()
)

post_validation_udf = udf(
    lambda v, d, t: _post_validation(v, d, t),
    StringType()
)


# TRANSFORMATION FUNCTIONS
def initial_wipe(df):
    try:
        df = df.select([F.trim(F.col(c)).alias(c) for c in df.columns])
        df = df.withColumn("vin", F.upper(F.col("vin")))
        log.info("initial_wipe complete.")
        return df
    except Exception as e:
        log.error(f"initial_wipe failed - {e}")
        raise


def pre_filter(df):
    try:
        df = df.withColumn(
            "_pre_reason",
            pre_validation_udf(
                F.col("vin"), F.col("service_date"), F.col("service_type")
            )
        )
        valid_df = df.filter(F.col("_pre_reason") == "").drop("_pre_reason")
        bad_df   = (
            df.filter(F.col("_pre_reason") != "")
              .withColumnRenamed("_pre_reason", "rejection_reason")
        )
        log.info(f"pre_filter - valid: {valid_df.count():,} | quarantine: {bad_df.count():,}")
        return valid_df, bad_df
    except Exception as e:
        log.error(f"pre_filter failed - {e}")
        raise


def apply_transformations(df):
    try:
        df = (
            df
            .withColumn(
                "service_date",
                F.to_date(normalize_date_udf(F.col("service_date")), "yyyy-MM-dd")
            )
            .withColumn("service_type", normalize_type_udf(F.col("service_type")))
            .withColumn("year", F.year(F.col("service_date"))) # Extract year from date
            .select(*SILVER_COLUMN_ORDER, "year") # Ensure year stays in the dataframe
        )
        log.info("apply_transformations complete.")
        return df
    except Exception as e:
        log.error(f"apply_transformations failed - {e}")
        raise


def post_filter(df):
    try:
        df = df.withColumn(
            "_post_reason",
            post_validation_udf(
                F.col("vin"), F.col("service_date"), F.col("service_type")
            )
        )
        df_clean = df.filter(F.col("_post_reason") == "").drop("_post_reason")
        df_bad   = (
            df.filter(F.col("_post_reason") != "")
              .withColumnRenamed("_post_reason", "rejection_reason")
        )
        log.info(f"post_filter - clean: {df_clean.count():,} | quarantine: {df_bad.count():,}")
        return df_clean, df_bad
    except Exception as e:
        log.error(f"post_filter failed - {e}")
        raise


def deduplicate(df):
    try:
        before = df.count()
        df     = df.dropDuplicates(["vin", "service_date", "service_type"])
        after  = df.count()
        log.info(f"deduplicate - removed: {before - after:,} | final: {after:,}")
        return df
    except Exception as e:
        log.error(f"deduplicate failed - {e}")
        raise


# GLUE DATA QUALITY
def run_data_quality(dyf, glue_context, run_year, s3_client):
    try:
        dq_results = EvaluateDataQuality.apply(
            frame=dyf,
            ruleset=DQ_RULESET,
            publishing_options={
                "dataQualityEvaluationContext"      : "maintenance_schedules_dq",
                "enableDataQualityCloudWatchMetrics": False,
                "enableDataQualityResultsPublishing": True,
            }
        )
        results_df = dq_results.toDF()
        results    = results_df.collect()

        dq_report = {
            "dq_run_year"      : run_year,
            "dq_run_timestamp" : datetime.now(timezone.utc).isoformat(),
            "dq_results"       : [
                {
                    "rule"   : row["Rule"],
                    "outcome": row["Outcome"],
                    "details": row.asDict().get("FailureReason", "")
                }
                for row in results
            ]
        }
        report_key = f"{DQ_REPORT_PREFIX}run_year={run_year}/dq_maintenance_schedules.json"
        s3_client.put_object(
            Bucket=BUCKET_NAME, Key=report_key,
            Body=json.dumps(dq_report, indent=2),
            ContentType="application/json"
        )
        log.info(f"DQ report written → s3://{BUCKET_NAME}/{report_key}")
        passed = sum(1 for r in results if r["Outcome"] == "Passed")
        failed = sum(1 for r in results if r["Outcome"] == "Failed")
        log.info(f"DQ results - Passed: {passed} | Failed: {failed}")
    except Exception as e:
        log.warning(f"Data quality evaluation failed - {e}. Continuing.")


# BAD DATA WRITER
def write_bad_data(df_bad, glue_context, run_year):
    try:
        if df_bad.rdd.isEmpty():
            log.info("No bad rows - skipping bad-data write.")
            return
        bad_count = df_bad.count()
        bad_path  = f"s3://{BUCKET_NAME}/{BAD_DATA_PREFIX}run_year={run_year}/"
        bad_dyf   = DynamicFrame.fromDF(df_bad, glue_context, "bad_data")
        glue_context.write_dynamic_frame.from_options(
            frame=bad_dyf,
            connection_type="s3",
            connection_options={"path": bad_path},
            format="csv",
            format_options={"withHeader": True},
        )
        log.info(f"{bad_count:,} bad rows written → {bad_path}")
    except Exception as e:
        log.warning(f"write_bad_data failed - {e}. Continuing.")


# AUDIT
def audit(df_raw, df_clean, df_bad_pre, df_bad_post):
    try:
        df_clean.cache()
        raw_count      = df_raw.count()
        clean_count    = df_clean.count()
        bad_pre_count  = df_bad_pre.count()
        bad_post_count = df_bad_post.count()

        log.info("─" * 55)
        log.info(f"  Raw rows         : {raw_count:>10,}")
        log.info(f"  Clean rows       : {clean_count:>10,}")
        log.info(f"  Pre-filter bad   : {bad_pre_count:>10,}")
        log.info(f"  Post-filter bad  : {bad_post_count:>10,}")
        log.info(f"  Total quarantine : {bad_pre_count + bad_post_count:>10,}")
        log.info("─" * 55)

        log.info("Null counts in Silver output (all must be 0):")
        df_clean.select([
            F.count(F.when(F.col(c).isNull(), c)).alias(c)
            for c in SILVER_COLUMN_ORDER
        ]).show()

        log.info("Silver schema:")
        df_clean.printSchema()

        log.info("Silver sample (5 rows):")
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
    except Exception as e:
        log.warning(f"audit failed - {e}. Continuing.")


# MAIN GLUE JOB
def main():
    log.info("OmniRoute | maintenance_schedules | Bronze → Silver | Glue Job")

    # Glue context setup 
    try:
        args = getResolvedOptions(sys.argv, ["JOB_NAME", "logical_date"])
    except Exception:
        args = getResolvedOptions(sys.argv, ["JOB_NAME"])
    sc           = SparkContext()
    glue_context = GlueContext(sc)
    spark        = glue_context.spark_session
    job          = Job(glue_context)
    job.init(args["JOB_NAME"], args)
    spark.sparkContext.setLogLevel("ERROR")

    s3       = boto3.client("s3")
    run_date = args.get("logical_date", datetime.now(timezone.utc).strftime("%Y-%m-%d"))

    global VALID_YEAR
    VALID_YEAR = datetime.strptime(run_date, "%Y-%m-%d").year
    run_year = str(VALID_YEAR)

    # All STRING - bad values stay visible for cleaning
    schema = StructType([
        StructField("vin",          StringType(), True),
        StructField("service_date", StringType(), True),
        StructField("service_type", StringType(), True),
    ])

    try:
        # 1.Discover CSV files in bronze prefix
        # S3KeySensor in Airflow DAG ensures files exist before this job runs
        target_key = f"{BRONZE_PREFIX}maintenance_schedules_{run_year}.csv"

        try:
            s3.head_object(Bucket=BUCKET_NAME, Key=target_key)
        except Exception:
            log.info(f"No maintenance schedule file found for year={run_year}. Exiting.")
            job.commit()
            return

        csv_files = [target_key]

        log.info(f"{len(csv_files)} CSV file(s) found. Run date: {run_date} | Run year: {run_year}")

        # 2. Process each file
        for key in csv_files:
            log.info("═" * 55)
            log.info(f"Processing: s3://{BUCKET_NAME}/{key}")
            log.info("═" * 55)

            try:
                # Read as DynamicFrame
                dyf = glue_context.create_dynamic_frame.from_options(
                    connection_type="s3",
                    connection_options={
                        "paths"  : [f"s3://{BUCKET_NAME}/{key}"],
                        "recurse": True,
                    },
                    format="csv",
                    format_options={"withHeader": True, "separator": ","},
                )
                log.info(f"Bronze read - {dyf.count()} rows.")

                # Convert to DataFrame for transformations
                df = dyf.toDF()
                for col_name in schema.fieldNames():
                    if col_name in df.columns:
                        df = df.withColumn(col_name, F.col(col_name).cast(StringType()))

                missing = set(EXPECTED_COLUMNS) - set(df.columns)
                if missing:
                    raise ValueError(f"Missing expected columns: {missing}")

                df_raw = df

                # Step 1: Global trim + uppercase VIN
                try:
                    df = initial_wipe(df)
                except Exception as e:
                    log.warning(f"Skipping initial_wipe - {e}")

                # Pre-filter on RAW values
                df, df_bad_pre = pre_filter(df)

                # Apply transformations on valid rows only
                try:
                    df = apply_transformations(df)
                except Exception as e:
                    log.error(f"apply_transformations failed - {e}")
                    raise

                # Catches business rule violations after transformation
                df_clean, df_bad_post = post_filter(df)

                #Deduplicate
                try:
                    df_clean = deduplicate(df_clean)
                except Exception as e:
                    log.warning(f"Skipping deduplicate - {e}")

                # Audit 
                audit(df_raw, df_clean, df_bad_pre, df_bad_post)

                # Merge pre + post quarantine and write
                all_bad = df_bad_pre.unionByName(df_bad_post, allowMissingColumns=True)
                write_bad_data(all_bad, glue_context, run_year)

                # Convert back to DynamicFrame
                dyf_clean = DynamicFrame.fromDF(df_clean, glue_context, "df_clean")
                dyf_clean = dyf_clean.apply_mapping([
                    ("vin",           "string", "vin",           "string"),
                    ("service_date",  "date",   "service_date",  "date"),
                    ("service_type",  "string", "service_type",  "string"),
                    ("year",          "int",    "year",          "int")
                ])
                log.info("apply_mapping complete.")

                # Glue Data Quality
                run_data_quality(dyf_clean, glue_context, run_year, s3)

                # ── Write Silver - Parquet overwrite partitioned by year
                glue_context.write_dynamic_frame.from_options(
                    frame=dyf_clean,
                    connection_type="s3",
                    connection_options={
                        "path"         : SILVER_PATH,
                        "partitionKeys": ["year"],
                    },
                    format="parquet",
                )
                log.info(f"Silver written → {SILVER_PATH}")
            except ValueError as ve:
                log.error(f"Validation error for {key} - {ve}")
            except Exception as e:
                log.error(f"Unexpected failure processing {key} - {e}")

    except Exception as e:
        log.error(f"Job-level failure - {e}")
        raise

    finally:
        job.commit()
        log.info("Glue job committed.")
        log.info("OmniRoute | maintenance_schedules | Job End")


if __name__ == "__main__":
    main()