import re
import json
import boto3
import logging
import sys
from datetime import datetime, timezone
from dateutil import parser as date_parser

from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    FloatType,
)

from awsglue.transforms import *
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
BRONZE_PREFIX    = "poc-bootcamp-group1-bronze/fuel_transactions/"
BAD_DATA_PREFIX  = "poc-bootcamp-group1-bronze/bad_data/fuel_transactions/"
DQ_REPORT_PREFIX = "poc-bootcamp-group1-bronze/dq_reports/fuel_transactions/"
SILVER_PATH      = "s3://ttn-de-bootcamp-silver-us-east-1/poc-bootcamp-group1-silver/fuel_transactions/"

EXPECTED_COLUMNS = {
    "transaction_id",
    "vin",
    "fuel_liters",
    "odometer_reading",
    "timestamp"
}

SILVER_COLUMN_ORDER = [
    "transaction_id",
    "vin",
    "fuel_liters",
    "odometer_reading",
    "timestamp"
]

VALID_RUN_DATE = datetime.now(timezone.utc).date()

VALID_FUEL_LITERS_MIN = 0.0
VALID_FUEL_LITERS_MAX = 400.0
VALID_ODO_MIN         = 5.0
VALID_ODO_MAX         = 999_999.0

TXN_ID_PATTERN = re.compile(r"^TXN_[1-9][0-9]*$")

WORD_TO_NUMBER_MAP = {
    "zero": 0, "one": 1, "two": 2,
    "three": 3, "four": 4, "five": 5,
    "six": 6, "seven": 7, "eight": 8,
    "nine": 9, "ten": 10, "eleven": 11,
    "twelve": 12, "thirteen": 13, "fourteen": 14,
    "fifteen": 15, "sixteen": 16, "seventeen": 17,
    "eighteen": 18, "nineteen": 19, "twenty": 20,
    "thirty": 30, "forty": 40, "fifty": 50,
    "sixty": 60, "seventy": 70, "eighty": 80,
    "ninety": 90, "hundred": 100, "thousand": 1000,
}

DQ_RULESET = """
    Rules = [
        IsComplete "transaction_id",
        IsComplete "vin",
        IsComplete "fuel_liters",
        IsComplete "odometer_reading",
        IsComplete "timestamp",
        IsUnique   "transaction_id",
        ColumnValues "vin" matches "^[A-Z0-9]{8}$",
        ColumnValues "fuel_liters" > 0.0,
        ColumnValues "fuel_liters" <= 400.0,
        ColumnValues "odometer_reading" > 5.0,
        ColumnValues "odometer_reading" <= 999999.0
    ]
"""


def _normalize_transaction_id(raw):
    if not raw or not raw.strip():
        return ""

    stripped = raw.strip()

    if TXN_ID_PATTERN.fullmatch(stripped):
        return stripped

    match = re.search(r"(?<![0-9-])([1-9][0-9]*)", stripped)

    if match:
        number = match.group(1)

        if len(number) <= 15:
            return f"TXN_{number}"

    return ""


def _words_to_number(text):
    if not text:
        return None

    tokens = text.strip().lower().split()
    result = 0
    current = 0

    try:
        for token in tokens:
            if token not in WORD_TO_NUMBER_MAP:
                return None

            val = WORD_TO_NUMBER_MAP[token]

            if val == 1000:
                current = (current if current > 0 else 1) * 1000
                result += current
                current = 0
            elif val == 100:
                current = (current if current > 0 else 1) * 100
            else:
                current += val

        result += current

        if result > 0:
            return float(result)

        return None

    except Exception:
        return None


def _parse_numeric(raw):
    if not raw or not str(raw).strip():
        return None

    stripped = str(raw).strip()

    try:
        return float(stripped)
    except ValueError:
        pass

    match = re.search(r"(\d+(?:\.\d+)?)", stripped)

    if match:
        try:
            return float(match.group(1))
        except ValueError:
            pass

    return _words_to_number(stripped)


def _normalize_timestamp(raw):
    if not raw or not raw.strip():
        return ""

    stripped = raw.strip()

    try:
        value = float(stripped)

        if 1_000_000_000 <= value <= 9_999_999_999:
            parsed = datetime.fromtimestamp(
                value,
                tz=timezone.utc
            ).replace(tzinfo=None)

            if parsed.date() != VALID_RUN_DATE:
                return ""

            return parsed.strftime("%Y-%m-%d %H:%M:%S")

        return ""

    except (ValueError, OSError):
        pass

    try:
        parsed = datetime.strptime(stripped, "%Y-%m-%d %H:%M:%S")

        if parsed.date() != VALID_RUN_DATE:
            return ""

        return parsed.strftime("%Y-%m-%d %H:%M:%S")

    except ValueError:
        return ""


def _pre_validation(transaction_id, vin, fuel_liters, odometer_reading, timestamp):
    reasons = []

    if not transaction_id or not str(transaction_id).strip():
        reasons.append("transaction_id:null_or_empty")
    elif not TXN_ID_PATTERN.fullmatch(str(transaction_id).strip()):
        reasons.append("transaction_id:unrecoverable")

    if not vin or not str(vin).strip():
        reasons.append("vin:null_or_empty")
    elif len(str(vin).strip()) != 8:
        reasons.append("vin:invalid_length")
    elif not re.fullmatch(r"[A-Za-z0-9]{8}", str(vin).strip()):
        reasons.append("vin:invalid_format")

    if not fuel_liters or not str(fuel_liters).strip():
        reasons.append("fuel_liters:null_or_empty")
    else:
        parsed = _parse_numeric(fuel_liters)

        if parsed is None:
            reasons.append("fuel_liters:non_numeric")
        elif parsed <= VALID_FUEL_LITERS_MIN or parsed > VALID_FUEL_LITERS_MAX:
            reasons.append("fuel_liters:out_of_range")

    if not odometer_reading or not str(odometer_reading).strip():
        reasons.append("odometer_reading:null_or_empty")
    else:
        parsed = _parse_numeric(odometer_reading)

        if parsed is None:
            reasons.append("odometer_reading:non_numeric")
        elif parsed <= VALID_ODO_MIN or parsed > VALID_ODO_MAX:
            reasons.append("odometer_reading:out_of_range")

    if not timestamp or not str(timestamp).strip():
        reasons.append("timestamp:null_or_empty")
    else:
        normalized = _normalize_timestamp(timestamp)

        if not normalized:
            stripped = str(timestamp).strip()

            try:
                value = float(stripped)

                if 1_000_000_000 <= value <= 9_999_999_999:
                    parsed = datetime.fromtimestamp(value, tz=timezone.utc)

                    if parsed.date() != VALID_RUN_DATE:
                        reasons.append("timestamp:out_of_range")
                    else:
                        reasons.append("timestamp:invalid_format")
                else:
                    reasons.append("timestamp:invalid_format")

            except (ValueError, OSError):
                try:
                    parsed = date_parser.parse(stripped, dayfirst=False)

                    if parsed.date() != VALID_RUN_DATE:
                        reasons.append("timestamp:out_of_range")
                    else:
                        reasons.append("timestamp:invalid_format")

                except (ValueError, OverflowError, TypeError):
                    reasons.append("timestamp:unparseable")

    return " | ".join(reasons)


def _post_validation(transaction_id, vin, fuel_liters, odometer_reading, timestamp):
    reasons = []

    if not transaction_id or not str(transaction_id).strip():
        reasons.append("transaction_id:null_or_empty")
    elif not TXN_ID_PATTERN.fullmatch(str(transaction_id).strip()):
        reasons.append("transaction_id:unrecoverable")

    if not vin or not str(vin).strip():
        reasons.append("vin:null_or_empty")
    elif not re.fullmatch(r"^(?=.*[A-Z])(?=.*[0-9])[A-Z0-9]{8}$", str(vin).strip()):
        reasons.append("vin:invalid_format")

    if fuel_liters is None:
        reasons.append("fuel_liters:null_or_empty")
    else:
        try:
            value = float(str(fuel_liters))

            if value <= VALID_FUEL_LITERS_MIN or value > VALID_FUEL_LITERS_MAX:
                reasons.append("fuel_liters:out_of_range")

        except (ValueError, AttributeError):
            reasons.append("fuel_liters:non_numeric")

    if odometer_reading is None:
        reasons.append("odometer_reading:null_or_empty")
    else:
        try:
            value = float(str(odometer_reading))

            if value <= VALID_ODO_MIN or value > VALID_ODO_MAX:
                reasons.append("odometer_reading:out_of_range")

        except (ValueError, AttributeError):
            reasons.append("odometer_reading:non_numeric")

    if timestamp is None:
        reasons.append("timestamp:null_after_transform")

    return " | ".join(reasons)


normalize_txn_udf = udf(
    lambda value: _normalize_transaction_id(value) if value else "",
    StringType()
)

parse_numeric_udf = udf(
    lambda value: _parse_numeric(value),
    FloatType()
)

normalize_timestamp_udf = udf(
    lambda value: _normalize_timestamp(value) if value else "",
    StringType()
)

pre_validation_udf = udf(
    lambda txn, vin, fuel, odo, ts: _pre_validation(txn, vin, fuel, odo, ts),
    StringType()
)

post_validation_udf = udf(
    lambda txn, vin, fuel, odo, ts: _post_validation(txn, vin, fuel, odo, ts),
    StringType()
)


def initial_wipe(df):
    try:
        df = df.select([
            F.trim(F.col(c)).alias(c)
            for c in df.columns
        ])

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
                F.col("transaction_id"),
                F.col("vin"),
                F.col("fuel_liters"),
                F.col("odometer_reading"),
                F.col("timestamp")
            )
        )

        valid_df = df.filter(F.col("_pre_reason") == "").drop("_pre_reason")

        bad_df = (
            df
            .filter(F.col("_pre_reason") != "")
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
            .withColumn("transaction_id", normalize_txn_udf(F.col("transaction_id")))
            .withColumn("fuel_liters", parse_numeric_udf(F.col("fuel_liters")).cast(FloatType()))
            .withColumn("odometer_reading", parse_numeric_udf(F.col("odometer_reading")).cast(FloatType()))
            .withColumn(
                "timestamp",
                F.to_timestamp(
                    normalize_timestamp_udf(F.col("timestamp")),
                    "yyyy-MM-dd HH:mm:ss"
                )
            )
            .select(*SILVER_COLUMN_ORDER)
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
                F.col("transaction_id"),
                F.col("vin"),
                F.col("fuel_liters"),
                F.col("odometer_reading"),
                F.col("timestamp")
            )
        )

        df_clean = df.filter(F.col("_post_reason") == "").drop("_post_reason")

        df_bad = (
            df
            .filter(F.col("_post_reason") != "")
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

        df = df.dropDuplicates(["transaction_id"])

        after = df.count()

        log.info(f"deduplicate - removed: {before - after:,} | final: {after:,}")
        return df

    except Exception as e:
        log.error(f"deduplicate failed - {e}")
        raise


def add_partition_columns(df, run_year, run_month, run_day):
    try:
        df = (
            df
            .withColumn("year", F.lit(run_year).cast("int"))
            .withColumn("month", F.lit(run_month).cast("int"))
            .withColumn("date", F.lit(run_day).cast("int"))
        )

        log.info("add_partition_columns complete.")
        return df

    except Exception as e:
        log.error(f"add_partition_columns failed - {e}")
        raise


def run_data_quality(dyf, glue_context, run_date, s3_client):
    try:
        dq_results = EvaluateDataQuality.apply(
            frame=dyf,
            ruleset=DQ_RULESET,
            publishing_options={
                "dataQualityEvaluationContext": "fuel_transactions_dq",
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

        report_key = f"{DQ_REPORT_PREFIX}run_date={run_date}/dq_fuel_transactions.json"

        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=report_key,
            Body=json.dumps(dq_report, indent=2),
            ContentType="application/json"
        )

        passed = sum(1 for row in results if row["Outcome"] == "Passed")
        failed = sum(1 for row in results if row["Outcome"] == "Failed")

        log.info(f"DQ report written -> s3://{BUCKET_NAME}/{report_key}")
        log.info(f"DQ results - Passed: {passed} | Failed: {failed}")

    except Exception as e:
        log.warning(f"Data quality evaluation failed - {e}. Continuing.")


def write_bad_data(df_bad, run_date):
    try:
        bad_count = df_bad.count()

        if bad_count == 0:
            log.info("No bad rows - skipping bad-data write.")
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
        log.warning(f"write_bad_data failed - {e}. Continuing.")


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

        log.info("Null counts in Silver output:")

        df_clean.select([
            F.count(F.when(F.col(c).isNull(), c)).alias(c)
            for c in SILVER_COLUMN_ORDER
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
        log.warning(f"audit failed - {e}. Continuing.")


def main():
    log.info("OmniRoute | fuel_transactions | Bronze -> Silver | Glue Job")

    success = False

    args = getResolvedOptions(sys.argv, ["JOB_NAME", "logical_date"])

    run_date = args["logical_date"]
    logical_dt = datetime.strptime(run_date, "%Y-%m-%d").date()

    run_year = logical_dt.year
    run_month = logical_dt.month
    run_day = logical_dt.day

    global VALID_RUN_DATE
    VALID_RUN_DATE = logical_dt

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
        StructField("transaction_id", StringType(), True),
        StructField("vin", StringType(), True),
        StructField("fuel_liters", StringType(), True),
        StructField("odometer_reading", StringType(), True),
        StructField("timestamp", StringType(), True),
    ])

    try:
        bronze_key = f"{BRONZE_PREFIX}fuel_transactions_{run_date}.csv"

        try:
            s3.head_object(
                Bucket=BUCKET_NAME,
                Key=bronze_key
            )
        except Exception:
            raise ValueError(
                f"No CSV file found in Bronze: s3://{BUCKET_NAME}/{bronze_key}"
            )

        path = f"s3://{BUCKET_NAME}/{bronze_key}"

        log.info(f"Processing: {path}")

        dyf = glue_context.create_dynamic_frame.from_options(
            connection_type="s3",
            connection_options={
                "paths": [path],
                "recurse": False,
            },
            format="csv",
            format_options={
                "withHeader": True,
                "separator": ","
            }
        )

        log.info(f"Bronze read - {dyf.count()} rows.")

        df = dyf.toDF()

        for col_name in schema.fieldNames():
            if col_name in df.columns:
                df = df.withColumn(
                    col_name,
                    F.col(col_name).cast(StringType())
                )

        missing = EXPECTED_COLUMNS - set(df.columns)

        if missing:
            raise ValueError(f"Missing expected columns: {missing}")

        df_raw = df

        df = initial_wipe(df)

        df, df_bad_pre = pre_filter(df)

        df = apply_transformations(df)

        df_clean, df_bad_post = post_filter(df)

        df_clean = deduplicate(df_clean)

        df_clean = add_partition_columns(
            df_clean,
            run_year,
            run_month,
            run_day
        )

        audit(
            df_raw,
            df_clean,
            df_bad_pre,
            df_bad_post
        )

        all_bad = df_bad_pre.unionByName(
            df_bad_post,
            allowMissingColumns=True
        )

        write_bad_data(all_bad, run_date)

        dyf_clean = DynamicFrame.fromDF(
            df_clean,
            glue_context,
            "df_clean"
        )

        dyf_clean = dyf_clean.apply_mapping([
            ("transaction_id", "string", "transaction_id", "string"),
            ("vin", "string", "vin", "string"),
            ("fuel_liters", "float", "fuel_liters", "float"),
            ("odometer_reading", "float", "odometer_reading", "float"),
            ("timestamp", "timestamp", "timestamp", "timestamp"),
            ("year", "int", "year", "int"),
            ("month", "int", "month", "int"),
            ("date", "int", "date", "int"),
        ])

        run_data_quality(
            dyf_clean,
            glue_context,
            run_date,
            s3
        )

        final_df = dyf_clean.toDF()

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

        log.info(f"Silver fuel_transactions written -> {SILVER_PATH}")

    except Exception as e:
        log.error(f"Job failed - {e}")
        raise

    finally:
        if success:
            job.commit()
            log.info("Glue job committed.")
        else:
            log.error("Glue job failed. Commit skipped.")

        log.info("OmniRoute | fuel_transactions | Job End")


if __name__ == "__main__":
    main()