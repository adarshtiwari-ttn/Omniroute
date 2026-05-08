import sys
import re
import json
import boto3
import logging
from word2number import w2n
from datetime import datetime, timezone

# PySpark imports
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.window import Window

# Glue imports  
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from awsgluedq.transforms import EvaluateDataQuality

# Logging setup
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger(__name__)

# CONFIGURATION
BUCKET_NAME      = "ttn-de-bootcamp-bronze-us-east-1"
BRONZE_PREFIX = "poc-bootcamp-group1-bronze/vehicle_registry/"
BAD_DATA_PREFIX = "poc-bootcamp-group1-bronze/bad_data/vehicle_registry/"
DQ_REPORT_PREFIX = "poc-bootcamp-group1-bronze/dq_reports/vehicle_registry/"
SILVER_PATH = "s3://ttn-de-bootcamp-silver-us-east-1/poc-bootcamp-group1-silver/vehicle_registry/"

EXPECTED_COLUMNS = {"vin", "model", "mfg_year", "fuel_type", "baseline_kmpl"}

VALID_FUEL_TYPES = ["Diesel", "Petrol", "Cng", "Lng"]
VALID_MFG_YEAR_MIN = 2020
VALID_MFG_YEAR_MAX = 2025
VALID_KMPL_MIN     = 2.0
VALID_KMPL_MAX     = 60.0

ABBREVIATIONS = {
    "petroleum": "Petrol",
    "gasoline": "Petrol",
    "compressednaturalgas": "Cng",
    "liquefiednaturalgas": "Lng",
    "liquifiednaturalgas": "Lng"
}

# GLUE DATA QUALITY RULESET
DQ_RULESET = """
    Rules = [
        IsComplete "vin",
        IsComplete "model",
        IsComplete "mfg_year",
        IsComplete "fuel_type",
        IsComplete "baseline_kmpl",
        IsUnique   "vin",
        ColumnValues "vin" matches "^[A-Z0-9]{8}$",
        ColumnValues "mfg_year" >= 2020,
        ColumnValues "mfg_year" <= 2025,
        ColumnValues "fuel_type" in [ "Diesel", "Petrol", "Cng", "Lng" ],
        ColumnValues "baseline_kmpl" >= 2.0,
        ColumnValues "baseline_kmpl" <= 60.0
    ]
"""

# HELPER FUNCTIONS & UDFS
def _normalize_fuel_python(raw: str) -> str:
    if not raw:
        return ""
    
    stripped = raw.strip()
    
    if not stripped:
        return ""
    
    stripped_lower = re.sub(r"[^A-Za-z]", "", stripped).lower()
    
    if not stripped_lower:
        return ""
    
    if stripped_lower in ABBREVIATIONS:
        return ABBREVIATIONS[stripped_lower]

    for fuel in VALID_FUEL_TYPES:
        ct = 0
        i, j = 0, 0
        tmp = fuel.lower()
        while i< len(stripped_lower) and j < len(tmp):
            if stripped_lower[i] == tmp[j]:
                ct += 1
                i += 1
            j += 1
            
        if ct / len(tmp) >= 0.90:  # 90% character match threshold
            return fuel
            
    return ""

def _words_to_number(text: str):
    try: 
        result = w2n.word_to_num(text)
        log.info(f"_words_to_number converted '{text}' to {result}")
        return result
    
    except ValueError: 
        log.error(f"word_to_number conversion failed for '{text}'")
        return None

def _pre_validation(vin, model, mfg_year, fuel_type, baseline_kmpl) -> str: 
    reasons = []
    # vin
    if not vin or not str(vin).strip(): 
        reasons.append("vin: null_or_empty")
    elif len(str(vin).strip()) != 8: 
        reasons.append("vin: invalid_length")
    elif not re.fullmatch(r"[A-Za-z0-9]{8}", str(vin).strip()): 
        reasons.append("vin: invalid_format")
        
    # model
    if not model or not str(model).strip(): 
        reasons.append("model: null_or_empty")
    else: 
        try: 
            float(str(model).strip())
            reasons.append("model: column_shift_detected")
        except ValueError: 
            pass
        
    # fuel_type
    if not fuel_type or not str(fuel_type).strip(): 
        reasons.append("fuel_type: null_or_empty")
    else: 
        try: 
            float(str(fuel_type).strip()) 
            reasons.append("fuel_type: column_shift_detected")
        except ValueError: 
            pass
        
    # mfg_year
    if not mfg_year or not str(mfg_year).strip(): 
        reasons.append("mfg_year: null_or_empty")
        
    # baseline_kmpl
    if not baseline_kmpl or not str(baseline_kmpl).strip(): 
        reasons.append("baseline_kmpl: null_or_empty")
    else: 
        try: 
            float(str(baseline_kmpl).strip()) 
        except ValueError: 
            if _words_to_number(str(baseline_kmpl).strip()) is None: 
                reasons.append("baseline_kmpl: non_numeric")
                
    return " | ".join(reasons)

def _post_validation(vin, model, mfg_year, fuel_type, baseline_kmpl) -> str: 
    
    reasons = []
    
    # vin
    if not vin or not str(vin).strip(): 
        reasons.append("vin: null_or_empty")
    elif not re.fullmatch(r"^(?=.*[A-Z])(?=.*[0-9])[A-Z0-9]{8}$", str(vin).strip()): 
        reasons.append("vin: invalid_format")
        
    # model 
    if not model or not str(model).strip(): 
        reasons.append("model: null_or_empty")
    
    # mfg_year
    if mfg_year is None: 
        reasons.append("mfg_year: null_or_empty")
    else: 
        try: 
            yr = int(str(mfg_year).strip())
            if yr < VALID_MFG_YEAR_MIN or yr > VALID_MFG_YEAR_MAX: 
                reasons.append("mfg_year: out_of_range")
        except: 
            reasons.append("mfg_year: non_numeric")
            
    # fuel_type
    if not fuel_type or not str(fuel_type).strip(): 
        reasons.append("fuel_type: null_or_empty")
    else: 
        if str(fuel_type).strip() not in VALID_FUEL_TYPES: 
            reasons.append("fuel_type: unrecognized")
            
    # baseline_kmpl
    if baseline_kmpl is None: 
        reasons.append("baseline_kmpl: null_or_empty")
    else: 
        try: 
            kmpl = float(str(baseline_kmpl).strip())
            if kmpl < VALID_KMPL_MIN: 
                reasons.append("baseline_kmpl: too_low")
            elif kmpl > VALID_KMPL_MAX: 
                reasons.append("baseline_kmpl: too_high")
        except: 
            reasons.append("baseline_kmpl: non_numeric")
            
    return " | ".join(reasons)

@udf(StringType())
def normalize_fuel_udf(v): 
    result = _normalize_fuel_python(v)
    return result if result else None

@udf(IntegerType())
def word_to_year(v): 
    if v is None: return None
    return _words_to_number(v)

pre_validation_udf = udf(
    lambda v, mo, y, f, k: _pre_validation(v, mo, y, f, k), 
    StringType()
)

post_validation_udf = udf(
    lambda v, mo, y, f, k: _post_validation(v, mo, y, f, k), 
    StringType()
)

# Transformations

def initial_casing(df): 
    df = df.select([F.trim(F.col(c)).alias(c) for c in df.columns])
    df = df.withColumn("vin", F.upper(F.col("vin")))
    df = df.withColumn("model", F.initcap(F.col("model")))
    df = df.withColumn("fuel_type", F.initcap(F.col("fuel_type")))
    return df

def clean_vin(df): 
    return df.withColumn("vin", 
            F.when(F.col("vin") \
            .rlike(r"^(?=.*[A-Z])(?=.*[0-9])[A-Z0-9]{8}$"), \
                F.col("vin")) \
                .otherwise(F.lit(None)))

def clean_model(df): 
    """clean and invalidate models"""
    # Standardize spacing
    df = df.withColumn("model_clean", 
                       F.trim(
                           F.regexp_replace(
                               F.col("model"), r"\s+", " ")
                           )
                       )

    # Abbreviation Logic (First character of every word)
    df = df.withColumn("model_abbrev", 
            F.upper(
                F.expr(
                    "array_join(transform(split(model_clean, ' '), x -> substring(x, 1, 1)), '')"
                    )
                ))

    # Dynamic Matching
    invalid_model_expr = (
        F.col("model_clean").isNull() |
        F.lower(F.col("model_clean")).contains("unknown") |
        F.lower(F.col("model_clean")).contains("n/a") |
        # Regex catches: "Model X1", "Model 123", "Model-Alpha", "Model@2020", "Model#1"
        F.col("model_clean").rlike(r"(?i)^model[\s\-\@\#]+([a-z]\d?|\d+|alpha|beta)$") |
        # Bad acronyms
        F.col("model_abbrev").isin("UM", "NA", "TBD")
    )

    # 4. Apply Invalidation
    df = df.withColumn("model", F.when(invalid_model_expr, F.lit(None)).otherwise(F.col("model_clean")))

    # 5. Final Formatting
    df = df.withColumn("model", F.regexp_replace(F.col("model"), r"(?<=\w)-(?=\w)", " "))
    df = df.withColumn("model", F.regexp_replace(F.col("model"), r"[^a-zA-Z0-9\s]", ""))
    df = df.withColumn("model", F.trim(F.regexp_replace(F.col("model"), r"\s+", " ")))

    return df.drop("model_clean", "model_abbrev")

def clean_mfg_year(df): 
    df = df.withColumn("year_t1", F.col("mfg_year").cast(IntegerType()))
    df = df.withColumn("year_t2", F.when(F.col("year_t1").isNull(), F.regexp_extract(F.col("mfg_year"), r"(20\d{2})", 1).cast(IntegerType())).otherwise(F.col("year_t1")))
    df = df.withColumn("year_t3", F.when(F.col("year_t2").isNull(), word_to_year(F.col("mfg_year"))).otherwise(F.col("year_t2")))
    return df.withColumn("mfg_year", F.when((F.col("year_t3") >= F.lit(VALID_MFG_YEAR_MIN)) & (F.col("year_t3") <= F.lit(VALID_MFG_YEAR_MAX)), F.col("year_t3")).otherwise(F.lit(None))).drop("year_t1", "year_t2", "year_t3")

def clean_fuel_type(df): 
    return df.withColumn("fuel_type", normalize_fuel_udf(F.col("fuel_type")))

def clean_baseline_kmpl(df): 
    df = df.withColumn("kmpl_raw", F.regexp_extract(F.col("baseline_kmpl"), r"(\d+(?:\.\d+)?)", 1))
    df = df.withColumn("baseline_kmpl", F.col("kmpl_raw").cast(DoubleType())).drop("kmpl_raw")
    return df.withColumn("baseline_kmpl", F.when((F.col("baseline_kmpl") > F.lit(VALID_KMPL_MIN)) & (F.col("baseline_kmpl") <= F.lit(VALID_KMPL_MAX)), F.col("baseline_kmpl")).otherwise(F.lit(None)))

def deduplicate(df): 
    df = df.dropDuplicates()
    completeness_expr = (
        F.when(F.col("model").isNotNull(), 1).otherwise(0) +
        F.when(F.col("mfg_year").isNotNull(), 1).otherwise(0) +
        F.when(F.col("fuel_type").isNotNull(), 1).otherwise(0) +
        F.when(F.col("baseline_kmpl").isNotNull(), 1).otherwise(0)
    )
    df = df.withColumn("data_score", completeness_expr)
    w_dedup = Window.partitionBy("vin").orderBy(F.desc("data_score"))
    return df.withColumn("rn", F.row_number().over(w_dedup)).filter(F.col("rn") == 1).drop("rn", "data_score")

def pre_filter(df): 
    df = df.withColumn("_pre_reason", pre_validation_udf(F.col("vin"), F.col("model"), F.col("mfg_year"), F.col("fuel_type"), F.col("baseline_kmpl")))
    valid_df = df.filter(F.col("_pre_reason") == "").drop("_pre_reason")
    bad_df   = df.filter(F.col("_pre_reason") != "").withColumnRenamed("_pre_reason", "rejection_reason")
    return valid_df, bad_df

def post_filter(df): 
    df = df.withColumn("_post_reason", post_validation_udf(F.col("vin"), F.col("model"), F.col("mfg_year"), F.col("fuel_type"), F.col("baseline_kmpl")))
    df_clean = df.filter(F.col("_post_reason") == "").drop("_post_reason")
    df_bad   = df.filter(F.col("_post_reason") != "").withColumnRenamed("_post_reason", "rejection_reason")
    
    df_clean = (df_clean
        .withColumn("mfg_year",      F.col("mfg_year").cast(IntegerType()))
        .withColumn("baseline_kmpl", F.col("baseline_kmpl").cast(DoubleType()))
        .withColumn("vin",           F.upper(F.col("vin")))
        .withColumn("model",         F.initcap(F.col("model")))
        .withColumn("fuel_type",     F.initcap(F.col("fuel_type")))
        .select(*EXPECTED_COLUMNS)
    )
    return df_clean, df_bad

def add_ingested_date(df, logical_date): 
    now = datetime.combine(logical_date, datetime.min.time())

    df = df.withColumn("ingested_timestamp", F.lit(now.strftime("%Y-%m-%d %H:%M:%S")).cast("timestamp"))
    
    # Extract partition columns so they are available in the schema
    df = df.withColumn("year", F.year(F.col("ingested_timestamp")))
    df = df.withColumn("month", F.month(F.col("ingested_timestamp")))
    df = df.withColumn("date", F.dayofmonth(F.col("ingested_timestamp")))
    return df

# UTILITIES (DQ, WRITERS, AUDIT)
def run_data_quality(dyf, glue_context, run_date, s3_client): 
    try: 
        dq_results = EvaluateDataQuality.apply(
            frame=dyf, ruleset=DQ_RULESET,
            publishing_options={
                "dataQualityEvaluationContext": "vehicle_registry_dq",
                "enableDataQualityCloudWatchMetrics": False,
                "enableDataQualityResultsPublishing": True,
            }
        )
        results = dq_results.toDF().collect()
        dq_report = {
            "dq_run_date": run_date,
            "dq_run_timestamp": datetime.now(timezone.utc).isoformat(),
            "dq_results": [{"rule": r["Rule"], "outcome": r["Outcome"], "details": r.asDict().get("FailureReason", "")} for r in results]
        }
        report_key = f"{DQ_REPORT_PREFIX}run_date={run_date}/dq_vehicle_registry.json"
        s3_client.put_object(Bucket=BUCKET_NAME, Key=report_key, Body=json.dumps(dq_report, indent=2), ContentType="application/json")
    except Exception as e: 
        log.warning(f"Data quality evaluation failed : {e}")

def write_bad_data(df_bad, glue_context, run_date):
    try: 
        if df_bad.rdd.isEmpty(): return
        bad_path = f"s3://{BUCKET_NAME}/{BAD_DATA_PREFIX}run_date={run_date}/"
        bad_dyf  = DynamicFrame.fromDF(df_bad, glue_context, "bad_data")
        glue_context.write_dynamic_frame.from_options(
            frame=bad_dyf, connection_type="s3", 
            connection_options={"path": bad_path}, 
            format="csv", 
            format_options={"withHeader": True}
        )
    except Exception as e: 
        log.warning(f"write_bad_data failed: {e}")

def audit(df_raw, df_clean, df_bad_pre, df_bad_post): 
    df_clean.cache()
    log.info(f"Raw rows: {df_raw.count()} | Clean rows: {df_clean.count()} | Total quarantine: {df_bad_pre.count() + df_bad_post.count()}")

# MAIN EXECUTION
def main(): 
    log.info("OmniRoute | vehicle_registry | Bronze -> Silver | Glue Job")
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "logical_date"])
    logical_date = datetime.strptime(args["logical_date"], "%Y-%m-%d").date()
    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)
    spark.sparkContext.setLogLevel("ERROR")

    s3 = boto3.client("s3")
    run_date = logical_date
    
    schema = StructType([
        StructField("vin", StringType(), True),
        StructField("model", StringType(), True),
        StructField("mfg_year", StringType(), True),
        StructField("fuel_type", StringType(), True),
        StructField("baseline_kmpl", StringType(), True),
    ])

    try: 
        files = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=BRONZE_PREFIX).get("Contents", [])
        csv_files = [f["Key"] for f in files if f["Key"].endswith(".csv")]

        if not csv_files: 
            log.info("No CSV files found. Exiting gracefully.")
            job.commit()
            return

        for key in csv_files: 
            try: 
                dyf = glue_context.create_dynamic_frame.from_options(
                    connection_type="s3", connection_options={"paths": [f"s3://{BUCKET_NAME}/{key}"], "recurse": True},
                    format="csv", format_options={"withHeader": True, "separator": ","}
                )
                
                df = dyf.toDF()
                for col_name in schema.fieldNames(): 
                    if col_name in df.columns: 
                        df = df.withColumn(col_name, F.col(col_name).cast(StringType()))

                df_raw = df

                # 1. Pre-validation
                df, df_bad_pre = pre_filter(df)

                # 2. Transformations
                df = initial_casing(df)
                df = clean_vin(df)
                df = clean_model(df)
                df = clean_mfg_year(df)
                df = clean_fuel_type(df)
                df = clean_baseline_kmpl(df)
                df = deduplicate(df)

                # 3. Post-validation
                df_clean, df_bad_post = post_filter(df)
                df_clean = add_ingested_date(df_clean, logical_date)

                # 4. Audit & Bad Data Routing
                audit(df_raw, df_clean, df_bad_pre, df_bad_post)
                all_bad = df_bad_pre.unionByName(df_bad_post, allowMissingColumns=True)
                write_bad_data(all_bad, glue_context, run_date)

                # 5. Silver Write
                dyf_clean = DynamicFrame.fromDF(df_clean, glue_context, "df_clean")
                
                # Apply mapping including the new partition columns
                dyf_clean = dyf_clean.apply_mapping([
                    ("vin", "string", "vin", "string"),
                    ("model", "string", "model", "string"),
                    ("mfg_year", "int", "mfg_year", "int"),
                    ("fuel_type", "string", "fuel_type", "string"),
                    ("baseline_kmpl", "double", "baseline_kmpl", "double"),
                    ("ingested_timestamp", "timestamp", "ingested_timestamp", "timestamp"),
                    ("year", "int", "year", "int"),
                    ("month", "int", "month", "int"),
                    ("date", "int", "date", "int")
                ])
                
                run_data_quality(dyf_clean, glue_context, run_date, s3)

                # Enable dynamic partition overwrite
                spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

                #  Controlled overwrite
                df_clean_write = dyf_clean.toDF()

                (
                    df_clean_write.write
                    .mode("overwrite")
                    .format("parquet")
                    .partitionBy("year", "month", "date")
                    .save(SILVER_PATH)
                )

            except Exception as e: 
                log.error(f"Unexpected failure processing {key} : {e}")

    except Exception as e: 
        log.error(f"Job-level failure : {e}")
        raise
    finally: 
        job.commit()
        log.info(" OmniRoute | vehicle_registry | Job End")

if __name__ == "__main__": 
    main()