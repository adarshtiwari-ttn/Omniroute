import boto3
import random
import string
import logging
from datetime import datetime, timezone, time
import awswrangler as wr

BUCKET = 'ttn-de-bootcamp-bronze-us-east-1'
DATASET_BUCKET = 'ttn-de-bootcamp-silver-us-east-1'

logging.basicConfig(
    level= logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

# Generators


def generate_out_of_domain_vins():
    
    # Invalid VIN formats
    out_of_domain_vins = [
        "12345678",  # Only digits
        "ABCDEFGH",  # Only letters
    ]
    
    # Non-existent VINs with different lengths
    for i in range(1, 15):
        if i == 8:
            continue  # Skip valid length
        generated_vin = ''.join(
            random.choices(
                string.ascii_uppercase + string.digits,
                k= i # Too long or too short
            )
        )
        out_of_domain_vins.append(generated_vin)
            
    return out_of_domain_vins

def generate_out_of_domain_driver_ids():
    
    out_of_domain_driver_ids = []
    
    for i in range(1, 21):
        
        # IDs that are out of the valid range
        out_of_domain_driver_ids.append(f"DRV_{10_000_000_000 + i}")
        
        out_of_domain_driver_ids.append(f"DRV_{-10_000_000_000 + i}")
        
        # IDs with invalid formats
        
        out_of_domain_driver_ids.extend([
            f"DRIVER_{i}", # Full word instead of abbreviation
            f"DR@V_{i}", # Rabdom character in between
            f"DRV-{i}", # Hyphen instead of underscore
            f"DRV{i}", # Missing underscore
            f"DRV_{i}Xyz", # Random suffix
            f"12345_{i}", # Random prefix
            f"DRV_12345_{i}", # Random prefix
            f"drv_{i}" # Casing issue
            ])
        
    return out_of_domain_driver_ids
    
def generate_out_of_domain_start_timestamps(start_ts):
    
    out_of_domain_start_timestamps = []
    day_ts = 24 * 60 * 60  # seconds in a day
        
    for i in range(-3, 4):  # 3 days before to 3 days after
        if i:  # Exclude the valid start date
            out_of_domain_start_timestamps.append(
                (start_ts + i * day_ts)
            )
    # Too old timestamp
    out_of_domain_start_timestamps.extend(
        [(start_ts + i*day_ts) for i in range(-10000, -9995, -1)]
    )
    
    # Too futuristic timestamp
    out_of_domain_start_timestamps.extend(
        [(start_ts + i*day_ts) for i in range(9995, 10000)]
    )
    
    return out_of_domain_start_timestamps

def generate_out_of_domain_end_timestamps(end_ts):
    
    out_of_domain_end_timestamps = []
    
    day_ts = 24 * 60 * 60  # seconds in a day
        
    for i in range(-3, 4):  # 3 days before to 3 days after
        if i:  # Exclude the valid start date
            out_of_domain_end_timestamps.append(
                (end_ts + i*day_ts)
            )
            
    # Too old timestamp
    out_of_domain_end_timestamps.extend(
        [(end_ts + i*day_ts) for i in range(-10000, -9995, -1)]
    )
    
    # Too futuristic timestamp
    out_of_domain_end_timestamps.extend(
        [(end_ts + i*day_ts) for i in range(9995, 10000)]
    )
    
    return out_of_domain_end_timestamps

def generate_out_of_domain_daily_rates():
    
    out_of_domain_daily_rates = []
    
    # Negative rates
    for _ in range(5):
        out_of_domain_daily_rates.extend([random.randint(-100, -1)])
    
    # Unrealistically high rates
    out_of_domain_daily_rates.extend([10_000, 100_000])
    
    # unrealistically low rates
    out_of_domain_daily_rates.extend([random.randint(0, 50) for i in range(5)])
    
    # Unrounded rates
    out_of_domain_daily_rates.extend([round(random.uniform(200, 1000), i) for i in range(5)])
    
    # Non-numeric values
    out_of_domain_daily_rates.extend(["Free", "N/A", "Five Hundred"])
    
    return out_of_domain_daily_rates
    
def generate_out_of_domain_regions():
    
    out_of_domain_regions = []
    
    # Non-existent regions
    out_of_domain_regions.extend(["Central", "North-Central", "International"])
    
    # Typos/random characters in between
    out_of_domain_regions.extend(["North%h", "Sou0tth", "E@asst", "We1st"])
    
    # Short Forms
    out_of_domain_regions.extend(["N", "S", "E", "W", "NE", "NW", "SE", "SW"])
    
    # Misspelled regions
    out_of_domain_regions.extend(["Noth", "Soth", "Eest", "Wst"])
    
    # Non-geographical values
    out_of_domain_regions.extend(["Urban", "Rural", "Suburban"])
    
    return out_of_domain_regions
    
def generate_driver_id(logical_date, i):
    clean_date = logical_date.strftime("%Y%m%d")
    return f"DRV_{clean_date}{i}"


def generate_start_timestamp(START_TS_MIN, START_TS_MAX):
    return int(random.uniform(START_TS_MIN, START_TS_MAX))


def bad_data(value, prob=0.05, out_of_domain=None):
    generated_prob = random.random()
    
    if out_of_domain is None:
        out_of_domain = []

    if generated_prob < prob / 2 and out_of_domain:
        return str(random.choice(out_of_domain))  # out of domain value

    return value if generated_prob > prob else ""  # null

def generate_vehicle_assignment_data(**context):
    
    # logical date from Airflow
    logical_date_ds = context['ds']
    
    logical_date = datetime.strptime(logical_date_ds, "%Y-%m-%d").date()
    year = logical_date.year
    month = logical_date.month
    date = logical_date.day
    
    logger.info(f"Generating vehicle assignment data on {logical_date}:\n")
    
    # Domain values
    regions = ["North", "South", "East", "West", "North-East", "North-West", "South-West", "South-East"]

    # Unix timestamps for logical date
    START_TS_MIN = int(datetime.combine(
        logical_date,
        time(0, 0, 0),
        tzinfo=timezone.utc
    ).timestamp())

    START_TS_MAX = int(datetime.combine(
        logical_date,
        time(23, 59, 59),
        tzinfo=timezone.utc
    ).timestamp())

    data = []
    
    s3 = boto3.client('s3')
    try:
        vehicle_registry = wr.s3.read_parquet(path=f"s3://{DATASET_BUCKET}/poc-bootcamp-group1-silver/vehicle_registry/year={year}/month={month}/date={date}/"
        )
        logger.info("Vehicle registry dataset loaded successfully from S3")
    except Exception as e:
        logger.error(f"Error loading vehicle registry dataset from S3: {e}")
        return
    
    vin_list = vehicle_registry["vin"].tolist()
    
    if not vin_list:
        logger.error("VIN list is empty")
        return
    # Writing Header
    data.append("vin,driver_id,start_timestamp,end_timestamp,daily_rate,region\n")

    
    base_start_ts = generate_start_timestamp(START_TS_MIN, START_TS_MAX)
    end_ts = START_TS_MAX
    
    # Calling generators
    
    out_of_domain_vins = generate_out_of_domain_vins()
    out_of_domain_driver_ids = generate_out_of_domain_driver_ids()
    out_of_domain_start_timestamps = generate_out_of_domain_start_timestamps(base_start_ts)
    out_of_domain_end_timestamps = generate_out_of_domain_end_timestamps(end_ts)
    out_of_domain_daily_rates = generate_out_of_domain_daily_rates()
    out_of_domain_regions = generate_out_of_domain_regions()    
    
    
    n = 150_000 if month == 1 and date == 1 else 10_000

    # Generating data
    for i in range(n):  # 10,000 records
        
        base_start_ts = generate_start_timestamp(START_TS_MIN, START_TS_MAX)
        
        vin = bad_data(
            random.choice(
                vin_list
            ), 
            0.02,
            out_of_domain_vins
        )  # 2% null and out of domain 

        driver_id = bad_data(
            generate_driver_id(logical_date, i), 
            0.03,
            out_of_domain_driver_ids
        ) # 3% null and out of domain
            
        start_ts = bad_data(
            base_start_ts, 
            0.02, 
            out_of_domain_start_timestamps
        )
        
        end_ts = bad_data(
            None, # null 
            0.03,
            out_of_domain_end_timestamps
        )

        daily_rate = bad_data(
            str(round(random.uniform(200, 1000), 2)),
            0.05,
            out_of_domain_daily_rates
        )

        region = bad_data(
            random.choice(regions), 
            0.05, 
            out_of_domain_regions
        ) # 5% null and out of domain

        row = f"{vin},{driver_id},{start_ts},{end_ts},{daily_rate},{region}\n"
        data.append(row)
        
    # Upload to S3
    try:
        KEY = f'poc-bootcamp-group1-bronze/vehicle_assignment/vehicle_assignment_{logical_date}.csv'

        s3.put_object(Bucket=BUCKET, Key=f"{KEY}", Body=''.join(data))
        logger.info(f"Vehicle assignment data for {logical_date} uploaded to S3 successfully.")
        
    except Exception as e:
        logger.error(f"Error uploading vehicle registry data to S3: {e}")