import boto3
import random
import string
import logging
import io
import awswrangler as wr
from datetime import datetime, timedelta, timezone
from collections import defaultdict


BUCKET = "ttn-de-bootcamp-bronze-us-east-1"
DATASET_BUCKET = "ttn-de-bootcamp-silver-us-east-1"

BRONZE_PREFIX = "poc-bootcamp-group1-bronze/fuel_transactions/"
REGISTRY_PREFIX = "poc-bootcamp-group1-silver/vehicle_registry/"
VEHICLE_DATASET_KEY = "poc-bootcamp-group1-bronze/static/vehicles_dataset.txt"


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)


# Domain values

distance_per_vehicle_type = {
    "heavy truck": (300, 800),
    "medium-sized truck": (200, 500),
    "LCV": (100, 300),
    "Car": (50, 200),
    "Bike": (20, 80),
}


# Out-of-domain values

out_of_domain_txn = []
out_of_domain_vins = []
out_of_domain_fuel_liters = []
out_of_domain_odo = []
out_of_domain_timestamps = []


# Generators

def generate_out_of_domain_txn():
    out_of_domain_txn.clear()

    # Casing issues
    out_of_domain_txn.extend([
        "Txn_123",
        "TxN_456",
        "tXn_789"
    ])

    # Typos
    out_of_domain_txn.extend([
        "TXN_a123",
        "TXN_123b",
        "TX*N_1@23"
    ])

    # Abbreviation issues
    out_of_domain_txn.extend([
        "TRANSACTION_123",
        "TX_123",
        "TNX_123",
        "T_123"
    ])

    # Negative and unrealistic values
    out_of_domain_txn.extend([
        "TXN_-123",
        "TXN_0"
    ])


def generate_out_of_domain_vins(vins):
    out_of_domain_vins.clear()

    # Non-existent VINs
    for _ in range(20):
        generated_vin = "".join(
            random.choices(
                string.ascii_uppercase + string.digits,
                k=8
            )
        )

        if generated_vin not in vins:
            out_of_domain_vins.append(generated_vin)

    # Invalid VIN formats
    out_of_domain_vins.extend([
        "12345678",
        "ABCDEFGH",
        "A1B2C3D4E5",
        "A1B2C3"
    ])


def generate_out_of_domain_fuel_liters():
    out_of_domain_fuel_liters.clear()

    # Negative, zero and unrealistic values
    out_of_domain_fuel_liters.extend([-10, 0])
    out_of_domain_fuel_liters.extend([500, 1000, 2000])

    # Non-numeric values
    out_of_domain_fuel_liters.extend([
        "Fifty",
        "N/A",
        "100liters",
        "liters50",
        "100l"
    ])


def generate_out_of_domain_odo():
    out_of_domain_odo.clear()

    # Negative, zero and unrealistic values
    out_of_domain_odo.extend([-100, 0])
    out_of_domain_odo.extend([1, 2])
    out_of_domain_odo.extend([1_000_000, 10_000_000, 100_000_000])

    # Non-numeric values
    out_of_domain_odo.extend([
        "One Hundred",
        "N/A",
        "100km",
        "km100",
        "100k"
    ])


def generate_out_of_domain_timestamps(logical_start):
    out_of_domain_timestamps.clear()

    ts = int(logical_start.timestamp())
    day_ts = 24 * 60 * 60

    # Nearby invalid dates
    for i in range(-3, 4):
        if i:
            out_of_domain_timestamps.append(ts + i * day_ts)

    # Very old timestamps
    out_of_domain_timestamps.extend([
        ts + i * day_ts
        for i in range(-10000, -9995)
    ])

    # Futuristic timestamps
    out_of_domain_timestamps.extend([
        ts + i * day_ts
        for i in range(9995, 10000)
    ])

    # Invalid timestamp formats
    out_of_domain_timestamps.extend([
        "2025-12-31 23:59:59",
        "2027-01-01 00:00:00",
        "Invalid Timestamp",
        "2026/01/01 00:00:00",
        "01-01-2026 00:00:00",
        "2026-01-01T00:00:00Z",
        "2026-01-01 00:00"
    ])


def generate_txn_id(i):
    return f"TXN_{i}"


def generate_distance(model_type):
    min_distance, max_distance = distance_per_vehicle_type[model_type]
    return random.uniform(min_distance, max_distance)


def generate_fuel(baseline, distance):
    try:
        baseline = float(baseline)
    except (TypeError, ValueError):
        baseline = random.uniform(3, 20)

    if baseline <= 0:
        baseline = random.uniform(3, 20)

    fuel_needed = distance / baseline
    variation = random.uniform(0.75, 1.25)

    return round(fuel_needed * variation, 2)


def generate_timestamp(logical_start):
    start_ts = int(logical_start.timestamp())
    end_ts = int((logical_start + timedelta(hours=12)).timestamp())

    return random.uniform(start_ts, end_ts)


def bad_data(value, prob=0.05, out_of_domain=None):
    if out_of_domain is None:
        out_of_domain = []

    r = random.random()

    if r < prob / 2 and out_of_domain:
        return str(random.choice(out_of_domain))

    return value if r > prob else ""

def normalize_model(model):
    return " ".join(str(model).strip().lower().split())

def read_vehicle_type_map():
    vehicle_type_map = {}

    s3 = boto3.client("s3")

    obj = s3.get_object(
        Bucket=BUCKET,
        Key=VEHICLE_DATASET_KEY
    )

    content = obj["Body"].read().decode("utf-8")

    for line in content.splitlines():
        parts = [x.strip() for x in line.strip().split(",")]

        if len(parts) < 5:
            continue

        model, model_type, fuel_type, mfg_year, baseline_kmpl = parts
        vehicle_type_map[normalize_model(model)] = model_type

    return vehicle_type_map


def read_vehicle_registry(year, month, date):
    path = (
        f"s3://{DATASET_BUCKET}/"
        f"{REGISTRY_PREFIX}"
        f"year={year}/month={month}/date={date}/"
    )

    df = wr.s3.read_parquet(
        path=path,
        dataset=True
    )

    required_columns = ["vin", "model", "baseline_kmpl"]

    for col_name in required_columns:
        if col_name not in df.columns:
            raise ValueError(f"Missing required column in vehicle_registry: {col_name}")

    vehicle_type_map = read_vehicle_type_map()

    df = df[["vin", "model", "baseline_kmpl"]].copy()

    df["vin"] = df["vin"].astype(str).str.strip()
    df["model"] = df["model"].astype(str).str.strip()
    df["vehicle_type"] = df["model"].apply(normalize_model).map(vehicle_type_map)

    df = df.dropna(subset=["vin", "model", "vehicle_type", "baseline_kmpl"])
    df = df[df["vin"] != ""]
    df = df[df["model"] != ""]
    df = df[df["vehicle_type"].isin(distance_per_vehicle_type.keys())]
    df = df.drop_duplicates(subset=["vin"])

    return df


# Main dataset function

def generate_fuel_transactions_data(logical_date, **context):
    logical_dt = datetime.strptime(logical_date, "%Y-%m-%d").date()

    year = logical_dt.year
    month = logical_dt.month
    date = logical_dt.day

    logical_start = datetime(
        year,
        month,
        date,
        0,
        0,
        0,
        tzinfo=timezone.utc
    )

    vehicle_registry = read_vehicle_registry(year, month, date)

    if vehicle_registry.empty:
        raise ValueError("No usable vehicles found after mapping vehicle_type from static file")

    vehicles = vehicle_registry.to_dict("records")
    valid_vins = vehicle_registry["vin"].tolist()

    generate_out_of_domain_txn()
    generate_out_of_domain_vins(valid_vins)
    generate_out_of_domain_fuel_liters()
    generate_out_of_domain_odo()
    generate_out_of_domain_timestamps(logical_start)

    vin_odometer = defaultdict(float)

    buffer = io.StringIO()
    buffer.write("transaction_id,vin,fuel_liters,odometer_reading,timestamp\n")

    txn_counter = 1
    num_records = 5_000

    for _ in range(num_records):
        vehicle = random.choice(vehicles)

        correct_vin = vehicle["vin"]
        model_type = vehicle["vehicle_type"]
        baseline_kmpl = vehicle["baseline_kmpl"]

        for _ in range(2):
            txn_id = bad_data(
                generate_txn_id(txn_counter),
                0.02,
                out_of_domain_txn
            )

            vin = bad_data(
                correct_vin,
                0.02,
                out_of_domain_vins
            )

            distance = generate_distance(model_type)

            vin_odometer[correct_vin] += distance
            correct_odometer = round(vin_odometer[correct_vin], 2)

            correct_fuel = generate_fuel(baseline_kmpl, distance)

            ts = generate_timestamp(logical_start)

            fuel_liters = bad_data(
                correct_fuel,
                0.05,
                out_of_domain_fuel_liters
            )

            odometer = bad_data(
                correct_odometer,
                0.05,
                out_of_domain_odo
            )

            time_stamp = bad_data(
                datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                0.05,
                out_of_domain_timestamps
            )

            buffer.write(f"{txn_id},{vin},{fuel_liters},{odometer},{time_stamp}\n")

            txn_counter += 1

    key = (
        f"{BRONZE_PREFIX}"
        f"fuel_transactions_{logical_date}.csv"
    )

    s3 = boto3.client("s3")
    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=buffer.getvalue()
    )

    logger.info(f"Fuel transactions uploaded to s3://{BUCKET}/{key}")