import boto3
import random
import string
import logging
import io
import awswrangler as wr
import pandas as pd
from datetime import datetime, timedelta
from collections import defaultdict

# Global variables
BUCKET = 'ttn-de-bootcamp-bronze-us-east-1'
DATASET_BUCKET = 'ttn-de-bootcamp-silver-us-east-1'

logging.basicConfig(
    level= logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)


service_type = {
    "Engine overhaul": 5, # major repair/rebuild of engine
    "Oil change": 1, # replacing engine oil and filter
    "Tire rotation": 1, # shifting tires for even wear
    "Wheel alignment": 1, # correcting wheel angles
    "Brake service": 2, # inspection and replacement of brake parts
    "Battery service": 1, # testing or replacing battery
    "Air filter replacement": 1, # cleaning/replacing air filter
    "Coolant service": 2, # flushing and refilling coolant
    "Transmission service": 1, # fluid change and inspection
    "Suspension service": 3, # checking shocks and struts
    "AC service": 2, # maintenance of air conditioning system
    "Fuel system cleaning": 2, # cleaning injectors and fuel lines
    "General inspection": 1, # full vehicle health check
    "Electrical system repair": 3 # fixing wiring, lights, etc.
}


# Out-of-domain values
out_of_domain_vins = []
out_of_domain_service_dates = []
out_of_domain_service_types = defaultdict(list)


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


def generate_out_of_domain_service_dates():
    
    # Unrealistic past date
    out_of_domain_service_dates.append("2018-05-10")
    
    # Unrealistic future date
    out_of_domain_service_dates.append("9999-01-01")
    
    # Invalid date format
    out_of_domain_service_dates.append("invalid-date")
    
    # Date with wrong format
    out_of_domain_service_dates.append("20261231")
    
    # Date with different format
    out_of_domain_service_dates.append("31-12-2026")
    out_of_domain_service_dates.append("12/31/2026")
    out_of_domain_service_dates.append("2026/12/31")
    

    return out_of_domain_service_dates

def generate_out_of_domain_service_types(service_type_list):
    for service in service_type_list:
        # Casing issues
        out_of_domain_service_types[service].extend(["Engine wash", "Painting", "Upgrade", "Cleaning"])
        match service:
            case "Engine overhaul":
                # Casing issues
                out_of_domain_service_types[service].extend(["engine overhaul", "ENGINE OVERHAUL"])
                
                # Typos
                out_of_domain_service_types[service].extend(["Engne overhaul", "Engin overhaul", "Engine overhal"])
            
                # Random characters in between
                out_of_domain_service_types[service].extend(["Eng@ine overhaul", "Eng#ine overhaul", "Eng$ine overhaul"])
                
                # Abbrevations/Different Names
                out_of_domain_service_types[service].extend(["Engine rebuild", "Major engine repair"])
            case "Oil change":
                # Casing issues
                out_of_domain_service_types[service].extend(["oil change", "OIL CHANGE"])
                
                # Typos
                out_of_domain_service_types[service].extend(["Oill change", "Oil chage", "Oli change"])
                
                # Random characters in between  service
                out_of_domain_service_types[service].extend(["Oil @change", "Oil $change", "O!il change"])
                
                # Abbreviations/Different Names
                out_of_domain_service_types[service].extend(["Engine oil replacement", "Oil filter change"])

            case "Tire rotation":
                # Casing issues
                out_of_domain_service_types[service].extend(["tire rotation", "TIRE ROTATION"])
                
                # Typos
                out_of_domain_service_types[service].extend(["Tire roation", "Tir rotation", "Tire rotatoin"])
                
                # Random characters in between  service
                out_of_domain_service_types[service].extend(["Tire @rotation", "Tire #rotation", "Tire rot@tion"])
                
                # Abbreviations/Different Names
                out_of_domain_service_types[service].extend(["Tire swapping", "Tire exchange"])

            case "Wheel alignment":
                # Casing issues
                out_of_domain_service_types[service].extend(["wheel alignment", "WHEEL ALIGNMENT"])
                
                # Typos
                out_of_domain_service_types[service].extend(["Wheel alingment", "Whel alignment", "Wheel aligment"])
                
                # Random characters in between  service
                out_of_domain_service_types[service].extend(["Wheel @alignment", "Wheel #alignment", "Wheel align@ment"])
                
                # Abbreviations/Different Names
                out_of_domain_service_types[service].extend(["Wheel balancing", "Suspension alignment"])

            case "Brake service":
                # Casing issues
                out_of_domain_service_types[service].extend(["brake service", "BRAKE SERVICE"])
                
                # Typos
                out_of_domain_service_types[service].extend(["Brakes service", "Brake servcie", "Brak service"])
                
                # Random characters in between  service
                out_of_domain_service_types[service].extend(["Brake $service", "Brake @service", "Br@ke service"])
                
                # Abbreviations/Different Names
                out_of_domain_service_types[service].extend(["Brake repair", "Brake pad replacement"])

            case "Battery service":
                # Casing issues
                out_of_domain_service_types[service].extend(["battery service", "BATTERY SERVICE"])
                
                # Typos
                out_of_domain_service_types[service].extend(["Batteri service", "Battery servce", "Battri service"])
                
                # Random characters in between  service
                out_of_domain_service_types[service].extend(["Battery #service", "Battery @service", "B@ttery service"])
                
                # Abbreviations/Different Names
                out_of_domain_service_types[service].extend(["Battery replacement", "Car battery check"])

            case "Air filter replacement":
                # Casing issues
                out_of_domain_service_types[service].extend(["air filter replacement", "AIR FILTER REPLACEMENT"])
                
                # Typos
                out_of_domain_service_types[service].extend(["Air fliter replacement", "Air filtter replacement", "Air filter replacemnt"])
                
                # Random characters in between  service
                out_of_domain_service_types[service].extend(["Air fil@ter replacement", "Air filt#er replacement", "Air filt$er replacement"])
                
                # Abbreviations/Different Names
                out_of_domain_service_types[service].extend(["Air filter change", "Cabin filter replacement"])

            case "Coolant service":
                # Casing issues
                out_of_domain_service_types[service].extend(["coolant service", "COOLANT SERVICE"])
                
                # Typos
                out_of_domain_service_types[service].extend(["Coolant sevce", "Coolnt service", "Coolent service"])
                
                # Random characters in between service
                out_of_domain_service_types[service].extend(["Coolant $service", "Cool@nt service", "Coo!ant service"])
                
                # Abbreviations/Different Names
                out_of_domain_service_types[service].extend(["Radiator service", "Coolant flush"])

            case "Transmission service":
                # Casing issues
                out_of_domain_service_types[service].extend(["transmission service", "TRANSMISSION SERVICE"])
                
                # Typos
                out_of_domain_service_types[service].extend(["Transmision service", "Transmision service", "Transmition service"])
                
                # Random characters in between service
                out_of_domain_service_types[service].extend(["Transmiss$ion service", "Transm!ssion service", "Transmis@sion service"])
                
                # Abbreviations/Different Names
                out_of_domain_service_types[service].extend(["Gearbox service", "Transmission fluid change"])

            case "Suspension service":
                # Casing issues
                out_of_domain_service_types[service].extend(["suspension service", "SUSPENSION SERVICE"])
                
                # Typos
                out_of_domain_service_types[service].extend(["Suspenstion service", "Suspenssion service", "Suspention service"])
                
                # Random characters in between  service
                out_of_domain_service_types[service].extend(["Suspens$ion service", "Suspens!ion service", "Suspens@ion service"])
                
                # Abbreviations/Different Names
                out_of_domain_service_types[service].extend(["Shock absorber service", "Suspension repair"])

            case "AC service":
                # Casing issues
                out_of_domain_service_types[service].extend(["ac service", "AC SERVICE"])
                
                # Typos
                out_of_domain_service_types[service].extend(["A/C service", "AC servcie", "A/C servcie"])
                
                # Random characters in between  service
                out_of_domain_service_types[service].extend(["AC #service", "A/C @service", "AC $service"])
                
                # Abbreviations/Different Names
                out_of_domain_service_types[service].extend(["Air conditioner check", "AC maintenance"])

            case "Fuel system cleaning":
                # Casing issues
                out_of_domain_service_types[service].extend(["fuel system cleaning", "FUEL SYSTEM CLEANING"])
                
                # Typos
                out_of_domain_service_types[service].extend(["Fuel sytsem cleaning", "Fuel system cleanig", "Fuel system clening"])
                
                # Random characters in between  service
                out_of_domain_service_types[service].extend(["Fuel sys@tem cleaning", "Fuel syst#m cleaning", "F!el system cleaning"])
                
                # Abbreviations/Different Names
                out_of_domain_service_types[service].extend(["Fuel injector cleaning", "Fuel line cleaning"])

            case "General inspection":
                # Casing issues
                out_of_domain_service_types[service].extend(["general inspection", "GENERAL INSPECTION"])
                
                # Typos
                out_of_domain_service_types[service].extend(["Generl inspection", "Genral inspection", "General inspecion"])
                
                # Random characters in between  service
                out_of_domain_service_types[service].extend(["General @inspection", "Gen@ral inspection", "Gen!ral inspection"])
                
                # Abbreviations/Different Names
                out_of_domain_service_types[service].extend(["Full vehicle inspection", "Routine check-up"])

            case "Electrical system repair":
                # Casing issues
                out_of_domain_service_types[service].extend(["electrical system repair", "ELECTRICAL SYSTEM REPAIR"])
                
                # Typos
                out_of_domain_service_types[service].extend(["Electrcial system repair", "Electricl system repair", "Electrical system repir"])
                
                # Random characters in between  service
                out_of_domain_service_types[service].extend(["Electrical $system repair", "Electri#cal system repair", "Electri!cal system repair"])
                
                # Abbreviations/Different Names
                out_of_domain_service_types[service].extend(["Wiring repair", "Electrical diagnostics"])

            case _:
                out_of_domain_service_types[service].append("Unknown service type")
    
    return out_of_domain_service_types

def generate_service_date(year):
    start_date = datetime(year, 1, 1)
    end_date = datetime(year, 12, 31)

    random_days = random.randint(0, (end_date - start_date).days)
    service_date = start_date + timedelta(days=random_days)

    return service_date
            
def  bad_data(value, prob=0.05, out_of_domain=[], args=None):
    generated_prob = random.random()

    if generated_prob < prob / 2 and out_of_domain:
        
        if args is not None:
            choices = out_of_domain.get(args, [])
            if choices:
                return random.choice(choices)
            
        return random.choice(out_of_domain)

    return value if generated_prob > prob else ""


# dataset

def read_parquet_vins_from_path(path: str, required: bool = False):
    try:
        files = [
            p for p in wr.s3.list_objects(path)
            if p.endswith(".parquet")
        ]

        if not files:
            msg = f"No parquet files found at {path}"
            if required:
                raise ValueError(msg)

            logger.info(msg)
            return []

        df = wr.s3.read_parquet(
            path=files,
            columns=["vin"],
            dataset=False
        )

        if df.empty or "vin" not in df.columns:
            msg = f"No VIN data found at {path}"
            if required:
                raise ValueError(msg)

            logger.info(msg)
            return []

        return (
            df["vin"]
            .dropna()
            .astype(str)
            .str.strip()
            .loc[lambda s: s != ""]
            .drop_duplicates()
            .tolist()
        )

    except Exception as e:
        if required:
            raise

        logger.warning(f"Skipping optional path {path}: {e}")
        return []


def read_vehicle_registry_vins(year, month, date):
    cur_path = (
        f"s3://{DATASET_BUCKET}/"
        f"poc-bootcamp-group1-silver/vehicle_registry/"
        f"year={year}/month={month}/date={date}/"
    )

    prev_path = (
        f"s3://{DATASET_BUCKET}/"
        f"poc-bootcamp-group1-silver/vehicle_registry/"
        f"year={year - 1}/"
    )

    current_vins = read_parquet_vins_from_path(
        path=cur_path,
        required=True
    )

    previous_vins = read_parquet_vins_from_path(
        path=prev_path,
        required=False
    )

    vins = list(set(current_vins + previous_vins))

    logger.info(f"Current day VINs: {len(current_vins):,}")
    logger.info(f"Previous year VINs: {len(previous_vins):,}")
    logger.info(f"Total VINs available: {len(vins):,}")

    return vins


def generate_maintenance_schedules_data(**context):
    logical_date_ds = context["ds"]

    logical_date = datetime.strptime(logical_date_ds, "%Y-%m-%d").date()
    year = logical_date.year
    month = logical_date.month
    date = logical_date.day

    logger.info(f"Generating maintenance schedules data for {logical_date}")

    s3 = boto3.client("s3")

    vehicles_vin = read_vehicle_registry_vins(
        year=year,
        month=month,
        date=date
    )

    if not vehicles_vin:
        raise ValueError("No valid VINs found in vehicle registry.")

    out_of_domain_vins = generate_out_of_domain_vins()
    out_of_domain_service_dates = generate_out_of_domain_service_dates()
    service_types_list = list(service_type.keys())
    out_of_domain_service_types = generate_out_of_domain_service_types(service_types_list)

    data = ["vin,service_date,service_type\n"]

    n = 150_000
    
    for _ in range(n):
        correct_service_type = random.choice(service_types_list)
        correct_service_date = generate_service_date(year)

        for i in range(service_type[correct_service_type]):
            vin = bad_data(
                random.choice(vehicles_vin),
                0.02,
                out_of_domain_vins
            )

            service_date = bad_data(
                (correct_service_date + timedelta(days=i)).strftime("%Y-%m-%d"),
                0.03,
                out_of_domain_service_dates
            )

            service = bad_data(
                correct_service_type,
                0.03,
                out_of_domain_service_types,
                correct_service_type
            )

            data.append(f"{vin},{service_date},{service}\n")

    try:
        key = f"poc-bootcamp-group1-bronze/maintenance_schedules/maintenance_schedules_{year}.csv"

        s3.put_object(
            Bucket=BUCKET,
            Key=key,
            Body="".join(data)
        )

        logger.info(f"Maintenance schedules data for {year} uploaded to s3://{BUCKET}/{key}")

    except Exception as e:
        logger.error(f"Error uploading maintenance schedules data to S3: {e}")
        raise