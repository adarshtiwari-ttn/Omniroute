import boto3
import random
import string
import sys
import logging
from datetime import datetime
from collections import defaultdict

# Global variables

BUCKET = "ttn-de-bootcamp-bronze-us-east-1"
DATASET_FILE = 'poc-bootcamp-group1-bronze/static/vehicles_dataset.txt'

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

def generate_out_of_domain_models():
    # Non-existent models
    out_of_domain_models = [
        "Model X1", "Model Y2", "Model Z3", "Model A4", "Model B5", "Unknown Model", "Model 123", "Model-Alpha", "Model@2020", "Model#1"
    ]
    
    return out_of_domain_models

def generate_out_of_domain_years():
    # Unrealistic manufacturing years
    out_of_domain_years = ["1899", "1025", "3000", "202"]
    
    # past years
    out_of_domain_years.extend([str(year) for year in range(2015, 2020)])
    
    # Too futuristic years
    out_of_domain_years.extend([str(year) for year in range(3026, 3031)])
    
    # Non-numeric values
    out_of_domain_years.extend(["Year2020", "Two Thousand Twenty", "20XX", "N/A", None])
    
    return out_of_domain_years

def generate_out_of_domain_fuel_types():
    
    valid_fuel_types = ["Petrol", "Diesel", "CNG", "LNG"]
    
    out_of_domain_fuel_types = defaultdict(list)
    
    for valid_fuel_type in valid_fuel_types:
        match valid_fuel_type:
            case "Petrol":
                # Ineligible fuel types
                out_of_domain_fuel_types[valid_fuel_type].extend(["EV", "Crude Oil", "Hydrogen", "Ethanol"])

                # Casing issues
                out_of_domain_fuel_types[valid_fuel_type].extend(["petrol", "PETROL"])

                # Typos
                out_of_domain_fuel_types[valid_fuel_type].extend(["Petro", "Petrrol", "Petorl"])

                # Random characters
                out_of_domain_fuel_types[valid_fuel_type].extend(["P@etrol", "Pe#trol", "Petr$ol"])

                # Different names
                out_of_domain_fuel_types[valid_fuel_type].extend(["Petroleum", "Gasoline"])

            case "Diesel":
                # Ineligible fuel types
                out_of_domain_fuel_types[valid_fuel_type].extend(["EV", "Crude Oil", "Hydrogen", "Ethanol"])

                # Casing issues
                out_of_domain_fuel_types[valid_fuel_type].extend(["diesel", "DIESEL"])

                # Typos
                out_of_domain_fuel_types[valid_fuel_type].extend(["Diesl", "Deisel", "Disel"])

                # Random characters
                out_of_domain_fuel_types[valid_fuel_type].extend(["D!esel", "Di#esel", "Dies$el"])

                # Different names
                out_of_domain_fuel_types[valid_fuel_type].extend(["Diesel fuel", "Gas oil"])

            case "CNG":
                # Ineligible fuel types
                out_of_domain_fuel_types[valid_fuel_type].extend(["EV", "Crude Oil", "Hydrogen", "Ethanol"])

                # Casing issues
                out_of_domain_fuel_types[valid_fuel_type].extend(["cng", "CNG "])

                # Typos
                out_of_domain_fuel_types[valid_fuel_type].extend(["CnG", "CGN", "CNGG"])

                # Random characters
                out_of_domain_fuel_types[valid_fuel_type].extend(["C#NG", "C@NG", "C$NG"])

                # Different names
                out_of_domain_fuel_types[valid_fuel_type].extend(["Compressed Natural Gas"])

            case "LNG":
                # Ineligible fuel types
                out_of_domain_fuel_types[valid_fuel_type].extend(["EV", "Crude Oil", "Hydrogen", "Ethanol"])

                # Casing issues
                out_of_domain_fuel_types[valid_fuel_type].extend(["lng", "LNG "])

                # Typos
                out_of_domain_fuel_types[valid_fuel_type].extend(["LnG", "LNGG", "LNG-"])

                # Random characters
                out_of_domain_fuel_types[valid_fuel_type].extend(["L#NG", "L@NG", "L$NG"])

                # Different names
                out_of_domain_fuel_types[valid_fuel_type].extend(["Liquefied Natural Gas", "Liquified Natural Gas"])

            case _:
                out_of_domain_fuel_types[valid_fuel_type].append("Unknown fuel type")
                
    return out_of_domain_fuel_types

def generate_out_of_domain_baseline_kmpl():
    
    # Negative and zero values
    out_of_domain_kmpl = [-5, 0, -10, -6597]
    
    # Unrealistically high values
    out_of_domain_kmpl.extend([4000, 650, 1200])
    
    # Non-numeric values
    out_of_domain_kmpl.extend(["Thirty", "N/A", "100kmpl", "kmpl50"])
    
    return out_of_domain_kmpl

def generate_vin():
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=8))


def  bad_data(value, prob=0.05, out_of_domain=[], args=None):
    generated_prob = random.random()

    if generated_prob < prob / 2 and out_of_domain:
        
        if args is not None:
            choices = out_of_domain.get(args, [])
            if choices:
                return random.choice(choices)
            
        return random.choice(out_of_domain)

    return value if generated_prob > prob else ""

def parse_vehicle_dataset(vehicle_dataset):
    logger.info("Parsing vehicle dataset:")
    
    vehicles = []
    
    try:
        for line in vehicle_dataset:
            parts = [x.strip() for x in line.strip().split(",")]

            if len(parts) != 5:
                continue

            model, model_type, fuel_type, mfg_year, baseline_kmpl = parts

            vehicles.append({
                "model": model,
                "fuel_type": fuel_type,
                "mfg_year": mfg_year,
                "baseline_kmpl": baseline_kmpl
            })
            
        logger.info(f"Loaded {len(vehicles)} vehicle records")

    except Exception as e:
        logger.error(f"Error loading vehicle dataset from S3: {e}")
        raise

    return vehicles

def generate_vehicle_registry_data(**context):
    
    # logical date from Airflow
    logical_date_ds = context['ds']
    
    logical_date = datetime.strptime(logical_date_ds, "%Y-%m-%d").date()
    month = logical_date.month
    date = logical_date.day
    
    logger.info(f"Generating vehicle registry data on {logical_date}:\n")
    
    n = n = 150_000 if month == 1 and date == 1 else 10_000
    
    s3 = boto3.client('s3')
    
    try:
        vehicles_dataset = s3.get_object(Bucket=BUCKET, Key=DATASET_FILE).get('Body').read().decode('utf-8').splitlines()
        logger.info("Vehicle dataset loaded successfully from S3")
    except Exception as e:
        logger.error(f"Error loading vehicle dataset from S3: {e}")
        return
    
    # Load dataset
    vehicles = parse_vehicle_dataset(vehicles_dataset)
    
    # Generate out-of-domain values
    out_of_domain_vins = generate_out_of_domain_vins()
    out_of_domain_models = generate_out_of_domain_models()
    out_of_domain_fuel_types = generate_out_of_domain_fuel_types()
    out_of_domain_years = generate_out_of_domain_years()
    out_of_domain_kmpl = generate_out_of_domain_baseline_kmpl()
    
    # Generate data locally
    data = []
    
    data.append("vin,model,mfg_year,fuel_type,baseline_kmpl\n")

    # duplicates allowed
    for _ in range(n):  # 10,000 records
        v = random.choice(vehicles)

        vin = bad_data(
            generate_vin(),
            0.05,
            out_of_domain_vins
        ) # 5% null and out of domain data
        
        model = bad_data(
            v["model"], 
            0.03,
            out_of_domain_models
        ) # 3% null
        
        mfg_year = bad_data(
            v["mfg_year"],
            0.03, 
            out_of_domain_years
        ) # 3% null and out of domain
        
        fuel_type = bad_data(
            v["fuel_type"],
            0.05,
            out_of_domain_fuel_types,
            v["fuel_type"]
        ) # 5% null and out of domain

        
        baseline_kmpl = bad_data(
            v["baseline_kmpl"],
            0.05,
            out_of_domain_kmpl
        ) # 5% null and out of domain 

        row = f"{vin},{model},{mfg_year},{fuel_type},{baseline_kmpl}\n"
        data.append(row)

    # Upload to S3
    try:
        KEY = f'poc-bootcamp-group1-bronze/vehicle_registry/vehicle_registry_{logical_date}.csv'
        
        s3.put_object(Bucket=BUCKET, Key=f"{KEY}", Body=''.join(data))
        logger.info(f"Vehicle registry data for {logical_date} uploaded to S3 successfully.")
        
    except Exception as e:
        logger.error(f"Error uploading vehicle registry data to S3: {e}")