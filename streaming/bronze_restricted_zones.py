import json
import math
import random
import boto3
import logging

# Config
BRONZE_BUCKET = "ttn-de-bootcamp-bronze-us-east-1"
RESTRICTED_ZONES_KEY = "poc-bootcamp-group1-bronze/static/restricted_zones.json"

logger = logging.getLogger(__name__)

def generate_restricted_zones_if_missing():

    s3 = boto3.client("s3")

    try:
        s3.head_object(Bucket=BRONZE_BUCKET, Key=RESTRICTED_ZONES_KEY)
        logger.info("Restricted zones file already exists. Skipping generation.")
        return
    except Exception:
        logger.error("Restricted zones file not found. Generating new one...")
        pass

    zone_name_type = [
        "High_Rish_Pass_A",
        "High_Rish_Pass_B",
        "High_Rish_Pass_C",
        "High_Rish_Pass_D",
        "High_Rish_Pass_E",
        "High_Rish_Pass_F",
        "High_Rish_Pass_G",
        "High_Rish_Pass_H",
    ]

    min_diff = math.sqrt(0.00008) / 2
    max_diff = math.sqrt(0.0057) / 2

    zones = []

    for zone_name in zone_name_type:
        lat_center = random.uniform(-80, 80)
        long_center = random.uniform(-170, 170)

        lat_half = random.uniform(min_diff, max_diff)
        long_half = random.uniform(min_diff, max_diff)

        zones.append({
            "zone_name": zone_name,
            "min_lat": round(lat_center - lat_half, 6),
            "max_lat": round(lat_center + lat_half, 6),
            "min_long": round(long_center - long_half, 6),
            "max_long": round(long_center + long_half, 6),
        })

    s3.put_object(
        Bucket=BRONZE_BUCKET,
        Key=RESTRICTED_ZONES_KEY,
        Body=json.dumps(zones, indent=2),
        ContentType="application/json",
    )
    
if __name__ == "__main__":
    generate_restricted_zones_if_missing()