from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.python import ShortCircuitOperator, PythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor 
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from Omniroute.bronze.bronze_vehicle_registry import generate_vehicle_registry_data # type: ignore
from Omniroute.bronze.bronze_vehicle_assignment import generate_vehicle_assignment_data # type: ignore
from Omniroute.bronze.bronze_maintenance_schedules import generate_maintenance_schedules_data # type: ignore

# CONSTANTS

AWS_REGION    = "us-east-1"
POSTGRES_CONN = "omni_route_postgres"
BRONZE_BUCKET = "ttn-de-bootcamp-bronze-us-east-1"

# Bronze S3 Prefixes
BRONZE_PREFIXES = {
    "vehicle_registry"      : "poc-bootcamp-group1-bronze/vehicle_registry/*",
    "vehicle_assignment"    : "poc-bootcamp-group1-bronze/vehicle_assignment/",
    "maintenance_schedules" : "poc-bootcamp-group1-bronze/maintenance_schedules/",
}

# Glue Job Names
GLUE = {
    # Bronze Generators
    "bronze_gen_vehicle_registry"      : generate_vehicle_registry_data,
    "bronze_gen_vehicle_assignment"    : generate_vehicle_assignment_data,
    "bronze_gen_maintenance_schedules" : generate_maintenance_schedules_data,

    # Silver
    "silver_vehicle_registry"          : "group1_silver_vehicle_registry",
    "silver_vehicle_assignment"        : "group1_silver_vehicle_assignment",
    "silver_maintenance_schedules"     : "group1_silver_maintenance_logs",

    # Gold
    "gold_asset_history_scd2"          : "group1_gold_asset_history_scd2",
    "gold_active_fleet_snapshot"       : "group1_gold_active_fleet_snapshot",
}

# Reporting SQL
REPORT_SQL = {
    "fleet_assignment_history": """
    
        INSERT INTO asset_history_scd2 (vin, driver_id, start_date, end_date, daily_rate, status, region)
        SELECT vin, driver_id, start_date, end_date, daily_rate, status, region 
        FROM staging.asset_history_scd2
        ON CONFLICT (vin, start_date) DO UPDATE SET
            end_date   = EXCLUDED.end_date,
            daily_rate = EXCLUDED.daily_rate,
            status     = EXCLUDED.status,
            region     = EXCLUDED.region;
    """
}


# SHORT CIRCUIT CHECKS

def is_jan_first(**context):
    return (
        context["logical_date"].month == 1 and
        context["logical_date"].day == 1
    )


# DAG

with DAG(
    dag_id="omni_route_midnight_batch_pipeline",
    description=(
        "OmniRoute Smart Logistics Engine"
        "Full batch pipeline: bronze generation -> silver -> gold -> reporting"
    ),
    schedule="0 0 * * *",
    start_date=datetime(2026, 1, 1),
    end_date = datetime(2026, 2, 3),
    catchup=True,
    max_active_runs=1,
    tags=["omni_route", "batch", "bronze", "silver", "gold", "reporting"],
) as dag:

    
    # # SHORT CIRCUIT GATES
    
    check_jan_first = ShortCircuitOperator(
        task_id="check_jan_first",
        python_callable=is_jan_first,
        ignore_downstream_trigger_rules=False,
    )

    
    # # BRONZE GENERATION
    
    bronze_gen_vehicle_registry = PythonOperator(
        task_id="bronze_gen_vehicle_registry",
        python_callable = generate_vehicle_registry_data,
        op_kwargs={
          "logical_date" : "{{ds}}",
        }
    )

    bronze_gen_vehicle_assignment = PythonOperator(
        task_id="bronze_gen_vehicle_assignment",
        python_callable= generate_vehicle_assignment_data,
        op_kwargs={
          "logical_date" : "{{ds}}",
        }
    )

    bronze_gen_maintenance_schedules = PythonOperator(
        task_id="bronze_gen_maintenance_schedules",
        python_callable= generate_maintenance_schedules_data,
        op_kwargs={
          "logical_date" : "{{ds}}"  
        }
    )

    
    # # S3 KEY SENSORS
    # # Wait for bronze files to land before
    # # running silver jobs
    # # poke_interval=60  — check every 60 seconds
    # # timeout=3600      — fail after 1 hour
    
    sense_vehicle_registry = S3KeySensor(
        task_id="sense_vehicle_registry",
        bucket_name=BRONZE_BUCKET,
        bucket_key=f"{BRONZE_PREFIXES["vehicle_registry"]}" + "vehicle_registry_{{ ds }}.csv",
        wildcard_match=True,
        poke_interval=60,
        timeout=3600,
        mode="reschedule",   # release worker slot while waiting
    )

    sense_vehicle_assignment = S3KeySensor(
        task_id="sense_vehicle_assignment",
        bucket_name=BRONZE_BUCKET,
        bucket_key=BRONZE_PREFIXES["vehicle_assignment"] + "vehicle_assignment_{{ ds }}.csv",
        wildcard_match=True,
        poke_interval=60,
        timeout=3600,
        mode="reschedule",
    )

    sense_maintenance_schedules = S3KeySensor(
        task_id="sense_maintenance_schedules",
        bucket_name=BRONZE_BUCKET,
        bucket_key=BRONZE_PREFIXES["maintenance_schedules"] + "maintenance_schedules_{{ logical_date.strftime('%Y') }}.csv",
        wildcard_match=True,
        
        poke_interval=60,
        timeout=3600,
        mode="reschedule",
    )

    # # SILVER JOBS
    
    silver_vehicle_registry = GlueJobOperator(
        task_id="silver_vehicle_registry",
        job_name=GLUE["silver_vehicle_registry"],
        region_name=AWS_REGION,
        wait_for_completion=True,
        script_args = {
            '--logical_date': "{{ ds }}",
        }
    )

    silver_vehicle_assignment = GlueJobOperator(
        task_id="silver_vehicle_assignment",
        job_name=GLUE["silver_vehicle_assignment"],
        region_name=AWS_REGION,
        wait_for_completion=True,
        script_args = {
            '--logical_date': "{{ ds }}",
        }
    )

    silver_maintenance_schedules = GlueJobOperator(
        task_id="silver_maintenance_schedules",
        job_name=GLUE["silver_maintenance_schedules"],
        region_name=AWS_REGION,
        wait_for_completion=True,
        script_args = {
            '--logical_date': "{{ ds }}",
        }
    )
    
    # # GOLD JOBS
    
    gold_asset_history_scd2 = GlueJobOperator(
        task_id="gold_asset_history_scd2",
        job_name=GLUE["gold_asset_history_scd2"],
        region_name=AWS_REGION,
        wait_for_completion=True,
        script_args = {
            "--logical_date": "{{ ds }}"
        }
    )
    
    # # REPORTING
    
    report_fleet_assignment_history = SQLExecuteQueryOperator(
        task_id="report_fleet_assignment_history",
        conn_id=POSTGRES_CONN,
        sql=REPORT_SQL["fleet_assignment_history"],
    )

    # End
    end_dag_1 = PythonOperator(
        task_id="end_dag_1",
        python_callable=lambda: print("OmniRoute midnight batch pipeline complete!"),
        trigger_rule="one_success",
    )
    
    
    # DEPENDENCIES
    
    # Bronze
    bronze_gen_vehicle_registry >> sense_vehicle_registry
    bronze_gen_vehicle_assignment >> sense_vehicle_assignment

    # # Silver
    sense_vehicle_registry >> silver_vehicle_registry
    silver_vehicle_registry >> bronze_gen_vehicle_assignment
    sense_vehicle_assignment    >> silver_vehicle_assignment

    check_jan_first >> bronze_gen_maintenance_schedules
    bronze_gen_maintenance_schedules >> sense_maintenance_schedules
    sense_maintenance_schedules >> silver_maintenance_schedules
    
    # # Gold
    
    silver_vehicle_assignment >> [check_jan_first, gold_asset_history_scd2]
    
    [report_fleet_assignment_history, silver_maintenance_schedules ] >> end_dag_1
    
    gold_asset_history_scd2    >> report_fleet_assignment_history