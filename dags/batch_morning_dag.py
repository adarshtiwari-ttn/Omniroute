from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.python import ShortCircuitOperator, PythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from Omniroute.bronze.bronze_fuel_transactions import generate_fuel_transactions_data  # type: ignore


AWS_REGION = "us-east-1"
POSTGRES_CONN = "omni_route_postgres"

BRONZE_BUCKET = "ttn-de-bootcamp-bronze-us-east-1"
SILVER_BUCKET = "ttn-de-bootcamp-silver-us-east-1"
GOLD_BUCKET = "ttn-de-bootcamp-gold-us-east-1"


BRONZE_FUEL_KEY = (
    "poc-bootcamp-group1-bronze/fuel_transactions/"
    "fuel_transactions_{{ ds }}.csv"
)

SILVER_FUEL_KEY = (
    "poc-bootcamp-group1-silver/fuel_transactions/"
    "year={{ logical_date.strftime('%Y') }}/"
    "month={{ logical_date.month }}/"
    "date={{ logical_date.day }}/"
    "*.parquet"
)

SILVER_REGISTRY_KEY = (
    "poc-bootcamp-group1-silver/vehicle_registry/"
    "year={{ logical_date.strftime('%Y') }}/"
    "month={{ logical_date.month }}/"
    "date={{ logical_date.day }}/"
    "*.parquet"
)

SILVER_MAINTENANCE_KEY = (
    "poc-bootcamp-group1-silver/maintenance_schedules/"
    "year={{ logical_date.strftime('%Y') }}/"
    "*.parquet"
)

GOLD_ASSET_HISTORY_KEY = (
    "poc-bootcamp-group1-gold/asset_history_scd2/"
    "year={{ logical_date.strftime('%Y') }}/"
    "month={{ logical_date.month }}/"
    "date={{ logical_date.day }}/"
    "*.parquet"
)


GLUE = {
    "silver_fuel_transactions": "group1_silver_fuel_transaction",
    "gold_active_fleet_snapshot": "group1_gold_active_fleet_snapshot",
    "gold_fuel_efficiency_audit": "group1_gold_fuel_efficiency_audit",
}


REPORT_SQL = {
    "active_fleet_snapshot": """
        INSERT INTO active_fleet_snapshot (
            model,
            snapshot_time,
            no_of_active_vehicles
        )
        SELECT
            model,
            snapshot_time,
            no_of_active_vehicles
        FROM staging.active_fleet_snapshot
        ON CONFLICT (model, snapshot_time) DO UPDATE SET
            no_of_active_vehicles = EXCLUDED.no_of_active_vehicles;
    """,

    "fuel_efficiency_audit": """
        DELETE FROM fuel_efficiency_audit
        WHERE audit_date = DATE '{{ ds }}';

        INSERT INTO fuel_efficiency_audit (
            vin,
            model,
            audit_date,
            km_per_liter,
            baseline_kmpl,
            status
        )
        SELECT
            vin,
            model,
            audit_date,
            km_per_liter,
            baseline_kmpl,
            status
        FROM staging.fuel_efficiency_audit
        WHERE audit_date = DATE '{{ ds }}';
    """,

    "monthly_rate_deduction": """
        TRUNCATE TABLE monthly_driver_deduction_report;

        WITH report_month AS (
            SELECT
                DATE_TRUNC('month', DATE '{{ ds }}' - INTERVAL '1 month')::date AS month_start,
                DATE_TRUNC('month', DATE '{{ ds }}')::date AS next_month_start,
                TO_CHAR(DATE '{{ ds }}' - INTERVAL '1 month', 'YYYY-MM') AS month_key
        ),

        active_driver_days AS (
            SELECT
                a.driver_id,
                MAX(a.daily_rate) AS base_rate,
                SUM(
                    GREATEST(
                        0,
                        LEAST(
                            COALESCE(a.end_date, r.next_month_start),
                            r.next_month_start
                        )
                        -
                        GREATEST(a.start_date, r.month_start)
                    )
                ) AS active_days
            FROM asset_history_scd2 a
            CROSS JOIN report_month r
            WHERE a.start_date < r.next_month_start
              AND COALESCE(a.end_date, r.next_month_start) > r.month_start
            GROUP BY a.driver_id
        )

        INSERT INTO monthly_driver_deduction_report (
            driver_id,
            total_strikes,
            total_rate_deduction,
            final_payable_daily_rate,
            status
        )
        SELECT
            d.driver_id,
            COALESCE(s.strike_count, 0) AS total_strikes,
            ROUND(
                (d.base_rate * 0.05 * COALESCE(s.strike_count, 0))::numeric,
                2
            )::double precision AS total_rate_deduction,
            ROUND(
                (
                    (d.base_rate * d.active_days)
                    - (d.base_rate * 0.05 * COALESCE(s.strike_count, 0))
                )::numeric,
                2
            )::double precision AS final_payable_daily_rate,
            COALESCE(s.status, 'ACTIVE') AS status
        FROM active_driver_days d
        LEFT JOIN driver_safety_status s
          ON d.driver_id = s.driver_id
         AND s.month = (SELECT month_key FROM report_month);
    """,

    "safety_compliance": """
        TRUNCATE TABLE safety_compliance_summary;

        WITH daily_events AS (
            SELECT
                driver_id,
                is_speed_violation,
                is_restricted_zone_breach
            FROM driver_safety_events
            WHERE DATE(event_timestamp) = DATE '{{ ds }}'
        ),

        top_drivers AS (
            SELECT driver_id
            FROM daily_events
            GROUP BY driver_id
            ORDER BY COUNT(*) DESC
            LIMIT 10
        )

        INSERT INTO safety_compliance_summary (
            total_violations_daily,
            top_ten_drivers_by_strikes,
            restricted_zone_breaches,
            speed_violations
        )
        SELECT
            COUNT(*) AS total_violations_daily,
            (SELECT COUNT(*) FROM top_drivers) AS top_ten_drivers_by_strikes,
            COALESCE(
                SUM(CASE WHEN is_restricted_zone_breach THEN 1 ELSE 0 END),
                0
            ) AS restricted_zone_breaches,
            COALESCE(
                SUM(CASE WHEN is_speed_violation THEN 1 ELSE 0 END),
                0
            ) AS speed_violations
        FROM daily_events;
    """,
}


def is_first_of_month(**context):
    return context["logical_date"].day == 1


with DAG(
    dag_id="omni_route_morning_batch_pipeline",
    description=(
        "OmniRoute morning pipeline: fuel transactions, active fleet snapshot, "
        "fuel efficiency audit, monthly deduction report, and safety compliance summary"
    ),
    schedule="0 5 * * *",
    start_date=datetime(2026, 1, 1),
    end_date = datetime(2026, 2, 3),
    catchup=True,
    max_active_runs=1,
    tags=["omni_route", "morning", "fuel", "gold", "reporting"],
) as dag:

    check_first_of_month = ShortCircuitOperator(
        task_id="check_first_of_month",
        python_callable=is_first_of_month,
        ignore_downstream_trigger_rules=False,
    )

    bronze_gen_fuel_transactions = PythonOperator(
        task_id="bronze_gen_fuel_transactions",
        python_callable=generate_fuel_transactions_data,
        op_kwargs={
            "logical_date": "{{ ds }}",
        },
    )

    sense_bronze_fuel_transactions = S3KeySensor(
        task_id="sense_bronze_fuel_transactions",
        bucket_name=BRONZE_BUCKET,
        bucket_key=BRONZE_FUEL_KEY,
        wildcard_match=True,
        aws_conn_id="aws_default",
        poke_interval=60,
        timeout=30 * 60,
        mode="reschedule",
    )

    silver_fuel_transactions = GlueJobOperator(
        task_id="silver_fuel_transactions",
        job_name=GLUE["silver_fuel_transactions"],
        region_name=AWS_REGION,
        wait_for_completion=True,
        script_args={
            "--logical_date": "{{ ds }}",
        },
    )

    sense_silver_fuel_transactions = S3KeySensor(
        task_id="sense_silver_fuel_transactions",
        bucket_name=SILVER_BUCKET,
        bucket_key=SILVER_FUEL_KEY,
        wildcard_match=True,
        aws_conn_id="aws_default",
        poke_interval=60,
        timeout=30 * 60,
        mode="reschedule",
    )

    sense_silver_vehicle_registry = S3KeySensor(
        task_id="sense_silver_vehicle_registry",
        bucket_name=SILVER_BUCKET,
        bucket_key=SILVER_REGISTRY_KEY,
        wildcard_match=True,
        aws_conn_id="aws_default",
        poke_interval=60,
        timeout=30 * 60,
        mode="reschedule",
    )

    sense_silver_maintenance_schedules = S3KeySensor(
        task_id="sense_silver_maintenance_schedules",
        bucket_name=SILVER_BUCKET,
        bucket_key=SILVER_MAINTENANCE_KEY,
        wildcard_match=True,
        aws_conn_id="aws_default",
        poke_interval=60,
        timeout=30 * 60,
        mode="reschedule",
    )

    sense_asset_history_scd2 = S3KeySensor(
        task_id="sense_asset_history_scd2",
        bucket_name=GOLD_BUCKET,
        bucket_key=GOLD_ASSET_HISTORY_KEY,
        wildcard_match=True,
        aws_conn_id="aws_default",
        poke_interval=60,
        timeout=30 * 60,
        mode="reschedule",
    )

    gold_active_fleet_snapshot = GlueJobOperator(
        task_id="gold_active_fleet_snapshot",
        job_name=GLUE["gold_active_fleet_snapshot"],
        region_name=AWS_REGION,
        wait_for_completion=True,
        script_args={
            "--logical_time": "{{ ds }} 05:00:00",
        },
    )

    gold_fuel_efficiency_audit = GlueJobOperator(
        task_id="gold_fuel_efficiency_audit",
        job_name=GLUE["gold_fuel_efficiency_audit"],
        region_name=AWS_REGION,
        wait_for_completion=True,
        script_args={
            "--logical_date": "{{ ds }}",
        },
    )

    report_active_fleet_snapshot = SQLExecuteQueryOperator(
        task_id="report_active_fleet_snapshot",
        conn_id=POSTGRES_CONN,
        sql=REPORT_SQL["active_fleet_snapshot"],
    )

    report_fuel_efficiency_audit = SQLExecuteQueryOperator(
        task_id="report_fuel_efficiency_audit",
        conn_id=POSTGRES_CONN,
        sql=REPORT_SQL["fuel_efficiency_audit"],
    )

    report_safety_compliance = SQLExecuteQueryOperator(
        task_id="report_safety_compliance",
        conn_id=POSTGRES_CONN,
        sql=REPORT_SQL["safety_compliance"],
    )

    report_monthly_rate_deduction = SQLExecuteQueryOperator(
        task_id="report_monthly_rate_deduction",
        conn_id=POSTGRES_CONN,
        sql=REPORT_SQL["monthly_rate_deduction"],
    )

    end_dag = PythonOperator(
        task_id="end_dag",
        python_callable=lambda: print("OmniRoute morning batch pipeline completed."),
    )
    bronze_gen_fuel_transactions >> sense_bronze_fuel_transactions
    sense_bronze_fuel_transactions >> silver_fuel_transactions
    silver_fuel_transactions >> sense_silver_fuel_transactions

    [
        sense_asset_history_scd2,
        sense_silver_vehicle_registry,
    ] >> gold_active_fleet_snapshot

    gold_active_fleet_snapshot >> report_active_fleet_snapshot

    [
        sense_silver_fuel_transactions,
        sense_silver_vehicle_registry,
        sense_silver_maintenance_schedules,
    ] >> gold_fuel_efficiency_audit

    gold_fuel_efficiency_audit >> report_fuel_efficiency_audit

    report_safety_compliance
    check_first_of_month >> report_monthly_rate_deduction
    
    [report_fuel_efficiency_audit, report_active_fleet_snapshot] >> end_dag