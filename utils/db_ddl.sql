CREATE DATABASE report;
\c report;
-- ======================================================
-- CORE DIMENSION TABLE
-- ======================================================

CREATE TABLE asset_history_scd2 (
    vin VARCHAR(50) NOT NULL,
    driver_id VARCHAR(50) NOT NULL,
    start_date DATE NOT NULL,
    end_date DATE,
    daily_rate DOUBLE PRECISION,
    status VARCHAR(20), -- IN-TRANSIT / ARCHIVED
    region VARCHAR(100),
    PRIMARY KEY (vin, start_date)
);

-- ======================================================
-- AUDIT & SUMMARY TABLES (SNAPSHOTS)
-- ======================================================

CREATE TABLE fuel_efficiency_audit (
    vin VARCHAR(50) NOT NULL,
    model VARCHAR(100),
    audit_date DATE NOT NULL,
    km_per_liter DOUBLE PRECISION,
    baseline_kmpl DOUBLE PRECISION,
    status VARCHAR(20), -- OK / FLAGGED
    PRIMARY KEY (vin, audit_date)
);

CREATE TABLE safety_compliance_summary (
    -- Snapshot: Overwritten daily. No PK needed.
    total_violations_daily INTEGER,
    top_ten_drivers_by_strikes INTEGER,
    restricted_zone_breaches INTEGER,
    speed_violations INTEGER
);

CREATE TABLE active_fleet_snapshot (
    model VARCHAR(100) NOT NULL,
    snapshot_time TIMESTAMP NOT NULL,
    no_of_active_vehicles INTEGER,
    PRIMARY KEY (model, snapshot_time)
);

-- ======================================================
-- MONTHLY REPORTING TABLES
-- ======================================================

CREATE TABLE driver_safety_status (
    -- Historical: Appended monthly
    driver_id VARCHAR(50) NOT NULL,
    month VARCHAR(10) NOT NULL, -- e.g., '2026-05'
    zone_sk VARCHAR(50),
    base_rate DOUBLE PRECISION,
    strike_count INTEGER,
    current_adjusted_rate DOUBLE PRECISION,
    status VARCHAR(20), -- ACTIVE / SUSPENDED
    PRIMARY KEY (driver_id, month)
);

CREATE TABLE monthly_driver_deduction_report (
    -- Snapshot: Overwritten monthly
    driver_id VARCHAR(50) PRIMARY KEY,
    total_strikes INTEGER,
    total_rate_deduction DOUBLE PRECISION,
    final_payable_daily_rate DOUBLE PRECISION,
    status VARCHAR(20) -- Active / Suspended
);

-- ======================================================
-- STREAMING FACT TABLE (APPENDED)
-- ======================================================

CREATE TABLE driver_safety_events (
    driver_id VARCHAR(50),
    vin VARCHAR(50) NOT NULL,
    event_timestamp TIMESTAMP NOT NULL,
    zone_sk VARCHAR(50),
    speed INTEGER,
    lat DOUBLE PRECISION,
    long DOUBLE PRECISION,
    is_speed_violation BOOLEAN,
    is_restricted_zone_breach BOOLEAN,
    PRIMARY KEY (vin, event_timestamp)
);

-- ======================================================
-- LOGICAL RELATIONSHIPS (COMMENTED OUT)
-- ======================================================
/* 
Note: These are commented out because Postgres requires FKs to reference 
a UNIQUE column or a full PRIMARY KEY. Since asset_history_scd2 tracks 
history, 'driver_id' and 'vin' are not unique on their own.
*/
-- ALTER TABLE driver_safety_status 
--     ADD CONSTRAINT fk_dss_driver FOREIGN KEY (driver_id) REFERENCES asset_history_scd2(driver_id);

-- ALTER TABLE monthly_driver_deduction_report 
--     ADD CONSTRAINT fk_mddr_driver FOREIGN KEY (driver_id) REFERENCES asset_history_scd2(driver_id);

-- ALTER TABLE fuel_efficiency_audit 
--     ADD CONSTRAINT fk_fea_vin FOREIGN KEY (vin) REFERENCES asset_history_scd2(vin);

-- ALTER TABLE driver_safety_events 
--     ADD CONSTRAINT fk_dse_driver FOREIGN KEY (driver_id) REFERENCES asset_history_scd2(driver_id);

-- ALTER TABLE driver_safety_events 
--     ADD CONSTRAINT fk_dse_vin FOREIGN KEY (vin) REFERENCES asset_history_scd2(vin);


-- 1. Grant full access to the database itself (Replace 'your_database_name' with your actual DB name)
GRANT ALL PRIVILEGES ON DATABASE report TO postgres;

-- 2. Grant full access to all existing tables in the public schema
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO postgres;

-- 3. Ensure the postgres user automatically gets access to any tables created in the future
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL PRIVILEGES ON TABLES TO postgres;


-- Staging Tables

CREATE TABLE staging.asset_history_scd2
(LIKE public.asset_history_scd2 INCLUDING ALL);

CREATE TABLE staging.active_fleet_snapshot
(LIKE public.active_fleet_snapshot INCLUDING ALL);

CREATE TABLE staging.fuel_efficiency_audit
(LIKE public.fuel_efficiency_audit INCLUDING ALL);

CREATE TABLE staging.driver_safety_status
(LIKE public.driver_safety_status INCLUDING ALL);

CREATE TABLE staging.safety_compliance_summary
(LIKE public.safety_compliance_summary INCLUDING ALL);

CREATE TABLE staging.monthly_driver_deduction_report
(LIKE public.monthly_driver_deduction_report INCLUDING ALL);

-- Initial data load
INSERT INTO staging.asset_history_scd2
SELECT * FROM public.asset_history_scd2;

INSERT INTO staging.active_fleet_snapshot
SELECT * FROM public.active_fleet_snapshot;

INSERT INTO staging.fuel_efficiency_audit
SELECT * FROM public.fuel_efficiency_audit;

INSERT INTO staging.driver_safety_status
SELECT * FROM public.driver_safety_status;

INSERT INTO staging.safety_compliance_summary
SELECT * FROM public.safety_compliance_summary;

INSERT INTO staging.monthly_driver_deduction_report
SELECT * FROM public.monthly_driver_deduction_report;

GRANT USAGE ON SCHEMA staging TO postgres;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA staging TO postgres;

ALTER DEFAULT PRIVILEGES IN SCHEMA staging
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO postgres;