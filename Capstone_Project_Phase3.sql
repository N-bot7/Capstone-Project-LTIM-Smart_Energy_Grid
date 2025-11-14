//Phase 3

-- Creating a stream on the staging table for substation load data
CREATE OR REPLACE STREAM stg_substation_load_stream 
  ON TABLE stg_substation_load ;

select * from stg_substation_load_stream;
  
--creating 2nd stream on stg_substaion_load
CREATE OR REPLACE STREAM  stg_substation_load_log ON TABLE stg_substation_load;
  
select * from stg_substation_load_log;

-- Creating a stream on the staging table for smart meter readings
CREATE OR REPLACE STREAM stg_smartmeter_readings_stream 
  ON TABLE smartmeter_readings ;

select * from stg_smartmeter_readings_stream;

--craeting 2nd stream on smartmeter_readings
CREATE OR REPLACE STREAM stg_smartmeter_readings_stream_log ON TABLE smartmeter_readings;

select * from stg_smartmeter_readings_stream_log;

--Inserting new values into stg_substation_load table
INSERT INTO stg_substation_load (REGIONID, TIMESTAMP, LOADMW, UTILIZATIONPCT, TRANSFORMERCOUNT, LOSSESPCT)
VALUES
(10045, '2023-11-01 02:30:00', 275.6, 81.2, 6, 5.0),
(10050, '2023-11-01 02:45:00', 260.1, 76.8, 5, 4.6),
(10055, '2023-11-01 03:00:00', 245.4, 73.1, 4, 5.4),
(10060, '2023-11-01 03:15:00', 285.8, 83.0, 6, 5.3),
(10065, '2023-11-01 03:30:00', 295.2, 84.7, 7, 4.9);

INSERT INTO stg_substation_load (REGIONID, TIMESTAMP, LOADMW, UTILIZATIONPCT, TRANSFORMERCOUNT, LOSSESPCT)
VALUES(10075, '2023-11-01 04:30:00', 255.2, 74.7, 5, 5.9);

-- checking the stream to check if it has captured the new changes
select * from stg_substation_load_stream;

-- inserting values into smartmeter readings table
INSERT INTO smartmeter_readings (CUSTOMERID, REGIONID, TIMESTAMP, KWH, AVGKW)
VALUES 
(400001, 10005, '2023-11-01 00:15:00', 100.3, 4.2),
(400002, 10010, '2023-11-01 00:30:00', 150.7, 5.0),
(400003, 10012, '2023-11-01 00:45:00', 180.8, 6.0),
(400004, 10015, '2023-11-01 01:00:00', 200.1, 6.2),
(400005, 10020, '2023-11-01 01:15:00', 130.4, 4.8);
INSERT INTO smartmeter_readings (CUSTOMERID, REGIONID, TIMESTAMP, KWH, AVGKW)
VALUES (400007, 10044, '2023-11-06 01:45:00', 1600.4, 5.8);

-- checking the stream to check if it has captured the new changes
select * from stg_smartmeter_readings_stream;

-- creating audit log table
CREATE OR REPLACE TABLE audit_log (
    audit_id INTEGER AUTOINCREMENT PRIMARY KEY,
    source_table STRING,
    action STRING,
    is_update BOOLEAN,
    region_id STRING,
    record_timestamp TIMESTAMP,
    log_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


 -- creating TASK for Upserting INTO FACT_CONSUMPTION FROM smartmeter_readings_stream Inserts NEW rows and UPDATES changed rows
CREATE OR REPLACE TASK task_upsert_fact_consumption
WAREHOUSE = CAPSTONE
SCHEDULE = 'USING CRON 0 * * * * Asia/Kolkata'      
AS
MERGE INTO FACT_CONSUMPTION AS T
USING (
    SELECT
        CUSTOMERID,
        REGIONID,
        TIMESTAMP,
        KWH,
        AVGKW,
        METADATA$ISUPDATE,
        METADATA$ACTION
    FROM stg_smartmeter_readings_stream
) S
ON T.CUSTOMER_ID = S.CUSTOMERID
AND T.TIMESTAMP = S.TIMESTAMP

WHEN MATCHED AND S.METADATA$ISUPDATE = TRUE THEN
    UPDATE SET
        T.DATE_KEY = DATE(S.TIMESTAMP),
        T.CONSUMPTION_KWH = S.KWH,
        T.AVERAGE_KW = S.AVGKW

WHEN NOT MATCHED AND S.METADATA$ACTION = 'INSERT' THEN
    INSERT (CUSTOMER_ID, REGION_ID, TIMESTAMP, CONSUMPTION_KWH, AVERAGE_KW, DATE_KEY)
    VALUES (S.CUSTOMERID, S.REGIONID, S.TIMESTAMP, S.KWH, S.AVGKW, DATE(S.TIMESTAMP));


CREATE OR REPLACE TASK task_upsert_fact_consumption
WAREHOUSE = CAPSTONE
SCHEDULE = '1 minute'
-- SCHEDULE = 'USING CRON 0 * * * * Asia/Kolkata'
AS
MERGE INTO FACT_CONSUMPTION AS T
USING (
    SELECT
        CUSTOMERID,
        REGIONID,
        TIMESTAMP,
        KWH,
        AVGKW,
        METADATA$ISUPDATE,
        METADATA$ACTION
    FROM stg_smartmeter_readings_stream
WHERE METADATA$ACTION IN ('INSERT', 'UPDATE')
) S
ON T.CUSTOMER_ID = S.CUSTOMERID
AND T.TIMESTAMP = S.TIMESTAMP
 
WHEN MATCHED AND S.METADATA$ISUPDATE = TRUE AND S.METADATA$ACTION = 'UPDATE' THEN
    UPDATE SET
        T.DATE_KEY = CASE WHEN DATE(S.TIMESTAMP)  IS DISTINCT FROM T.DATE_KEY  THEN DATE(S.TIMESTAMP)  ELSE T.DATE_KEY END,
        T.CONSUMPTION_KWH = CASE WHEN S.KWH  IS DISTINCT FROM T.CONSUMPTION_KWH  THEN S.KWH  ELSE T.CONSUMPTION_KWH END,
        T.AVERAGE_KW = CASE WHEN S.AVGKW  IS DISTINCT FROM T.AVERAGE_KW  THEN S.AVGKW  ELSE T.AVERAGE_KW END
 
WHEN NOT MATCHED AND S.METADATA$ACTION = 'INSERT' THEN
    INSERT (CUSTOMER_ID, REGION_ID, TIMESTAMP, CONSUMPTION_KWH, AVERAGE_KW, DATE_KEY)
    VALUES (S.CUSTOMERID, S.REGIONID, S.TIMESTAMP, S.KWH, S.AVGKW, DATE(S.TIMESTAMP)); 

alter task task_upsert_fact_consumption resume;
alter task task_upsert_fact_consumption SUSPEND;
select * from stg_smartmeter_readings_stream;
select * from fact_consumption;


 -- creating TASK for UPSERTING INTO FACT_LOAD FROM stg_substation_load_stream
CREATE OR REPLACE TASK task_upsert_fact_load
WAREHOUSE = Capstone
--SCHEDULE = 'USING CRON 5 * * * * Asia/Kolkata'   -- runs 5 min after Task-1 (chain effect)
SCHEDULE='1 minute'
AS
MERGE INTO FACT_LOAD AS T
USING (
    SELECT
        RegionID,
        Timestamp,
        LoadMW,
        UtilizationPct,
        TransformerCount,
        LossesPct,
        METADATA$ISUPDATE,
        METADATA$ACTION
    FROM stg_substation_load_stream
) S
ON T.REGION_ID = S.RegionID
AND T.TIMESTAMP = S.Timestamp

WHEN MATCHED AND S.METADATA$ISUPDATE = TRUE THEN
    UPDATE SET
        T.DATE_KEY = DATE(S.TIMESTAMP),
        T.LOAD_MW = S.LoadMW,
        T.UTILIZATION_PCT = S.UtilizationPct,
        T.TRANSFORMER_COUNT = S.TransformerCount,
        T.LOSSES_PCT = S.LossesPct

WHEN NOT MATCHED AND S.METADATA$ACTION = 'INSERT' THEN
    INSERT (REGION_ID, TIMESTAMP, LOAD_MW, UTILIZATION_PCT, TRANSFORMER_COUNT, LOSSES_PCT, DATE_KEY)
    VALUES (S.RegionID, S.Timestamp, S.LoadMW, S.UtilizationPct, S.TransformerCount, S.LossesPct, DATE(S.TIMESTAMP));

select * from fact_load;
select * from stg_substation_load_stream;
alter task task_upsert_fact_load SUSPEND;
alter task task_upsert_fact_load resume;

 -- creating TASK for Logging STREAM CHANGES INTO audit_log From BOTH streams
CREATE OR REPLACE TASK task_log_stream_changes
WAREHOUSE = Capstone
--SCHEDULE = 'USING CRON 10 * * * * Asia/Kolkata'
SCHEDULE='1 minute'
AS
INSERT INTO audit_log (source_table, action, is_update, region_id, record_timestamp)
SELECT
    'SMARTMETER_READINGS' AS source_table,
    METADATA$ACTION AS action,
    METADATA$ISUPDATE AS is_update,
    REGIONID AS region_id,
    TIMESTAMP AS record_timestamp
FROM stg_smartmeter_readings_stream_log
WHERE METADATA$ACTION IN ('INSERT','DELETE') OR METADATA$ISUPDATE = TRUE

UNION ALL

SELECT
    'STG_SUBSTATION_LOAD' AS source_table,
    METADATA$ACTION AS action,
    METADATA$ISUPDATE AS is_update,
    RegionID AS region_id,
    Timestamp AS record_timestamp
FROM stg_substation_load_log
WHERE METADATA$ACTION IN ('INSERT','DELETE') OR METADATA$ISUPDATE = TRUE;


alter task task_upsert_fact_consumption resume;
alter task task_upsert_fact_consumption suspend;
execute task task_upsert_fact_consumption;
select * from fact_consumption;

show tasks;

alter task task_upsert_fact_load resume;
alter task task_upsert_fact_load suspend;
execute task task_upsert_fact_load;
select * from fact_load;

alter task task_log_stream_changes resume;
alter task task_log_stream_changes suspend;
execute task task_log_stream_changes;

select * from audit_log;


show tasks;

--Task 10: Materialized Views for Performance
-- mv_load_forecast_accuracy: aggregated forecast vs actual by region
create or replace materialized view mv_load_forecast_accuracy
as 
select region_id,sum(consumption_kwh) as total_consumption_KWH , avg(average_kw) as average_forcast_kw
from fact_consumption
group by region_id;

select * from mv_load_forecast_accuracy;

--mv_renewable_contribution: avg renewable share by day/region
create or replace materialized view mv_renewable_contribution
as 
select region_id,day(gendate) as Day,sum(generated_mwh) as Total_GENERATED_MWH,avg(installed_capacity_mw) as avg_installed_capacity_mw,
avg(capacity_factor_pct) as avg_capacity_factor_pct
from fact_generation
group by day,region_id;

select * from mv_renewable_contribution;

--mv_overload_watchlist: highlight transformers over threshold
create or replace materialized view mv_overload_watchlist
as 
select * from fact_load
where utilization_pct>100;

select * from mv_overload_watchlist;