CREATE OR REPLACE VIEW VW_OVERLOAD_RISK_STATE_SIMPLE AS
WITH load_with_state AS (
    SELECT
        f.REGION_ID,
        r.STATE,
        f.TIMESTAMP,
        DATE(f.TIMESTAMP) AS DATE_KEY,      -- ✅ For DIM_DATE join
        f.UTILIZATION_PCT,

        -- previous utilization within same STATE
        LAG(f.UTILIZATION_PCT) OVER (
            PARTITION BY r.STATE
            ORDER BY f.TIMESTAMP
        ) AS PREVIOUS_UTIL,

        -- pick the latest record for that STATE
        ROW_NUMBER() OVER (
            PARTITION BY r.STATE
            ORDER BY f.TIMESTAMP DESC
        ) AS RN
    FROM FACT_LOAD f
    JOIN DIM_REGION r
      ON f.REGION_ID = r.REGION_ID        -- ✅ REGION_ID preserved
)
SELECT
    REGION_ID,               -- ✅ kept for linking to DIM_REGION if needed
    STATE,
    DATE_KEY,
    SNAPSHOT_TS,
    UTILIZATION_PCT,
    PREVIOUS_UTIL,
    UTIL_INCREASE,
    RISK_BAND,
    RECOMMENDED_ACTION
FROM (
    SELECT
        REGION_ID,
        STATE,
        DATE_KEY,
        TIMESTAMP AS SNAPSHOT_TS,
        UTILIZATION_PCT,
        PREVIOUS_UTIL,
        (UTILIZATION_PCT - PREVIOUS_UTIL) AS UTIL_INCREASE,

        CASE
            WHEN UTILIZATION_PCT >= 90 THEN 'CRITICAL'
            WHEN UTILIZATION_PCT >= 80 THEN 'WARNING'
            WHEN UTILIZATION_PCT > PREVIOUS_UTIL THEN 'WARNING'
            ELSE 'NORMAL'
        END AS RISK_BAND,

        CASE
            WHEN UTILIZATION_PCT >= 90 THEN 'Immediate load relief required'
            WHEN UTILIZATION_PCT >= 80 THEN 'High load: balance within region'
            WHEN UTILIZATION_PCT > PREVIOUS_UTIL THEN 'Rising trend: monitor'
            ELSE 'Stable'
        END AS RECOMMENDED_ACTION,

        RN
    FROM load_with_state
)
WHERE RN = 1      -- ✅ Only latest per STATE
ORDER BY UTILIZATION_PCT DESC;


select * from vw_overload_risk_state_simple;

CREATE OR REPLACE TABLE alert_log (
    REGION_ID STRING,
    TIMESTAMP TIMESTAMP,
    LOAD_MW FLOAT,
    UTILIZATION_PCT FLOAT,
    LOSSES_PCT FLOAT,
    ALERT_TYPE STRING,
    ALERT_TIME TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TASK task_overload_alert
WAREHOUSE = Capstone
SCHEDULE = '1 minute'
AS
INSERT INTO alert_log (REGION_ID, TIMESTAMP, LOAD_MW, UTILIZATION_PCT, LOSSES_PCT, ALERT_TYPE)
SELECT REGION_ID, TIMESTAMP, LOAD_MW, UTILIZATION_PCT, LOSSES_PCT,
       CASE 
           WHEN UTILIZATION_PCT > 90 THEN 'Critical'
           WHEN UTILIZATION_PCT > 80 THEN 'Warning'
           ELSE 'Normal'
       END AS ALERT_TYPE
FROM FACT_LOAD
WHERE UTILIZATION_PCT > 80 OR LOSSES_PCT > 10;

alter task task_overload_alert suspend;

select * from alert_log;

