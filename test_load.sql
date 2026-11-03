USE ROLE LOADER_ROLE;
USE DATABASE ANALYSECOST;

-- =====================================================================
-- STEP 1: TRUNCATE RAW DATA TABLES
-- =====================================================================
TRUNCATE TABLE IF EXISTS RAW.PCF_RAW;
TRUNCATE TABLE IF EXISTS RAW.NEBULA_IT_PRODUCTS;
TRUNCATE TABLE IF EXISTS RAW.NEBULA_FINANCE;
TRUNCATE TABLE IF EXISTS RAW.NEBULA_PCODE_TO_PROJECT_MAPPINGS;
TRUNCATE TABLE IF EXISTS RAW.NEBULA_PCODE_SPLIT;
TRUNCATE TABLE IF EXISTS RAW.NEBULA_SUBSCRIPTIONS_TYPE;

-- =====================================================================
-- STEP 2: TRUNCATE AUDIT & LOGGING TABLES
-- =====================================================================
-- Cleans run tracking, file ledgers, stage snapshots, and exception history
TRUNCATE TABLE IF EXISTS AUDIT.LOAD_RUN;
TRUNCATE TABLE IF EXISTS AUDIT.FILE_LEDGER;
TRUNCATE TABLE IF EXISTS AUDIT.STAGE_INVENTORY;
TRUNCATE TABLE IF EXISTS AUDIT.RECONCILE_EXCEPTION;
TRUNCATE TABLE IF EXISTS AUDIT.RECONCILE_RUN;

-- (Optional) If you have staging tables managed by dbt that need cleaning:
-- USE ROLE TRANSFORM;
-- TRUNCATE TABLE IF EXISTS STAGING.<staging_table_name>;


select * from AUDIT.LOAD_SOURCE_CONFIG;

SHOW TABLES IN SCHEMA ANALYSECOST.AUDIT;



USE ROLE LOADER_ROLE;
USE DATABASE ANALYSECOST;
USE WAREHOUSE COMPUTE_WH;

-- Step 1: If using Snowpipe, refresh for files dropped in the last 7 days
-- ALTER PIPE RAW.PCF_PIPE REFRESH PREFIX = 'pcf/';

-- Step 2: Trigger manual load for all enabled config sources
CALL AUDIT.SP_LOAD_RAW_MANUAL('SUBSCRIPTIONS');

SELECT run_id, source_name, target_table, status, files_loaded, rows_loaded, error_message
FROM AUDIT.LOAD_RUN
ORDER BY started_at DESC
LIMIT 1;

-- Step 3: Run standalone sync to capture any pending copy events into ledger
CALL AUDIT.SP_SYNC_LEDGER(24);


-- 1. Check if the IT_PRODUCT files are already sitting in the ledger:
SELECT target_table, file_name, status, row_loaded, attempted_at, synced_at
FROM AUDIT.FILE_LEDGER
WHERE target_table = 'RAW.NEBULA_SUBSCRIPTIONS_TYPE'
ORDER BY attempted_at DESC;


-- Step 4: Run the 4-way reconciliation audit
--CALL AUDIT.SP_RECONCILE('IT_PRODUCT');

SELECT 
    RELATIVE_PATH, 
    SIZE, 
    LAST_MODIFIED,
    DATEDIFF('day', LAST_MODIFIED, CURRENT_TIMESTAMP()) AS days_old
FROM DIRECTORY(@RAW.DEVCOSTDATA)
WHERE RELATIVE_PATH LIKE 'config/subscription/%';

-- 3. Run default incremental across ALL sources (last 3 days)
--CALL AUDIT.SP_RECONCILE();

-- 4. Run full historical audit across ALL sources (all-time)
CALL AUDIT.SP_RECONCILE('SUBSCRIPTIONS', 0);



USE ROLE LOADER_ROLE;
USE DATABASE ANALYSECOST;

select * from AUDIT.LOAD_SOURCE_CONFIG;

-- 1. Check Batch Ingestion Runs
SELECT 
    run_id, 
    source_name, 
    target_table, 
    status, 
    files_loaded, 
    files_failed, 
    rows_loaded, 
    started_at, 
    finished_at,
    error_message
FROM AUDIT.LOAD_RUN 
ORDER BY started_at DESC;

-- 2. Inspect Ingested Files in Audit Ledger
SELECT 
    target_table, 
    file_name, 
    load_path, 
    status, 
    row_loaded, 
    row_parsed, 
    error_count, 
    attempted_at, 
    synced_at
FROM AUDIT.FILE_LEDGER 
ORDER BY attempted_at DESC;

-- 3. Check Overall Reconciliation Status
-- Note: Uses EXECUTED_AT as defined in the AUDIT.RECONCILE_RUN DDL
SELECT 
    reconcile_id, 
    status, 
    azure_files, 
    blocking_count, 
    warning_count, 
    exception_count, 
    executed_at
FROM AUDIT.RECONCILE_RUN 
ORDER BY executed_at DESC 
LIMIT 5;

-- 4. Drill Down into Exceptions from the Latest Reconciliation
SELECT 
    exception_type, 
    source_name, 
    file_name, 
    detail, 
    recommended_action, 
    detected_at
FROM AUDIT.RECONCILE_EXCEPTION
WHERE reconcile_id = (
    SELECT reconcile_id 
    FROM AUDIT.RECONCILE_RUN 
    ORDER BY executed_at DESC 
    LIMIT 1
)
ORDER BY exception_type;

-- 5. Inspect Serverless Task Execution History
SELECT 
    NAME, 
    STATE, 
    SCHEDULED_TIME, 
    QUERY_START_TIME, 
    COMPLETED_TIME, 
    ERROR_CODE, 
    ERROR_MESSAGE
FROM TABLE(ANALYSECOST.INFORMATION_SCHEMA.TASK_HISTORY(
    SCHEDULED_TIME_RANGE_START => DATEADD('day', -1, CURRENT_TIMESTAMP())
))
ORDER BY SCHEDULED_TIME DESC;
