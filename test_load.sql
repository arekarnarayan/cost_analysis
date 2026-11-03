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

desc table AUDIT.LOAD_SOURCE_CONFIG;

--SHOW TABLES IN SCHEMA ANALYSECOST.AUDIT;

ALTER STAGE RAW.DEVCOSTDATA REFRESH;    -- refresh the Directory Table after uploading the file to Azure as auto refresh is 


USE ROLE LOADER_ROLE;
USE DATABASE ANALYSECOST;
USE WAREHOUSE COMPUTE_WH;

-- Step 1: If using Snowpipe, refresh for files dropped in the last 7 days
-- ALTER PIPE RAW.PCF_PIPE REFRESH PREFIX = 'pcf/';

-- Step 2: Trigger manual load for all enabled config sources
CALL AUDIT.SP_LOAD_RAW_MANUAL('ALL');


PCODE_MAPPING=NO_NEW_FILES PCODE_SPLIT=SUCCESS SUBSCRIPTIONS=NO_NEW_FILES IT_PRODUCT=NO_NEW_FILES FINANCE_PARAM=NO_NEW_FILES

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
CALL AUDIT.SP_RECONCILE('PCODE_MAPPING', 0);



----anonymous block script (EXECUTE IMMEDIATE $$ ... $$) that iterates through each enabled dataset in AUDIT.LOAD_SOURCE_CONFIG
USE ROLE LOADER_ROLE;
USE DATABASE ANALYSECOST;
USE SCHEMA AUDIT;
USE WAREHOUSE COMPUTE_WH;

EXECUTE IMMEDIATE $$
DECLARE
    res      RESULTSET;
    c_config CURSOR FOR 
        SELECT source_name, target_table, load_mode
        FROM AUDIT.LOAD_SOURCE_CONFIG
        WHERE enabled = TRUE
        and LOAD_MODE = 'MANUAL'
        ORDER BY load_mode DESC, source_name ASC;

    v_src    VARCHAR;
    v_tbl    VARCHAR;
    v_mode   VARCHAR;
    v_out    VARCHAR;
    v_status VARCHAR;
BEGIN
    -- 1. Create a session temporary table to store the results of each run
    CREATE OR REPLACE TEMPORARY TABLE TEMP_RECONCILE_RESULTS (
        source_name      VARCHAR,
        target_table     VARCHAR,
        load_mode        VARCHAR,
        overall_status   VARCHAR,
        reconcile_detail VARCHAR,
        executed_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    );

    -- 2. Loop through every enabled source from the config table
    FOR r IN c_config DO
        v_src  := r.source_name;
        v_tbl  := r.target_table;
        v_mode := r.load_mode;
        v_out  := '';

        BEGIN
            -- Execute SP_RECONCILE for this specific source (0 = all-time historical lookback)
            CALL AUDIT.SP_RECONCILE(:v_src, 0) INTO :v_out;
            
            -- Extract status prefix (PASS, FAIL, PASS_WITH_WARNINGS)
            v_status := SPLIT_PART(:v_out, ' ', 1);

        EXCEPTION
            WHEN OTHER THEN
                v_status := 'ERROR';
                v_out    := 'SQL Exception: ' || SQLERRM;
        END;

        -- 3. Record the result
        INSERT INTO TEMP_RECONCILE_RESULTS (
            source_name, target_table, load_mode, overall_status, reconcile_detail
        )
        VALUES (
            :v_src, :v_tbl, :v_mode, :v_status, :v_out
        );
    END FOR;

    -- 4. Return the entire output as an interactive grid in Snowsight
    res := (
        SELECT 
            source_name,
            target_table,
            load_mode,
            overall_status,
            reconcile_detail,
            executed_at
        FROM TEMP_RECONCILE_RESULTS
        ORDER BY 
            CASE overall_status WHEN 'FAIL' THEN 1 WHEN 'PASS_WITH_WARNINGS' THEN 2 ELSE 3 END,
            source_name
    );

    RETURN TABLE(res);
END;
$$;


--If any source displays FAIL or PASS_WITH_WARNINGS, run this query to view the discrepancies found by the loop:
SELECT 
    e.detected_at,
    e.source_name,
    e.exception_type,
    e.file_name,
    e.detail,
    e.recommended_action
FROM AUDIT.RECONCILE_EXCEPTION e
WHERE e.reconcile_id IN (
    SELECT DISTINCT SPLIT_PART(SPLIT_PART(reconcile_detail, 'reconcile_id=', 2), ' ', 1)
    FROM TEMP_RECONCILE_RESULTS
    WHERE overall_status IN ('FAIL', 'PASS_WITH_WARNINGS')
)
ORDER BY e.detected_at DESC;

