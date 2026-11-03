USE ROLE ACCOUNTADMIN;

CREATE DATABASE IF NOT EXISTS ANALYSECOST;
CREATE SCHEMA IF NOT EXISTS ANALYSECOST.RAW;
CREATE SCHEMA IF NOT EXISTS ANALYSECOST.AUDIT;
CREATE SCHEMA IF NOT EXISTS ANALYSECOST.STAGING;

-- Loader role: the only role that writes to RAW and AUDIT
CREATE ROLE IF NOT EXISTS LOADER_ROLE;
GRANT ROLE LOADER_ROLE TO ROLE SYSADMIN;

create or replace warehouse COMPUTE_WH
    warehouse_size = 'x-small'
    auto_suspend = 60
    auto_resume = true
    comment = 'Pipeline workloads - pipeline';

GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE LOADER_ROLE;
GRANT USAGE ON DATABASE ANALYSECOST TO ROLE LOADER_ROLE;
GRANT USAGE ON SCHEMA ANALYSECOST.RAW TO ROLE LOADER_ROLE;
GRANT ALL ON SCHEMA ANALYSECOST.AUDIT TO ROLE LOADER_ROLE;
GRANT ALL ON FUTURE TABLES IN SCHEMA ANALYSECOST.AUDIT TO ROLE LOADER_ROLE;
GRANT SELECT, INSERT ON ALL TABLES IN SCHEMA ANALYSECOST.RAW TO ROLE LOADER_ROLE;
GRANT SELECT, INSERT ON FUTURE TABLES IN SCHEMA ANALYSECOST.RAW TO ROLE LOADER_ROLE;
GRANT SELECT, INSERT, update, DELETE ON ALL TABLES IN SCHEMA ANALYSECOST.AUDIT TO ROLE LOADER_ROLE;
GRANT SELECT, INSERT, update, DELETE ON FUTURE TABLES IN SCHEMA ANALYSECOST.AUDIT TO ROLE LOADER_ROLE;
GRANT CREATE PIPE ON SCHEMA ANALYSECOST.RAW TO ROLE LOADER_ROLE;
GRANT EXECUTE TASK ON ACCOUNT TO ROLE LOADER_ROLE;
GRANT EXECUTE MANAGED TASK ON ACCOUNT TO ROLE LOADER_ROLE;


CREATE ROLE IF NOT EXISTS TRANSFORM;
GRANT ROLE TRANSFORM TO ROLE ACCOUNTADMIN;


CREATE USER IF NOT EXISTS dbt
  PASSWORD='dbtPassword123'
  LOGIN_NAME='dbt'
  MUST_CHANGE_PASSWORD=FALSE
  DEFAULT_WAREHOUSE='COMPUTE_WH'
  DEFAULT_ROLE=TRANSFORM
  DEFAULT_NAMESPACE='analysecost.RAW'
  COMMENT='DBT user used for data transformation';
ALTER USER dbt SET TYPE = LEGACY_SERVICE;

GRANT ROLE TRANSFORM TO USER dbt;

-- dbt role: read RAW, build STAGING, no writes to RAW
--REVOKE ALL ON ALL TABLES IN SCHEMA ANALYSECOST.RAW FROM ROLE TRANSFORM;
GRANT SELECT ON ALL TABLES IN SCHEMA ANALYSECOST.RAW TO ROLE TRANSFORM;
GRANT SELECT ON FUTURE TABLES IN SCHEMA ANALYSECOST.RAW TO ROLE TRANSFORM;
GRANT SELECT ON ALL TABLES IN SCHEMA ANALYSECOST.AUDIT TO ROLE TRANSFORM;

GRANT ALL ON SCHEMA ANALYSECOST.STAGING TO ROLE TRANSFORM;



---------------------------------------------------------------------------------------------
-- 1. Create the storage integration (run once by ACCOUNTADMIN)
use role accountadmin;
Use database analysecost;

create storage integration if not exists project3_adls_integration
    type = external_stage
    storage_provider = 'azure'
    enabled = true
    azure_tenant_id = 'e5ce28dd-0763-48b9-8900-5863b66c2266'
    storage_allowed_locations = (
        'azure://stazurecostanalysis1.blob.core.windows.net/devcostdata/'
    );


desc integration project3_adls_integration;
-- CRITICAL :: approve URL and then grant blob_reader role to user mentioned in the AZURE_MULTI_TENANT_APP_NAME


GRANT USAGE ON INTEGRATION project3_adls_integration TO ROLE LOADER_ROLE;

-- Grant schema-level privileges
-- 1. Database level
GRANT USAGE ON DATABASE ANALYSECOST TO ROLE LOADER_ROLE;


GRANT USAGE ON SCHEMA RAW TO ROLE LOADER_ROLE;
GRANT CREATE STAGE ON SCHEMA RAW TO ROLE LOADER_ROLE;

-- Optional (best practice)
GRANT USAGE ON ALL STAGES IN SCHEMA RAW TO ROLE LOADER_ROLE;
GRANT USAGE ON FUTURE STAGES IN SCHEMA RAW TO ROLE LOADER_ROLE;


GRANT CREATE FILE FORMAT ON SCHEMA RAW TO ROLE LOADER_ROLE;

-- Recommended additional grants
GRANT USAGE ON ALL FILE FORMATS IN SCHEMA RAW TO ROLE LOADER_ROLE;
GRANT USAGE ON FUTURE FILE FORMATS IN SCHEMA RAW TO ROLE LOADER_ROLE;

---------------------------------------------------------------------------------------------

USE ROLE ACCOUNTADMIN;
USE DATABASE ANALYSECOST;


-- 2. Create the stage using the integration
CREATE OR replace STAGE RAW.devcostdata
  URL = 'azure://stazurecostanalysis1.blob.core.windows.net/devcostdata/'
  STORAGE_INTEGRATION = project3_adls_integration;


GRANT USAGE ON STAGE RAW.DEVCOSTDATA TO ROLE LOADER_ROLE;
GRANT READ ON STAGE RAW.DEVCOSTDATA TO ROLE LOADER_ROLE;

-- Show all stages
SHOW STAGES LIKE 'devcostdata';


-- 1. Enable Directory Table without auto-refresh (no notification integration required)
ALTER STAGE RAW.DEVCOSTDATA SET 
    DIRECTORY = (
        ENABLE = TRUE, 
        AUTO_REFRESH = FALSE
    );

-- 2. Manually refresh the directory table
ALTER STAGE RAW.DEVCOSTDATA REFRESH;

-- 3. Verify directory contents
SELECT * FROM DIRECTORY(@RAW.DEVCOSTDATA) LIMIT 10;


-- List with pattern (e.g. only Parquet files)
LIST @devcostdata PATTERN = '.*\.parquet';

CREATE FILE FORMAT IF NOT EXISTS RAW.PARQUET
  TYPE = PARQUET
  COMPRESSION = AUTO
  USE_LOGICAL_TYPE = TRUE
  USE_VECTORIZED_SCANNER = TRUE
  BINARY_AS_TEXT = TRUE
  COMMENT = 'Parquet file format for PCF cost files';

CREATE FILE FORMAT IF NOT EXISTS RAW.CSV
  TYPE = CSV
  FIELD_DELIMITER = ','
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1
  TRIM_SPACE = TRUE
  NULL_IF = ('NULL', 'null', '')
  EMPTY_FIELD_AS_NULL = TRUE
  COMMENT = 'CSV format for config files';

---------------------------------------------------------------------------------------------

use role accountadmin;
Use database analysecost;

-- PCF: whole row as VARIANT

CREATE TABLE IF NOT EXISTS RAW.PCF_RAW (
    raw_row_id              NUMBER AUTOINCREMENT,
    record_payload          VARIANT        NOT NULL,
    source_system           VARCHAR,
    load_id                 VARCHAR        NOT NULL,
    source_file_name        VARCHAR        NOT NULL,
    source_file_row_number  NUMBER,
    loaded_at               TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP()
);


-- Config tables: typed VARCHAR columns, append-only
CREATE TABLE IF NOT EXISTS RAW.NEBULA_IT_PRODUCTS (
    Purpose VARCHAR, Pcode VARCHAR, Team VARCHAR, Team_Code VARCHAR,
    source_system VARCHAR, load_id VARCHAR NOT NULL, source_file_name VARCHAR NOT NULL,
    source_file_row_number NUMBER, loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);


CREATE TABLE IF NOT EXISTS RAW.NEBULA_FINANCE (
    Parameter VARCHAR, Value VARCHAR, Load_Date VARCHAR,
    source_system VARCHAR, load_id VARCHAR NOT NULL, source_file_name VARCHAR NOT NULL,
    source_file_row_number NUMBER, loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);


CREATE TABLE IF NOT EXISTS RAW.NEBULA_PCODE_TO_PROJECT_MAPPINGS (
    Pcode VARCHAR, Allocation VARCHAR, Azure_Budget_Group VARCHAR, Project_Code VARCHAR, __START_AT VARCHAR,
    source_system VARCHAR, load_id VARCHAR NOT NULL, source_file_name VARCHAR NOT NULL,
    source_file_row_number NUMBER, loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);


CREATE TABLE IF NOT EXISTS RAW.NEBULA_PCODE_SPLIT (
    Azure_Budget_Group VARCHAR, Pcode VARCHAR, Project_Code VARCHAR, Allocation VARCHAR, share VARCHAR, __START_AT VARCHAR,
    source_system VARCHAR, load_id VARCHAR NOT NULL, source_file_name VARCHAR NOT NULL,
    source_file_row_number NUMBER, loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);


CREATE TABLE IF NOT EXISTS RAW.NEBULA_SUBSCRIPTIONS_TYPE (
    Environment VARCHAR, Description_Purpose VARCHAR, Subscription_name VARCHAR, __START_AT VARCHAR, subscriptions VARCHAR,
    source_system VARCHAR, load_id VARCHAR NOT NULL, source_file_name VARCHAR NOT NULL,
    source_file_row_number NUMBER, loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

---------------------------------------------------------------------------------------------

--audit tables ---

--1. for manula or auto load --config table -- SP pull entry from config and then execute immediate

-- Control table: one row per source. Adding a config file type is an INSERT.
CREATE TABLE IF NOT EXISTS AUDIT.LOAD_SOURCE_CONFIG (
    source_name     VARCHAR        NOT NULL,   -- unique key by convention
    source_prefix   VARCHAR        NOT NULL,   -- path under the container, e.g. 'pcf/'
    file_pattern    VARCHAR        NOT NULL,   -- regex, e.g. '.*[.]parquet'
    file_format     VARCHAR        NOT NULL,   -- 'RAW.PARQUET' or 'RAW.CSV'
    target_table    VARCHAR        NOT NULL,   -- 'RAW.PCF_RAW'
    target_columns  VARCHAR        NOT NULL,   -- columns before metadata
    select_list     VARCHAR        NOT NULL,   -- '$1' or '$1::VARCHAR, $2::VARCHAR, ...'
    load_mode       VARCHAR        NOT NULL,   -- 'PIPE' or 'MANUAL'
    enabled         BOOLEAN        DEFAULT TRUE
);

--configuration insertion into config table

INSERT INTO AUDIT.LOAD_SOURCE_CONFIG VALUES
 ('PCF', 'pcf/', '.*[.]parquet', 'RAW.PARQUET', 'RAW.PCF_RAW', 'record_payload', '$1', 'MANUAL', TRUE),
 ('IT_PRODUCT', 'config/it_product/', '.*[.]csv', 'RAW.CSV', 'RAW.NEBULA_IT_PRODUCTS',
  'Purpose, Pcode, Team, Team_Code', '$1::VARCHAR, $2::VARCHAR, $3::VARCHAR, $4::VARCHAR', 'MANUAL', TRUE),
 ('FINANCE_PARAM', 'config/finance_param/', '.*[.]csv', 'RAW.CSV', 'RAW.NEBULA_FINANCE',
  'Parameter, Value, Load_Date', '$1::VARCHAR, $2::VARCHAR, $3::VARCHAR', 'MANUAL', TRUE);



-- -- Seed Ingestion Configurations
-- MERGE INTO AUDIT.LOAD_SOURCE_CONFIG tgt
-- USING (
--     SELECT 'PCF' AS source_name, 'pcf/' AS source_prefix, '.*[.]parquet' AS file_pattern, 'RAW.PARQUET' AS file_format, 'RAW.PCF_RAW' AS target_table, 'record_payload' AS target_columns, '$1' AS select_list, 'MANUAL' AS load_mode, TRUE AS enabled
--     UNION ALL
--     SELECT 'IT_PRODUCT', 'config/it_product/', '.*[.]csv', 'RAW.CSV', 'RAW.NEBULA_IT_PRODUCTS', 'Purpose, Pcode, Team, Team_Code', '$1::VARCHAR, $2::VARCHAR, $3::VARCHAR, $4::VARCHAR', 'MANUAL', TRUE
--     UNION ALL
--     SELECT 'FINANCE_PARAM', 'config/finance_param/', '.*[.]csv', 'RAW.CSV', 'RAW.NEBULA_FINANCE', 'Parameter, Value, Load_Date', '$1::VARCHAR, $2::VARCHAR, $3::VARCHAR', 'MANUAL', TRUE
-- ) src
-- ON tgt.source_name = src.source_name
-- WHEN MATCHED THEN UPDATE SET
--     source_prefix = src.source_prefix,
--     file_pattern = src.file_pattern,
--     file_format = src.file_format,
--     target_table = src.target_table,
--     target_columns = src.target_columns,
--     select_list = src.select_list,
--     load_mode = src.load_mode,
--     enabled = src.enabled
-- WHEN NOT MATCHED THEN INSERT VALUES (
--     src.source_name, src.source_prefix, src.file_pattern, src.file_format,
--     src.target_table, src.target_columns, src.select_list, src.load_mode, src.enabled
-- );


select * from AUDIT.LOAD_SOURCE_CONFIG;

---audit log tables

-- One row per manual batch (Option 1)
CREATE TABLE IF NOT EXISTS AUDIT.LOAD_RUN (
    run_id          VARCHAR        NOT NULL,
    source_name     VARCHAR        NOT NULL,
    target_table    VARCHAR        NOT NULL,
    load_path       VARCHAR        NOT NULL,   -- MANUAL
    started_at      TIMESTAMP_NTZ,
    finished_at     TIMESTAMP_NTZ,
    status          VARCHAR,                   -- RUNNING / SUCCESS / PARTIAL / FAILED / NO_NEW_FILES / ERROR
    files_loaded    NUMBER,
    files_failed    NUMBER,
    rows_loaded     NUMBER,
    error_message   VARCHAR,
    triggered_by    VARCHAR        DEFAULT CURRENT_USER()
);

-- One row per file attempt, from both paths (Option 1 and Option 2)
CREATE TABLE IF NOT EXISTS AUDIT.FILE_LEDGER (
    target_table         VARCHAR        NOT NULL,
    file_name            VARCHAR        NOT NULL,   -- as reported by COPY_HISTORY
    load_path            VARCHAR        NOT NULL,   -- PIPE / MANUAL
    attempted_at         TIMESTAMP_NTZ  NOT NULL,   -- COPY_HISTORY LAST_LOAD_TIME (attempt key)
    status               VARCHAR        NOT NULL,   -- LOADED / PARTIAL / FAILED
    raw_status           VARCHAR,
    file_size            NUMBER,
    row_parsed           NUMBER,
    row_loaded           NUMBER,
    error_count          NUMBER,
    first_error_message  VARCHAR,
    first_error_line     NUMBER,
    first_error_column   VARCHAR,
    pipe_received_at     TIMESTAMP_NTZ,
    first_commit_at      TIMESTAMP_NTZ,
    ingest_latency_sec   NUMBER,
    synced_at            TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_file_ledger PRIMARY KEY (target_table, file_name, attempted_at)
);


-- RAW row counts per file, used by the rowcount check
CREATE OR REPLACE VIEW AUDIT.V_RAW_FILE_ROWS AS
SELECT 'RAW.PCF_RAW' AS target_table, source_file_name AS file_name, COUNT(*) AS raw_rows FROM RAW.PCF_RAW GROUP BY source_file_name
UNION ALL SELECT 'RAW.NEBULA_IT_PRODUCTS', source_file_name, COUNT(*) FROM RAW.NEBULA_IT_PRODUCTS GROUP BY source_file_name
UNION ALL SELECT 'RAW.NEBULA_FINANCE', source_file_name, COUNT(*) FROM RAW.NEBULA_FINANCE GROUP BY source_file_name
UNION ALL SELECT 'RAW.NEBULA_PCODE_TO_PROJECT_MAPPINGS', source_file_name, COUNT(*) FROM RAW.NEBULA_PCODE_TO_PROJECT_MAPPINGS GROUP BY source_file_name
UNION ALL SELECT 'RAW.NEBULA_PCODE_SPLIT', source_file_name, COUNT(*) FROM RAW.NEBULA_PCODE_SPLIT GROUP BY source_file_name
UNION ALL SELECT 'RAW.NEBULA_SUBSCRIPTIONS_TYPE', source_file_name, COUNT(*) FROM RAW.NEBULA_SUBSCRIPTIONS_TYPE GROUP BY source_file_name;



select * from AUDIT.V_RAW_FILE_ROWS;

select * from AUDIT.V_COPY_HISTORY_RAW;

-- One view over COPY_HISTORY for all RAW tables. Both pipe and manual loads appear here.
USE ROLE LOADER_ROLE;
USE DATABASE ANALYSECOST;
USE SCHEMA AUDIT;

CREATE OR REPLACE VIEW AUDIT.V_COPY_HISTORY_RAW AS
SELECT 
    'RAW.PCF_RAW' AS target_table, 
    FILE_NAME, PIPE_NAME, LAST_LOAD_TIME, STATUS, FILE_SIZE, ROW_PARSED, ROW_COUNT,
    ERROR_COUNT, FIRST_ERROR_MESSAGE, FIRST_ERROR_LINE_NUMBER, FIRST_ERROR_COLUMN_NAME, 
    PIPE_RECEIVED_TIME, LAST_LOAD_TIME AS FIRST_COMMIT_TIME
FROM TABLE(ANALYSECOST.INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'RAW.PCF_RAW', 
    START_TIME => DATEADD('day', -14, CURRENT_TIMESTAMP())
))

UNION ALL

SELECT 
    'RAW.NEBULA_IT_PRODUCTS', 
    FILE_NAME, PIPE_NAME, LAST_LOAD_TIME, STATUS, FILE_SIZE, ROW_PARSED, ROW_COUNT,
    ERROR_COUNT, FIRST_ERROR_MESSAGE, FIRST_ERROR_LINE_NUMBER, FIRST_ERROR_COLUMN_NAME, 
    PIPE_RECEIVED_TIME, LAST_LOAD_TIME AS FIRST_COMMIT_TIME
FROM TABLE(ANALYSECOST.INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'RAW.NEBULA_IT_PRODUCTS', 
    START_TIME => DATEADD('day', -14, CURRENT_TIMESTAMP())
))

UNION ALL

SELECT 
    'RAW.NEBULA_FINANCE', 
    FILE_NAME, PIPE_NAME, LAST_LOAD_TIME, STATUS, FILE_SIZE, ROW_PARSED, ROW_COUNT,
    ERROR_COUNT, FIRST_ERROR_MESSAGE, FIRST_ERROR_LINE_NUMBER, FIRST_ERROR_COLUMN_NAME, 
    PIPE_RECEIVED_TIME, LAST_LOAD_TIME AS FIRST_COMMIT_TIME
FROM TABLE(ANALYSECOST.INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'RAW.NEBULA_FINANCE', 
    START_TIME => DATEADD('day', -14, CURRENT_TIMESTAMP())
))

UNION ALL

SELECT 
    'RAW.NEBULA_PCODE_TO_PROJECT_MAPPINGS', 
    FILE_NAME, PIPE_NAME, LAST_LOAD_TIME, STATUS, FILE_SIZE, ROW_PARSED, ROW_COUNT,
    ERROR_COUNT, FIRST_ERROR_MESSAGE, FIRST_ERROR_LINE_NUMBER, FIRST_ERROR_COLUMN_NAME, 
    PIPE_RECEIVED_TIME, LAST_LOAD_TIME AS FIRST_COMMIT_TIME
FROM TABLE(ANALYSECOST.INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'RAW.NEBULA_PCODE_TO_PROJECT_MAPPINGS', 
    START_TIME => DATEADD('day', -14, CURRENT_TIMESTAMP())
))

UNION ALL

SELECT 
    'RAW.NEBULA_PCODE_SPLIT', 
    FILE_NAME, PIPE_NAME, LAST_LOAD_TIME, STATUS, FILE_SIZE, ROW_PARSED, ROW_COUNT,
    ERROR_COUNT, FIRST_ERROR_MESSAGE, FIRST_ERROR_LINE_NUMBER, FIRST_ERROR_COLUMN_NAME, 
    PIPE_RECEIVED_TIME, LAST_LOAD_TIME AS FIRST_COMMIT_TIME
FROM TABLE(ANALYSECOST.INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'RAW.NEBULA_PCODE_SPLIT', 
    START_TIME => DATEADD('day', -14, CURRENT_TIMESTAMP())
))

UNION ALL

SELECT 
    'RAW.NEBULA_SUBSCRIPTIONS_TYPE', 
    FILE_NAME, PIPE_NAME, LAST_LOAD_TIME, STATUS, FILE_SIZE, ROW_PARSED, ROW_COUNT,
    ERROR_COUNT, FIRST_ERROR_MESSAGE, FIRST_ERROR_LINE_NUMBER, FIRST_ERROR_COLUMN_NAME, 
    PIPE_RECEIVED_TIME, LAST_LOAD_TIME AS FIRST_COMMIT_TIME
FROM TABLE(ANALYSECOST.INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'RAW.NEBULA_SUBSCRIPTIONS_TYPE', 
    START_TIME => DATEADD('day', -14, CURRENT_TIMESTAMP())
));

--------------------reconciliation -------------------

USE ROLE ACCOUNTADMIN;
USE DATABASE ANALYSECOST;

-- 1. Temporary inventory staging for reconciliation runs
CREATE TABLE IF NOT EXISTS AUDIT.STAGE_INVENTORY (
    reconcile_id        VARCHAR NOT NULL,
    file_key            VARCHAR NOT NULL,
    size_bytes          NUMBER,
    last_modified_raw   VARCHAR,
    source_name         VARCHAR,
    target_table        VARCHAR,
    scanned_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- 2. Audit exceptions ledger
CREATE TABLE IF NOT EXISTS AUDIT.RECONCILE_EXCEPTION (
    exception_id        NUMBER AUTOINCREMENT,
    reconcile_id        VARCHAR NOT NULL,
    exception_type      VARCHAR NOT NULL, -- LEDGER_STALE, PIPE_NOT_RUNNING, MISSING_LOAD, etc.
    source_name         VARCHAR,
    file_name           VARCHAR,
    detail              VARCHAR,
    recommended_action  VARCHAR,
    detected_at         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- 3. High-level reconciliation run ledger
CREATE TABLE IF NOT EXISTS AUDIT.RECONCILE_RUN (
    reconcile_id        VARCHAR NOT NULL PRIMARY KEY,
    azure_files         NUMBER,
    exception_count     NUMBER,
    blocking_count      NUMBER,
    warning_count       NUMBER,
    status              VARCHAR, -- PASS, PASS_WITH_WARNINGS, FAIL
    executed_at         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);


select * from AUDIT.V_RAW_FILE_ROWS;  V_COPY_HISTORY_RAW

-- 5. Notification Integration (Optional - needed if calling SYSTEM$SEND_EMAIL)
-- CREATE NOTIFICATION INTEGRATION IF NOT EXISTS PCF_EMAIL_INT
--     TYPE = EMAIL
--     ENABLED = TRUE
--     ALLOWED_RECIPIENTS = ('ops-team@example.com');

USE ROLE ACCOUNTADMIN;
USE DATABASE ANALYSECOST;

-- Grant select on current and future views in AUDIT schema to LOADER_ROLE
GRANT SELECT ON ALL VIEWS IN SCHEMA ANALYSECOST.AUDIT TO ROLE LOADER_ROLE;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA ANALYSECOST.AUDIT TO ROLE LOADER_ROLE;

-- Or transfer ownership of the view to LOADER_ROLE
GRANT OWNERSHIP ON VIEW ANALYSECOST.AUDIT.V_COPY_HISTORY_RAW TO ROLE LOADER_ROLE COPY CURRENT GRANTS;