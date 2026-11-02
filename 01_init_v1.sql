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

-- Stage and file formats (LOADER_ROLE uses these)
USE ROLE LOADER_ROLE;
USE DATABASE ANALYSECOST;
USE WAREHOUSE COMPUTE_WH;



-- 2. Create the stage using the integration
CREATE OR replace STAGE RAW.devcostdata
  URL = 'azure://stazurecostanalysis1.blob.core.windows.net/devcostdata/'
  STORAGE_INTEGRATION = project3_adls_integration;

drop stage RAW.devcostdata;

GRANT USAGE ON STAGE RAW.DEVCOSTDATA TO ROLE LOADER_ROLE;
GRANT READ ON STAGE RAW.DEVCOSTDATA TO ROLE LOADER_ROLE;

-- Show all stages
SHOW STAGES LIKE 'devcostdata';

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


