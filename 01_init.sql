-- Snowflake user creation
-- Step 1: Use an admin role
USE ROLE ACCOUNTADMIN;

-- Step 2: Create the `transform` role and assign it to ACCOUNTADMIN
CREATE ROLE IF NOT EXISTS TRANSFORM;
GRANT ROLE TRANSFORM TO ROLE ACCOUNTADMIN;

-- Step 3: Create a default warehouse
create or replace warehouse COMPUTE_WH
    warehouse_size = 'x-small'
    auto_suspend = 60
    auto_resume = true
    comment = 'Pipeline workloads - pipeline';
    
GRANT OPERATE ON WAREHOUSE COMPUTE_WH TO ROLE TRANSFORM;

-- Step 4: Create a database and schema for the MovieLens project
CREATE DATABASE IF NOT EXISTS analysecost;
CREATE SCHEMA IF NOT EXISTS analysecost.RAW;

-- Step 5: Create the `dbt` user and assign to the transform role
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


-- Step 6: Grant permissions to the `transform` role
GRANT ALL ON WAREHOUSE COMPUTE_WH TO ROLE TRANSFORM;
GRANT ALL ON DATABASE analysecost TO ROLE TRANSFORM;
GRANT ALL ON ALL SCHEMAS IN DATABASE analysecost TO ROLE TRANSFORM;
GRANT ALL ON FUTURE SCHEMAS IN DATABASE analysecost TO ROLE TRANSFORM;
GRANT ALL ON ALL TABLES IN SCHEMA analysecost.RAW TO ROLE TRANSFORM;
GRANT ALL ON FUTURE TABLES IN SCHEMA analysecost.RAW TO ROLE TRANSFORM;

-- -- required for dbt user to create new schema like staging, intermediate, mart
-- USE ROLE ACCOUNTADMIN;

-- -- 1. Allow the TRANSFORM role to create schemas in the analysecost database
-- GRANT CREATE SCHEMA ON DATABASE analysecost TO ROLE TRANSFORM;

-- -- 2. Explicitly create your Staging schema now (recommended best practice)
-- CREATE SCHEMA IF NOT EXISTS analysecost.STAGING;

-- -- 3. Grant full permissions on the new staging schema to TRANSFORM
-- GRANT ALL ON SCHEMA analysecost.STAGING TO ROLE TRANSFORM;
-- GRANT ALL ON FUTURE TABLES IN SCHEMA analysecost.STAGING TO ROLE TRANSFORM;

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


-- 2. Create the stage using the integration
CREATE OR replace STAGE RAW.devcostdata
  URL = 'azure://stazurecostanalysis1.blob.core.windows.net/devcostdata/'
  STORAGE_INTEGRATION = project3_adls_integration;

-- Show all stages
SHOW STAGES LIKE 'devcostdata';

-- List with pattern (e.g. only Parquet files)
LIST @devcostdata PATTERN = '.*\.parquet';


--3. file format
CREATE FILE FORMAT IF NOT EXISTS RAW.parquet
    TYPE = PARQUET
    COMPRESSION = AUTO
    USE_LOGICAL_TYPE = TRUE                    -- use Parquet logical types (dates, decimals, etc.)
    USE_VECTORIZED_SCANNER = TRUE              -- faster scans (Snowflake recommendation)
    REPLACE_INVALID_CHARACTERS = TRUE          -- replace invalid UTF-8 instead of erroring
    BINARY_AS_TEXT = TRUE
    COMMENT = 'Parquet file format for cost data files';

describe file format raw.parquet;

-- Quick sample of real Parquet data only
SELECT 
    METADATA$FILENAME,
    METADATA$FILE_ROW_NUMBER,
    $1
FROM @devcostdata/pcf/
  (
    FILE_FORMAT => 'raw.parquet',
    PATTERN => '.*\\.parquet'
  )
LIMIT 10;

--4. create raw tables

USE DATABASE ANALYSECOST;
USE SCHEMA RAW;

---------------------------------------------------------------
-- 0. PCF data
---------------------------------------------------------------
CREATE TABLE IF NOT EXISTS RAW.pcf_data(
    SubscriptionName        VARCHAR,
    UsageStart              VARCHAR,
    UsageEnd                VARCHAR,
    IsEstimated             VARCHAR,
    TotalPretaxCost         DOUBLE,
    Currency                VARCHAR,
    ConsumedService         VARCHAR,
    Product                 VARCHAR,
    InstanceId              VARCHAR,
    tags                    VARCHAR,
    file_name               VARCHAR,
    system_name             VARCHAR DEFAULT 'nebula-legacy',
    start_time              TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

--drop table analysecost.raw.pcf_data;

--Quick diagnostic before re-running COPYRun this to see the actual column names inside your Parquet files:
-- SELECT $1
-- FROM @devcostdata/pcf/
--   (FILE_FORMAT => 'raw.parquet')
-- LIMIT 5;

-- SELECT *
-- FROM TABLE(
--     INFER_SCHEMA(
--         LOCATION => '@devcostdata/pcf/',
--         FILE_FORMAT => 'raw.parquet'
--     )
-- );

---When loading Parquet into a multi-column table, Snowflake requires you to tell it how to map the columns.Fix – use MATCH_BY_COLUMN_NAME
-- COPY INTO analysecost.RAW.pcf_data
-- FROM @devcostdata/pcf/
-- FILE_FORMAT = (FORMAT_NAME = 'raw.parquet')
-- MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
-- PATTERN = '.*\\.parquet'
-- INCLUDE_METADATA = (
--     system_name = METADATA$FILENAME,   -- not needed
--     -- defaults still often stay NULL with MATCH_BY_COLUMN_NAME
-- );


---Alternative (more explicit & safer)

COPY INTO RAW.pcf_data (
    SubscriptionName,
    UsageStart,
    UsageEnd,
    IsEstimated,
    TotalPretaxCost,
    Currency,
    ConsumedService,
    Product,
    InstanceId,
    tags,
    file_name,
    system_name,
    start_time
)
FROM (
    SELECT
        $1:SubscriptionName::VARCHAR,
        $1:UsageStart::VARCHAR,
        $1:UsageEnd::VARCHAR,
        $1:IsEstimated::VARCHAR,
        $1:TotalPretaxCost::DOUBLE,
        $1:Currency::VARCHAR,
        $1:ConsumedService::VARCHAR,
        $1:Product::VARCHAR,
        $1:InstanceId::VARCHAR,
        $1:tags::VARCHAR,
        METADATA$FILENAME,
        'nebula-legacy',
        CURRENT_TIMESTAMP()
    FROM @devcostdata/pcf/
    (FILE_FORMAT => 'raw.parquet', PATTERN => '.*\\.parquet')
)
ON_ERROR = CONTINUE;

select count(1) from RAW.pcf_data;

desc TABLE RAW.pcf_data;


---Config files to be uplaoded with csv

CREATE OR REPLACE FILE FORMAT raw.csv
  TYPE = CSV
  FIELD_DELIMITER = ','
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1
  TRIM_SPACE = TRUE
  NULL_IF = ('NULL', 'null', '')
  EMPTY_FIELD_AS_NULL = TRUE;


  -- ============================================================
-- RAW layer – Append-only tables (no rejection on COPY)
-- ============================================================


-- 1. IT Products (source: config/it_products) -- type 1
CREATE OR REPLACE TABLE RAW.nebula_it_products (
    Purpose             VARCHAR,
    Pcode               VARCHAR,
    Team                VARCHAR,
    Team_Code           VARCHAR,
    -- metadata
    file_name           VARCHAR,
    system_name         VARCHAR          DEFAULT 'nebula-legacy',
    start_time          TIMESTAMP_NTZ    DEFAULT CURRENT_TIMESTAMP()
);

LIST @devcostdata/config/it_product/ PATTERN = '.*\\.csv';

COPY INTO ANALYSECOST.RAW.nebula_it_products (
    Purpose,
    Pcode,
    Team,
    Team_Code,
    file_name,
    system_name,
    start_time
)
FROM (
    SELECT
        $1::VARCHAR,          -- Purpose
        $2::VARCHAR,          -- Pcode
        $3::VARCHAR,          -- Team
        $4::VARCHAR,          -- Team_Code
        METADATA$FILENAME,
        'nebula-legacy',
        CURRENT_TIMESTAMP()
    FROM @devcostdata/config/it_product/
    (FILE_FORMAT => 'raw.csv', PATTERN => '.*\\.csv')
)
ON_ERROR = CONTINUE;


-- 2. Finance Params (source: config/finance_params) -- type 2
CREATE OR REPLACE TABLE RAW.nebula_finance (
    Parameter           VARCHAR,
    Value               VARCHAR,
    Load_Date           VARCHAR,
    -- metadata
    file_name           VARCHAR,
    system_name         VARCHAR          DEFAULT 'nebula-legacy',
    start_time          TIMESTAMP_NTZ    DEFAULT CURRENT_TIMESTAMP()
);

LIST @devcostdata/config/finance_param/ PATTERN = '.*\\.csv';

Copy into ANALYSECOST.RAW.nebula_finance(
    Parameter,
    Value,
    Load_Date,    
    file_name,
    system_name,
    start_time
    )
    FROM (
        SELECT
            $1::VARCHAR,          -- Parameter
            $2::VARCHAR,          -- Value
            $3::VARCHAR,          -- Load_Date
            METADATA$FILENAME,
            'nebula-legacy',
            CURRENT_TIMESTAMP()
        FROM @devcostdata/config/finance_param
        (FILE_FORMAT => 'raw.csv', PATTERN => '.*\\.csv')
    )
ON_ERROR = CONTINUE;

-- 3. Pcode to Project Mappings (source: config/pcode_to_project_mappings)
CREATE OR REPLACE TABLE RAW.nebula_pcode_to_project_mappings (
    Pcode               VARCHAR,
    Allocation          VARCHAR,
    Azure_Budget_Group  VARCHAR,
    Project_Code        VARCHAR,
    __START_AT          VARCHAR,
    -- metadata
    file_name           VARCHAR,
    system_name         VARCHAR          DEFAULT 'nebula-legacy',
    start_time          TIMESTAMP_NTZ    DEFAULT CURRENT_TIMESTAMP()
);

LIST @devcostdata/config/pcode_to_project_mapping/ PATTERN = '.*\\.csv';

COPY INTO ANALYSECOST.RAW.nebula_pcode_to_project_mappings (
    Pcode,
    Allocation,
    Azure_Budget_Group,
    Project_Code,
    __START_AT,
    file_name,
    system_name,
    start_time
)
FROM (
    SELECT
        $1::VARCHAR,          -- Pcode
        $2::VARCHAR,          -- Allocation
        $3::VARCHAR,          -- Azure_Budget_Group
        $4::VARCHAR,          -- Project_Code
        $5::VARCHAR,          -- __START_AT
        METADATA$FILENAME,
        'nebula-legacy',
        CURRENT_TIMESTAMP()
    FROM @devcostdata/config/pcode_to_project_mapping
    (FILE_FORMAT => 'raw.csv', PATTERN => '.*\\.csv')
)
ON_ERROR = CONTINUE;


-- 4. Pcode Split (source: config/pcode_split)
CREATE OR REPLACE TABLE RAW.nebula_pcode_split (
    Azure_Budget_Group  VARCHAR,
    Pcode               VARCHAR,
    Project_Code        VARCHAR,
    Allocation          VARCHAR,
    share               VARCHAR,
    __START_AT          VARCHAR,
    -- metadata
    file_name           VARCHAR,
    system_name         VARCHAR          DEFAULT 'nebula-legacy',
    start_time          TIMESTAMP_NTZ    DEFAULT CURRENT_TIMESTAMP()
);

LIST @devcostdata/config/pcode_split/ PATTERN = '.*\\.csv';

COPY INTO ANALYSECOST.RAW.nebula_pcode_split (
    Azure_Budget_Group,
    Pcode,
    Project_Code,
    Allocation,
    share,
    __START_AT,
    file_name,
    system_name,
    start_time
)
FROM (
    SELECT
        $1::VARCHAR,          -- Azure_Budget_Group
        $2::VARCHAR,          -- Pcode
        $3::VARCHAR,          -- Project_Code
        $4::VARCHAR,          -- Allocation
        $5::VARCHAR,          -- share
        $6::VARCHAR,          -- __START_AT
        METADATA$FILENAME,
        'nebula-legacy',
        CURRENT_TIMESTAMP()
    FROM @devcostdata/config/pcode_split
    (FILE_FORMAT => 'raw.csv', PATTERN => '.*\\.csv')
)
ON_ERROR = CONTINUE;

-- 5. Subscription Types (source: config/subscription_types)
CREATE OR REPLACE TABLE RAW.nebula_subscriptions_type (
    Environment         VARCHAR,
    Description_Purpose VARCHAR,
    Subscription_name   VARCHAR,
    __START_AT          VARCHAR,
    subscriptions       VARCHAR,
    -- metadata
    file_name           VARCHAR,
    system_name         VARCHAR          DEFAULT 'nebula-legacy',
    start_time          TIMESTAMP_NTZ    DEFAULT CURRENT_TIMESTAMP()
);

LIST @devcostdata/config/subscription/ PATTERN = '.*\\.csv';

COPY INTO ANALYSECOST.RAW.nebula_subscriptions_type (
    Environment,
    Description_Purpose,
    Subscription_name,
    __START_AT,
    subscriptions,
    file_name,
    system_name,
    start_time
)
FROM (
    SELECT
        $1::VARCHAR,          -- Environment
        $2::VARCHAR,          -- Description_Purpose
        $3::VARCHAR,          -- Subscription_name
        $4::VARCHAR,          -- __START_AT
        $5::VARCHAR,          -- subscriptions
        METADATA$FILENAME,
        'nebula-legacy',
        CURRENT_TIMESTAMP()
    FROM @devcostdata/config/subscription
    (FILE_FORMAT => 'raw.csv', PATTERN => '.*\\.csv')
)
ON_ERROR = CONTINUE;


---enable public-private key for dbt to authenticate ---

ALTER USER dbt 
SET RSA_PUBLIC_KEY='MII...B';

DESCRIBE USER dbt;


---after dbt run ---
--drop schema staging;
--drop schema intermediate;

desc table raw.nebula_finance;

----- get rowcount for it_product -- scd type 1 ---
select 'nebula_it_products', count(1) from raw.nebula_it_products
union all
select 'stg_it_products' ,count(1) from staging.stg_it_products
union all
select 'dim_it_products' , count(1) from intermediate.dim_it_products;

select * from raw.nebula_it_products;           -- raw append only on error - continue
select * from staging.stg_it_products;          -- view
select * from intermediate.dim_it_products;     -- scd type 1 with deduplicate with surrogate key


--- get details for finance  -- scd type 2 ---
select * from raw.nebula_finance;               -- raw append only on error - continue
select * from STAGING.STG_FINANCE_PARAMS;       --view
select * from INTERMEDIATE.DIM_FINANCE_PARAM_TIMELINE;  --scd type 2 with surrogate key

-- SELECT * FROM analysecost.intermediate.finance_snapshot;

-- drop view if exists staging.stg_it_products;
-- drop table if exists intermediate.dim_it_products;

-- drop table if exists ANALYSECOST.INTERMEDIATE.DIM_FINANCE_PARAM_TIMELINE;
-- DROP TABLE IF EXISTS INTERMEDIATE.SNAP_FINANCE_PARAM_TIMELINE;
-- DROP TABLE IF EXISTS INTERMEDIATE.INT_FINANCE_PARAM_TIMELINE;
-- DROP TABLE IF EXISTS intermediate.dim_finance_history;
-- DROP TABLE IF EXISTS intermediate.finance_snapshot;
-- Drop View IF EXISTS staging.stg_nebula_finance;
-- drop table if exists intermediate.dim_finance_history2;
-- drop task ANALYSECOST.INTERMEDIATE.RUN_FINANCE_PIPELINE_TASK;

--drop view staging.stg_nebula_finance;
--drop table intermediate.dim_finance_history;



SELECT
    PARAMETER,
    PARAMETER_VALUE,
    BUSINESS_VALID_FROM,
    BUSINESS_VALID_TO,
    SOURCE_FILE_NAME,
    SOURCE_INGESTED_AT,
    IS_CURRENT
FROM ANALYSECOST.INTERMEDIATE.INT_FINANCE_PARAM_TIMELINE
WHERE PARAMETER = 'RICost'
ORDER BY BUSINESS_VALID_FROM;

SELECT
    PARAMETER,
    PARAMETER_VALUE,
    BUSINESS_VALID_FROM,
    BUSINESS_VALID_TO,
    DBT_VALID_FROM,
    DBT_VALID_TO,
    SOURCE_FILE_NAME
FROM ANALYSECOST.INTERMEDIATE.SNAP_FINANCE_PARAM_TIMELINE
WHERE PARAMETER = 'RICost'
ORDER BY BUSINESS_VALID_FROM, DBT_VALID_FROM;