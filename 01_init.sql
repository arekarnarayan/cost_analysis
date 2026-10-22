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


-- 1. Create the storage integration (run once by ACCOUNTADMIN)
use role accountadmin;

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
CREATE OR replace STAGE devcostdata
  URL = 'azure://stazurecostanalysis1.blob.core.windows.net/devcostdata/'
  STORAGE_INTEGRATION = project3_adls_integration;

-- Show all stages
SHOW STAGES LIKE 'devcostdata';

-- List with pattern (e.g. only Parquet files)
LIST @devcostdata PATTERN = '.*\.parquet';

Use database analysecost;

--3. file format
CREATE FILE FORMAT IF NOT EXISTS raw.parquet
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

--4. create raw table
CREATE TABLE IF NOT EXISTS analysecost.RAW.pcf_data(
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
    system_name             VARCHAR DEFAULT 'nebula-legacy',
    start_time              TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

drop table analysecost.raw.pcf_data;

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
COPY INTO analysecost.RAW.pcf_data (
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
    system_name,          -- force the default
    start_time            -- force the default
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
        'nebula-legacy',                    -- explicit default
        CURRENT_TIMESTAMP()                 -- explicit default
    FROM @devcostdata/pcf/
    (FILE_FORMAT => 'raw.parquet', PATTERN => '.*\\.parquet')
);

select count(1) from analysecost.RAW.pcf_data;

desc TABLE analysecost.RAW.pcf_data;