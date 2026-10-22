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




