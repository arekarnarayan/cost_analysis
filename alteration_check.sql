USE ROLE LOADER_ROLE;
USE DATABASE ANALYSECOST;


select * from AUDIT.LOAD_SOURCE_CONFIG;

update AUDIT.LOAD_SOURCE_CONFIG set SOURCE_PREFIX = 'config/subscription/' where SOURCE_NAME = 'SUBSCRIPTIONS';

-- 1. Check if Snowflake can see any files in that specific stage folder
SELECT 
    RELATIVE_PATH, 
    SIZE, 
    LAST_MODIFIED
FROM DIRECTORY(@RAW.DEVCOSTDATA)
WHERE RELATIVE_PATH ILIKE 'config/subscription/%';

SELECT 
    RELATIVE_PATH, 
    SIZE, 
    LAST_MODIFIED
FROM DIRECTORY(@RAW.DEVCOSTDATA)
WHERE RELATIVE_PATH ILIKE 'config/it_product/%';

-- 2. Check if the table ALREADY has data from a previous load
SELECT COUNT(*) AS current_rows, MAX(loaded_at) AS last_loaded_timestamp 
FROM RAW.NEBULA_SUBSCRIPTIONS_TYPE;

-- 3. Check Snowflake's internal load history for this table (did it load earlier?)
SELECT 
    FILE_NAME, 
    STATUS, 
    ROW_COUNT, 
    LAST_LOAD_TIME
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'RAW.NEBULA_SUBSCRIPTIONS_TYPE', 
    START_TIME => DATEADD('day', -7, CURRENT_TIMESTAMP())
));