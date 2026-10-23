{{ config(materialized='view', schema='staging') }}

SELECT
    PARAMETER,
    -- Remove commas from the string before attempting to cast to FLOAT
    TRY_CAST(REPLACE(VALUE, ',', '') AS FLOAT) AS VALUE,-- Cast string value to numeric if needed
    TO_DATE(LOAD_DATE) AS load_date,
    FILE_NAME,
    SYSTEM_NAME,
    START_TIME
FROM {{ source('raw', 'nebula_finance') }}