-- models/staging/stg_finance_params.sql

{{ config(materialized='view') }}

SELECT
        TRIM(PARAMETER) AS PARAMETER,
        TRY_CAST(REPLACE(value, ',', '') AS FLOAT) AS PARAMETER_VALUE,   --TRY_TO_DECIMAL(TRIM(VALUE), 38, 10) AS PARAMETER_VALUE,
        TRIM(VALUE) AS SOURCE_VALUE,
        TRY_TO_DATE(LOAD_DATE) AS BUSINESS_VALID_FROM,
        FILE_NAME AS SOURCE_FILE_NAME,
        SYSTEM_NAME,
        START_TIME AS SOURCE_INGESTED_AT
FROM {{ source('raw', 'nebula_finance') }}