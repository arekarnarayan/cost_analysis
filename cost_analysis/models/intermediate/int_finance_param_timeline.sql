
-- models/intermediate/int_finance_param_timeline.sql

{{
    config(
        materialized = 'table',
        schema = 'INTERMEDIATE',
        alias = 'DIM_FINANCE_PARAM_TIMELINE'
    )
}}

WITH raw_data AS (

    SELECT
        NULLIF(TRIM(PARAMETER), '') AS PARAMETER,
        TRY_TO_DECIMAL(
            REPLACE(TRIM(VALUE), ',', ''),
            38,
            10
        ) AS PARAMETER_VALUE,
        TRIM(VALUE) AS SOURCE_VALUE,

        -- Set the format explicitly to match the actual source files.
        TRY_TO_DATE(
            TRIM(LOAD_DATE),
            'YYYY-MM-DD'
        ) AS BUSINESS_VALID_FROM,

        FILE_NAME AS SOURCE_FILE_NAME,
        START_TIME AS SOURCE_INGESTED_AT

    FROM {{ source('raw', 'nebula_finance') }}

),

valid_data AS (

    SELECT *
    FROM raw_data
    WHERE PARAMETER IS NOT NULL
      AND BUSINESS_VALID_FROM IS NOT NULL
      AND SOURCE_VALUE IS NOT NULL
      AND PARAMETER_VALUE IS NOT NULL

),

deduplicated AS (

    SELECT *
    FROM valid_data

    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY PARAMETER, BUSINESS_VALID_FROM
        ORDER BY
            SOURCE_INGESTED_AT DESC NULLS LAST,
            SOURCE_FILE_NAME DESC,
            SOURCE_VALUE DESC
    ) = 1

),

timeline AS (

    SELECT
        PARAMETER,
        PARAMETER_VALUE,
        SOURCE_VALUE,
        BUSINESS_VALID_FROM,

        LEAD(BUSINESS_VALID_FROM) OVER (
            PARTITION BY PARAMETER
            ORDER BY BUSINESS_VALID_FROM
        ) AS BUSINESS_VALID_TO,

        SOURCE_FILE_NAME,
        SOURCE_INGESTED_AT

    FROM deduplicated

)

SELECT
    {{ dbt_utils.generate_surrogate_key([
        'PARAMETER',
        'BUSINESS_VALID_FROM'
    ]) }} AS FINANCE_PARAM_KEY,

    PARAMETER,
    PARAMETER_VALUE,
    SOURCE_VALUE,
    BUSINESS_VALID_FROM,
    BUSINESS_VALID_TO,
    SOURCE_FILE_NAME,
    SOURCE_INGESTED_AT,

    BUSINESS_VALID_TO IS NULL AS IS_LATEST_KNOWN,

    CURRENT_TIMESTAMP() AS DBT_PROCESSED_AT

FROM timeline
