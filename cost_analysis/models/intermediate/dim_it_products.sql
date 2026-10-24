-- models/intermediate/dim_it_products.sql

{{
    config(
        materialized='incremental',
        schema='INTERMEDIATE',
        unique_key='PCODE',
        incremental_strategy='merge'
    )
}}

WITH source_data AS (

    SELECT
        PCODE,
        PURPOSE,
        TEAM,
        TEAM_CODE,
        FILE_NAME,
        SOURCE_INGESTED_AT

    FROM {{ ref('stg_it_products') }}

    {% if is_incremental() %}
    WHERE SOURCE_INGESTED_AT >= (
        SELECT COALESCE(
            DATEADD(
                second,
                -5,
                MAX(SOURCE_INGESTED_AT)
            ),
            '1900-01-01'::TIMESTAMP_NTZ
        )
        FROM {{ this }}
    )
    {% endif %}

),

deduplicated AS (

    SELECT *
    FROM source_data

    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY PCODE
        ORDER BY
            SOURCE_INGESTED_AT DESC NULLS LAST,
            FILE_NAME DESC,
            PURPOSE DESC,
            TEAM_CODE DESC
    ) = 1

)

SELECT
    {{ dbt_utils.generate_surrogate_key(['PCODE']) }}
        AS IT_PRODUCT_SK,
    PCODE,
    PURPOSE,
    TEAM,
    TEAM_CODE,
    FILE_NAME,
    SOURCE_INGESTED_AT,
    CURRENT_TIMESTAMP() AS DBT_PROCESSED_AT

FROM deduplicated