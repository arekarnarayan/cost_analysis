-- models/staging/stg_it_products.sql

{{ config(materialized='view', schema='STAGING') }}

SELECT
    TRIM(PURPOSE) AS PURPOSE,
    TRIM(PCODE) AS PCODE,
    TRIM(TEAM) AS TEAM,
    TRIM(TEAM_CODE) AS TEAM_CODE,
    FILE_NAME,
    START_TIME AS SOURCE_INGESTED_AT
FROM {{ source('raw', 'nebula_it_products') }}