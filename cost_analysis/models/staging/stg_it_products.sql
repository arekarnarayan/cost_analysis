{{ config (materialized='view', schema='staging') }}

SELECT
    -- cast or rename columns from your raw layer
    Purpose AS Purpose,
    Pcode AS Pcode,
    Team AS Team,
    Team_Code AS Team_Code,

    -- ...
FROM {{ source('raw', 'nebula_it_products') }}