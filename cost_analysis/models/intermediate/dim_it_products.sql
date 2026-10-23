{{
    config(
        materialized='incremental',
        schema='intermediate',
        unique_key='Pcode',
        incremental_strategy='merge'
    )
}}

WITH source_data AS (
    SELECT 
        Pcode, 
        Purpose,
        team,
        Team_Code,
        CURRENT_TIMESTAMP() AS updated_at
    FROM {{ ref('stg_it_products') }}
    
    {% if is_incremental() %}
    -- Optional optimization: only pull records from staging that are newer than the max updated_at in the target table
    WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }})
    {% endif %}
),

deduplicated AS (
    SELECT 
        Pcode,
        Purpose,
        team,
        Team_Code,
        updated_at,
        ROW_NUMBER() OVER (
            PARTITION BY Pcode 
            ORDER BY updated_at DESC
        ) as rn
    FROM source_data
)

SELECT
    -- Generate a unique surrogate key MD5 hash using the business key
    {{ dbt_utils.generate_surrogate_key(['Pcode']) }} AS it_product_sk,
    Pcode,
    Purpose,
    team,
    Team_Code,
    updated_at
FROM deduplicated
WHERE rn = 1


-- {{ config (materialized='table', schema='intermediate', unique_key='Pcode') }}

-- WITH source_data AS (
--     SELECT 
--         -- Select columns from your staging layer
--         Pcode, -- Make sure this is your unique business key
--         Purpose,
--         team,
--         Team_Code,
--         -- Add a metadata timestamp for when it was processed
--         CURRENT_TIMESTAMP() AS updated_at
--     FROM {{ ref('stg_it_products') }}
-- ),

-- -- Optional: Deduplicate source data in case staging has multiple records per product_id
-- deduplicated AS (
--     SELECT 
--         *,
--         ROW_NUMBER() OVER (
--             PARTITION BY Pcode 
--             ORDER BY updated_at DESC
--         ) as rn
--     FROM source_data
-- )

-- SELECT 
--     Pcode,
--     Purpose,
--     team,
--     team_code,
--     updated_at
-- FROM deduplicated
-- WHERE rn = 1