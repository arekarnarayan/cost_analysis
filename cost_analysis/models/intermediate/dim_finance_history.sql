{# {{
    config(
        materialized='incremental',
        schema='intermediate',
        unique_key='finance_sk',
        pre_hook="""
            {% if is_incremental() %}
                UPDATE {{ this }} AS target
                SET target.valid_to = incoming.load_date,
                    target.is_current = FALSE
                FROM (
                    SELECT DISTINCT parameter, TO_DATE(load_date) AS load_date
                    FROM {{ ref('stg_nebula_finance') }}
                    WHERE TO_DATE(load_date) > (SELECT COALESCE(MAX(valid_from), '1900-01-01') FROM {{ this }})
                ) AS incoming
                WHERE target.parameter = incoming.parameter
                  AND target.is_current = TRUE
                  AND target.valid_from < incoming.load_date;
            {% endif %}
        """
    )
}}

WITH incoming_data AS (
    SELECT 
        parameter,
        TRY_CAST(REPLACE(value, ',', '') AS FLOAT) AS value,
        TO_DATE(load_date) AS load_date
    FROM {{ ref('stg_nebula_finance') }}

    {% if is_incremental() %}
    WHERE TO_DATE(load_date) > (SELECT COALESCE(MAX(valid_from), '1900-01-01') FROM {{ this }})
    {% endif %}
)

SELECT 
    {{ dbt_utils.generate_surrogate_key(['parameter', 'load_date']) }} AS finance_sk,
    parameter,
    value,
    load_date AS valid_from,
    '9999-12-31'::DATE AS valid_to,
    TRUE AS is_current
FROM incoming_data #}




{{
    config(
        materialized='table',
        schema='intermediate'
    )
}}

WITH ranked_finance AS (
    SELECT
        parameter,
        value,
        load_date,
        file_name,
        -- Look ahead to find the next load_date for the same parameter to establish history boundaries
        LEAD(load_date) OVER (
            PARTITION BY parameter 
            ORDER BY load_date ASC
        ) AS next_load_date
    FROM {{ ref('stg_nebula_finance') }}
),

add_history_bounds AS (
    SELECT
        parameter,
        value,
        load_date AS valid_from,
        -- If there is a next load date, valid_to is just before it. Otherwise, it's open-ended (9999-12-31).
        COALESCE(next_load_date, '9999-12-31'::DATE) AS valid_to,
        -- Flag the current active record
        CASE 
            WHEN next_load_date IS NULL THEN TRUE 
            ELSE FALSE 
        END AS is_current,
        file_name
    FROM ranked_finance
)

SELECT
    -- Unique Surrogate Key for this specific historical version row
    {{ dbt_utils.generate_surrogate_key(['parameter', 'valid_from']) }} AS finance_sk,
    parameter,
    value,
    valid_from,
    valid_to,
    is_current
FROM add_history_bounds