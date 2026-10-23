{% snapshot finance_snapshot %}

{{
    config(
        target_database='analysecost',
        target_schema='intermediate',
        unique_key="parameter",
        strategy='check',
        check_cols=['value']
    )
}}

SELECT 
    parameter,
    TRY_CAST(REPLACE(value, ',', '') AS FLOAT) AS value,
    load_date
FROM {{ source('raw', 'nebula_finance') }}

{% endsnapshot %}