
{% snapshot snap_finance_param_timeline %}

{{
    config(
        schema = 'INTERMEDIATE',
        unique_key = 'FINANCE_PARAM_KEY',
        strategy = 'check',
        check_cols = [
            'PARAMETER_VALUE',
            'SOURCE_VALUE',
            'BUSINESS_VALID_FROM',
            'BUSINESS_VALID_TO',
            'SOURCE_FILE_NAME',
            'SOURCE_INGESTED_AT'
        ],
        hard_deletes = 'invalidate'
    )
}}

SELECT
    FINANCE_PARAM_KEY,
    PARAMETER,
    PARAMETER_VALUE,
    SOURCE_VALUE,
    BUSINESS_VALID_FROM,
    BUSINESS_VALID_TO,
    SOURCE_FILE_NAME,
    SOURCE_INGESTED_AT
FROM {{ ref('int_finance_param_timeline') }}

{% endsnapshot %}
