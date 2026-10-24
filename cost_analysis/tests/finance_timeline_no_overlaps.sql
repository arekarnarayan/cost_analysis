WITH ordered_timeline AS (

    SELECT
        PARAMETER,
        BUSINESS_VALID_FROM,
        BUSINESS_VALID_TO,

        LEAD(BUSINESS_VALID_FROM) OVER (
            PARTITION BY PARAMETER
            ORDER BY BUSINESS_VALID_FROM
        ) AS NEXT_VALID_FROM

    FROM {{ ref('int_finance_param_timeline') }}

)

SELECT *
FROM ordered_timeline
WHERE BUSINESS_VALID_TO IS DISTINCT FROM NEXT_VALID_FROM