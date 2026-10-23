USE ROLE ACCOUNTADMIN;

-- Ensure TRANSFORM role can create tasks in the intermediate schema
GRANT EXECUTE TASK ON ACCOUNT TO ROLE TRANSFORM;
GRANT CREATE TASK ON SCHEMA analysecost.intermediate TO ROLE TRANSFORM;
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE TRANSFORM;


USE ROLE TRANSFORM;
USE DATABASE analysecost;
USE SCHEMA intermediate;


-- 1. Create the target table with correct data types
CREATE OR REPLACE TABLE analysecost.intermediate.dim_finance_history2 (
    finance_sk VARCHAR,
    parameter VARCHAR,
    value FLOAT,
    valid_from DATE,
    valid_to DATE,
    is_current BOOLEAN
);



CREATE OR REPLACE PROCEDURE run_cost_analysis_pipeline()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    row_count INTEGER;
BEGIN

    -- Check if the target table is empty
    SELECT COUNT(*) INTO :row_count
    FROM analysecost.intermediate.dim_finance_history2;

    ----------------------------------------------------------------
    -- CASE 1: Table is empty → Full history rebuild
    ----------------------------------------------------------------
    IF (row_count = 0) THEN

        INSERT INTO analysecost.intermediate.dim_finance_history2 (
            finance_sk,
            parameter,
            value,
            valid_from,
            valid_to,
            is_current
        )
        WITH ordered AS (
            SELECT
                parameter,
                TRY_CAST(REPLACE(value, ',', '') AS FLOAT) AS value,
                TO_DATE(load_date) AS valid_from,
                LEAD(TO_DATE(load_date)) OVER (
                    PARTITION BY parameter 
                    ORDER BY TO_DATE(load_date)
                ) AS next_valid_from
            FROM analysecost.staging.stg_nebula_finance
            WHERE value IS NOT NULL
              AND load_date IS NOT NULL
        )
        SELECT
            MD5(COALESCE(parameter::VARCHAR,'') || '-' || COALESCE(valid_from::VARCHAR,'')) AS finance_sk,
            parameter,
            value,
            valid_from,
            COALESCE(next_valid_from, '9999-12-31'::DATE) AS valid_to,
            CASE WHEN next_valid_from IS NULL THEN TRUE ELSE FALSE END AS is_current
        FROM ordered;

        RETURN 'Full SCD2 history rebuild completed (table was empty)';

    ----------------------------------------------------------------
    -- CASE 2: Table has data → Incremental load
    ----------------------------------------------------------------
    ELSE

        -- Close old current records
        UPDATE analysecost.intermediate.dim_finance_history2 AS target
        SET 
            target.valid_to   = src.new_valid_from,
            target.is_current = FALSE
        FROM (
            SELECT 
                parameter,
                TO_DATE(load_date) AS new_valid_from
            FROM analysecost.staging.stg_nebula_finance
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY parameter 
                ORDER BY TO_DATE(load_date) DESC
            ) = 1
        ) src
        WHERE target.parameter  = src.parameter
          AND target.is_current = TRUE
          AND target.valid_from < src.new_valid_from;

        -- Insert only brand-new versions
        INSERT INTO analysecost.intermediate.dim_finance_history2 (
            finance_sk,
            parameter,
            value,
            valid_from,
            valid_to,
            is_current
        )
        SELECT
            MD5(COALESCE(parameter::VARCHAR,'') || '-' || COALESCE(TO_DATE(load_date)::VARCHAR,'')) AS finance_sk,
            parameter,
            TRY_CAST(REPLACE(value, ',', '') AS FLOAT) AS value,
            TO_DATE(load_date) AS valid_from,
            '9999-12-31'::DATE AS valid_to,
            TRUE AS is_current
        FROM analysecost.staging.stg_nebula_finance s
        WHERE TO_DATE(s.load_date) = (
                SELECT MAX(TO_DATE(load_date))
                FROM analysecost.staging.stg_nebula_finance s2
                WHERE s2.parameter = s.parameter
              )
          AND NOT EXISTS (
                SELECT 1 
                FROM analysecost.intermediate.dim_finance_history2 t
                WHERE t.parameter  = s.parameter
                  AND t.valid_from = TO_DATE(s.load_date)
              );

        RETURN 'Incremental SCD2 load completed successfully';
    END IF;

END;
$$;

-- Create task specifying the schema explicitly
CREATE OR REPLACE TASK analysecost.intermediate.run_finance_pipeline_task
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = 'USING CRON 0 0 * * * UTC'
AS
  CALL run_cost_analysis_pipeline();

-- Resume the task
ALTER TASK analysecost.intermediate.run_finance_pipeline_task RESUME;


SELECT *
FROM TABLE(information_schema.task_history(
    task_name => 'RUN_FINANCE_PIPELINE_TASK'
))
ORDER BY scheduled_time DESC;

CALL run_cost_analysis_pipeline();

SELECT * FROM analysecost.intermediate.dim_finance_history2;



-- ---How to clean the current bad data first
-- -- Keep only the latest version per parameter as current
-- CREATE OR REPLACE TEMP TABLE tmp_latest AS
-- SELECT *
-- FROM analysecost.intermediate.dim_finance_history2
-- QUALIFY ROW_NUMBER() OVER (
--     PARTITION BY parameter 
--     ORDER BY valid_from DESC
-- ) = 1;

-- -- Truncate and reload clean data
-- TRUNCATE TABLE analysecost.intermediate.dim_finance_history2;

-- INSERT INTO analysecost.intermediate.dim_finance_history2
-- SELECT 
--     finance_sk,
--     parameter,
--     value,
--     valid_from,
--     '9999-12-31'::DATE AS valid_to,
--     TRUE AS is_current
-- FROM tmp_latest;