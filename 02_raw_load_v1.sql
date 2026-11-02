

USE ROLE LOADER_ROLE;
USE DATABASE ANALYSECOST;


-- SET load_id = UUID_STRING();

-- INSERT INTO AUDIT.LOAD_BATCH (load_id, source_name, stage_name, file_pattern, status)
-- SELECT $load_id, 'PCF', '@RAW.devcostdata/pcf/', '.*\\.parquet', 'RUNNING';

-- COPY INTO RAW.PCF_RAW (
--     load_id, source_system, source_file_name,
--     source_file_row_number, source_file_modified_at, record_payload
-- )
-- FROM (
--     SELECT
--         $load_id,
--         'nebula-legacy',
--         METADATA$FILENAME,
--         METADATA$FILE_ROW_NUMBER,
--         METADATA$FILE_LAST_MODIFIED,
--         $1                                   -- whole Parquet row as VARIANT
--     FROM @RAW.devcostdata/pcf/
--          (FILE_FORMAT => 'RAW.PARQUET', PATTERN => '.*\\.parquet')
-- )
-- ON_ERROR = 'SKIP_FILE'      -- a corrupt file is rejected alone; the rest load
-- FORCE = FALSE;              -- Snowflake load metadata prevents duplicate file loads

-- -- Capture per-file results for this batch
-- INSERT INTO AUDIT.FILE_LOAD_LOG (
--     load_id, file_name, stage_location, file_size, status,
--     row_parsed, row_loaded, error_count,
--     first_error_message, first_error_line, last_load_time
-- )
-- SELECT
--     $load_id, FILE_NAME, STAGE_LOCATION, FILE_SIZE, STATUS,
--     ROW_PARSED, ROW_COUNT, ERROR_COUNT,
--     FIRST_ERROR_MESSAGE, FIRST_ERROR_LINE_NUMBER, LAST_LOAD_TIME
-- FROM TABLE(ANALYSECOST.INFORMATION_SCHEMA.COPY_HISTORY(
--         TABLE_NAME => 'PCF_RAW',
--         START_TIME => DATEADD('hour', -2, CURRENT_TIMESTAMP())))
-- WHERE LAST_LOAD_TIME >= (SELECT started_at FROM AUDIT.LOAD_BATCH WHERE load_id = $load_id);

-- -- Close the batch
-- UPDATE AUDIT.LOAD_BATCH b
-- SET finished_at  = CURRENT_TIMESTAMP(),
--     status       = CASE
--                      WHEN f.files_failed = 0 THEN 'SUCCESS'
--                      WHEN f.files_loaded = 0 THEN 'FAILED'
--                      ELSE 'PARTIAL'
--                    END,
--     files_seen   = f.files_seen,
--     files_loaded = f.files_loaded,
--     files_failed = f.files_failed,
--     row_total    = f.row_total
-- FROM (
--     SELECT
--         COUNT(*)                          AS files_seen,
--         COUNT_IF(status = 'Loaded')       AS files_loaded,
--         COUNT_IF(status <> 'Loaded')      AS files_failed,
--         SUM(row_loaded)                   AS row_total
--     FROM AUDIT.FILE_LOAD_LOG
--     WHERE load_id = $load_id
-- ) f
-- WHERE b.load_id = $load_id;

select * from AUDIT.LOAD_RUN ;

CREATE OR REPLACE PROCEDURE AUDIT.SP_LOAD_RAW_MANUAL(P_SOURCE VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS
DECLARE
    res       RESULTSET;
    v_summary VARCHAR DEFAULT '';
BEGIN
    -- Query is executed within BEGIN block where :P_SOURCE is properly bound
    res := (
        SELECT source_name, source_prefix, file_pattern, file_format,
               target_table, target_columns, select_list
        FROM AUDIT.LOAD_SOURCE_CONFIG
        WHERE enabled
          AND ((UPPER(:P_SOURCE) <> 'ALL' AND UPPER(source_name) = UPPER(:P_SOURCE))
               OR (UPPER(:P_SOURCE) = 'ALL' AND load_mode = 'MANUAL'))
    );

    -- Loop directly through the RESULTSET
    FOR s IN res DO
        LET v_src     VARCHAR := s.source_name;
        LET v_prefix  VARCHAR := s.source_prefix;
        LET v_pattern VARCHAR := s.file_pattern;
        LET v_format  VARCHAR := s.file_format;
        LET v_target  VARCHAR := s.target_table;
        LET v_cols    VARCHAR := s.target_columns;
        LET v_select  VARCHAR := s.select_list;
        LET v_run_id  VARCHAR := UUID_STRING();
        LET v_started TIMESTAMP_NTZ := CURRENT_TIMESTAMP();
        LET v_files   NUMBER DEFAULT 0;
        LET v_ok      NUMBER DEFAULT 0;
        LET v_bad     NUMBER DEFAULT 0;
        LET v_rows    NUMBER DEFAULT 0;
        LET v_status  VARCHAR DEFAULT 'RUNNING';
        LET v_err     VARCHAR DEFAULT '';
        LET v_sql     VARCHAR;

        INSERT INTO AUDIT.LOAD_RUN (run_id, source_name, target_table, load_path, started_at, status)
        VALUES (:v_run_id, :v_src, :v_target, 'MANUAL', :v_started, 'RUNNING');

        BEGIN
            -- Valid COPY INTO transformation syntax
            v_sql := 'COPY INTO ' || v_target || ' (' || v_cols
                  || ', source_system, load_id, source_file_name, source_file_row_number, loaded_at) '
                  || 'FROM (SELECT ' || v_select
                  || ', ''nebula-legacy'', ''' || v_run_id
                  || ''', METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, CURRENT_TIMESTAMP() '
                  || 'FROM @RAW.DEVCOSTDATA/' || v_prefix || ') '
                  || 'FILE_FORMAT = (FORMAT_NAME = ''' || v_format || ''') '
                  || 'PATTERN = ''' || v_pattern || ''' '
                  || 'ON_ERROR = ''SKIP_FILE''';

            EXECUTE IMMEDIATE :v_sql;


            -- Gather execution results
            SELECT COUNT(*), 
                   COUNT_IF(status = 'LOADED'), 
                   COUNT_IF(status <> 'LOADED'), 
                   COALESCE(SUM(row_loaded), 0)
              INTO :v_files, :v_ok, :v_bad, :v_rows
            FROM AUDIT.FILE_LEDGER
            WHERE target_table = :v_target
              AND load_path = 'MANUAL'
              AND attempted_at >= :v_started;

            v_status := CASE WHEN :v_files = 0 THEN 'NO_NEW_FILES'
                             WHEN :v_bad = 0   THEN 'SUCCESS'
                             WHEN :v_ok = 0    THEN 'FAILED'
                             ELSE 'PARTIAL' END;

        EXCEPTION
            WHEN OTHER THEN
                v_err    := SQLERRM;
                v_status := 'ERROR';
                
                -- CALL SYSTEM$SEND_EMAIL(
                --     'PCF_EMAIL_INT', 
                --     'ops-team@example.com',
                --     'Snowflake manual load ERROR: ' || :v_src,
                --     'Run ' || :v_run_id || ': ' || :v_err
                -- );
        END;

        UPDATE AUDIT.LOAD_RUN
        SET finished_at   = CURRENT_TIMESTAMP(), 
            status        = :v_status,
            files_loaded  = :v_ok, 
            files_failed  = :v_bad, 
            rows_loaded   = :v_rows,
            error_message = :v_err
        WHERE run_id = :v_run_id;

        v_summary := v_summary || v_src || '=' || v_status || ' ';
    END FOR;

    RETURN TRIM(v_summary);
END;




CREATE OR REPLACE PROCEDURE AUDIT.SP_SYNC_LEDGER(P_LOOKBACK_HOURS NUMBER)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS
DECLARE
    v_started TIMESTAMP_NTZ;
    v_new     NUMBER DEFAULT 0;
    v_failed  NUMBER DEFAULT 0;
    v_body    VARCHAR;
BEGIN
    v_started := CURRENT_TIMESTAMP();

    INSERT INTO AUDIT.FILE_LEDGER (
        target_table, file_name, load_path, attempted_at, status, raw_status,
        file_size, row_parsed, row_loaded, error_count,
        first_error_message, first_error_line, first_error_column,
        pipe_received_at, first_commit_at, ingest_latency_sec)
    SELECT
        v.target_table,
        v.FILE_NAME,
        IFF(v.PIPE_NAME IS NULL, 'MANUAL', 'PIPE'),
        v.LAST_LOAD_TIME,
        CASE v.STATUS WHEN 'Loaded' THEN 'LOADED'
                      WHEN 'Partially loaded' THEN 'PARTIAL'
                      ELSE 'FAILED' END,
        v.STATUS, v.FILE_SIZE, v.ROW_PARSED, v.ROW_COUNT, v.ERROR_COUNT,
        v.FIRST_ERROR_MESSAGE, v.FIRST_ERROR_LINE_NUMBER, v.FIRST_ERROR_COLUMN_NAME,
        v.PIPE_RECEIVED_TIME, v.FIRST_COMMIT_TIME,
        DATEDIFF('second', v.PIPE_RECEIVED_TIME, v.FIRST_COMMIT_TIME)
    FROM AUDIT.V_COPY_HISTORY_RAW v
    WHERE v.LAST_LOAD_TIME >= DATEADD('hour', -1 * :P_LOOKBACK_HOURS, CURRENT_TIMESTAMP())
      AND NOT EXISTS (
          SELECT 1 FROM AUDIT.FILE_LEDGER l
          WHERE l.target_table = v.target_table
            AND l.file_name = v.FILE_NAME
            AND l.attempted_at = v.LAST_LOAD_TIME);

    SELECT COUNT(*), COUNT_IF(status <> 'LOADED')
      INTO :v_new, :v_failed
    FROM AUDIT.FILE_LEDGER
    WHERE synced_at >= :v_started;

    IF (v_failed > 0) THEN
        SELECT LISTAGG(target_table || ' | ' || file_name || ' | ' || status || ' | '
                       || COALESCE(first_error_message, ''), CHR(10))
               WITHIN GROUP (ORDER BY attempted_at)
          INTO :v_body
        FROM AUDIT.FILE_LEDGER
        WHERE synced_at >= :v_started AND status <> 'LOADED';

        CALL SYSTEM$SEND_EMAIL('PCF_EMAIL_INT', 'ops-team@example.com',
             'Snowflake RAW load: ' || v_failed || ' file(s) failed', v_body);
    END IF;

    RETURN 'synced=' || v_new || ' failed=' || v_failed;
END;


select * from AUDIT.LOAD_SOURCE_CONFIG;

-- One source
CALL AUDIT.SP_LOAD_RAW_MANUAL('FINANCE_PARAM');

SELECT * 
FROM AUDIT.LOAD_RUN 
ORDER BY started_at DESC;

SELECT * 
FROM AUDIT.FILE_LEDGER 
ORDER BY attempted_at DESC;

SHOW STAGES LIKE '%DEVCOSTDATA%' IN ACCOUNT;

SHOW PROCEDURES LIKE '%SYNC_LEDGER%' IN ACCOUNT;

select * from raw.nebula_finance; 