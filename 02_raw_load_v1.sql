

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


-- AUDIT.SP_SYNC_LEDGER
-- Purpose: Synchronizes low-level copy events from AUDIT.V_COPY_HISTORY_RAW into the primary audit table AUDIT.FILE_LEDGER.
-- Execution Rights: EXECUTE AS CALLER (required to query INFORMATION_SCHEMA.COPY_HISTORY).
-- Input Parameters:
-- P_LOOKBACK_HOURS (NUMBER): Time window (in hours) to scan backwards from CURRENT_TIMESTAMP(). Defaults to 24 hours if NULL.

-- Step-by-Step Logic:

-- Idempotent Ingestion: Evaluates historical copy records within the lookback window. Uses a NOT EXISTS composite key check (target_table, file_name, attempted_at) to guarantee zero duplicate insertions.
-- Status Normalization: Standardizes engine statuses into 'LOADED', 'PARTIAL', 'SKIPPED', 'IN_PROGRESS', or 'FAILED'.
-- Path & SLA Metrics:
-- Flags load path as 'PIPE' if PIPE_NAME IS NOT NULL, else 'MANUAL'.
-- Calculates pipe end-to-end latency in seconds: DATEDIFF('second', PIPE_RECEIVED_TIME, FIRST_COMMIT_TIME).
-- Run Summary: Returns the count of newly synced and failed files ('synced=X failed=Y').

USE ROLE LOADER_ROLE;
USE DATABASE ANALYSECOST;
USE SCHEMA AUDIT;

CREATE OR REPLACE PROCEDURE AUDIT.SP_SYNC_LEDGER(P_LOOKBACK_HOURS NUMBER)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
DECLARE
    v_hours   NUMBER;
    v_started TIMESTAMP_NTZ;
    v_new     NUMBER DEFAULT 0;
    v_failed  NUMBER DEFAULT 0;
BEGIN
    v_started := CURRENT_TIMESTAMP();
    v_hours   := COALESCE(:P_LOOKBACK_HOURS, 24);

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
        CASE UPPER(v.STATUS) 
            WHEN 'LOADED'           THEN 'LOADED'
            WHEN 'PARTIALLY LOADED' THEN 'PARTIAL'
            WHEN 'LOAD SKIPPED'     THEN 'SKIPPED'
            WHEN 'LOAD IN PROGRESS' THEN 'IN_PROGRESS'
            ELSE 'FAILED' 
        END,
        v.STATUS, 
        v.FILE_SIZE, 
        v.ROW_PARSED, 
        v.ROW_COUNT, 
        v.ERROR_COUNT,
        v.FIRST_ERROR_MESSAGE, 
        v.FIRST_ERROR_LINE_NUMBER, 
        v.FIRST_ERROR_COLUMN_NAME,
        v.PIPE_RECEIVED_TIME, 
        COALESCE(v.FIRST_COMMIT_TIME, v.LAST_LOAD_TIME),
        DATEDIFF('second', v.PIPE_RECEIVED_TIME, COALESCE(v.FIRST_COMMIT_TIME, v.LAST_LOAD_TIME))
    FROM AUDIT.V_COPY_HISTORY_RAW v
    WHERE v.LAST_LOAD_TIME >= DATEADD('hour', -1 * :v_hours, CURRENT_TIMESTAMP())
      AND NOT EXISTS (
          SELECT 1 FROM AUDIT.FILE_LEDGER l
          WHERE l.target_table = v.target_table
            AND l.file_name    = v.FILE_NAME
            AND l.attempted_at = v.LAST_LOAD_TIME);

    SELECT COALESCE(COUNT(*), 0), 
           COALESCE(COUNT_IF(status NOT IN ('LOADED', 'SKIPPED')), 0)
      INTO :v_new, :v_failed
    FROM AUDIT.FILE_LEDGER
    WHERE synced_at >= :v_started;

    RETURN 'synced=' || COALESCE(v_new, 0) || ' failed=' || COALESCE(v_failed, 0);
END;

-- AUDIT.SP_LOAD_RAW_MANUAL
-- Purpose: Orchestrates batch ingestion for structured CSV configuration files from external stage @RAW.DEVCOSTDATA to RAW.* tables based on entries in AUDIT.LOAD_SOURCE_CONFIG.
-- Execution Rights: EXECUTE AS CALLER (runs using LOADER_ROLE).
-- Input Parameters:
-- P_SOURCE (VARCHAR): Target source identifier (e.g., 'FINANCE_PARAM') or 'ALL' to loop through all manual feeds.

-- Step-by-Step Logic:
-- Source Discovery: Filters LOAD_SOURCE_CONFIG where enabled = TRUE and load_mode = 'MANUAL'.
-- Execution Logging: Generates a run_id (UUID) and creates an initial record in AUDIT.LOAD_RUN with status 'RUNNING'.
-- Dynamic COPY INTO: Injects standard metadata columns (source_system, load_id, source_file_name, source_file_row_number, loaded_at) alongside domain columns using ON_ERROR = 'SKIP_FILE'.
-- Synchronous Result Evaluation: Inspects the captured query ID using positional notation:
-- If $1 contains '0 files processed' 
--  Status: 'NO_NEW_FILES'.

-- If files were processed 
--  Aggregates $2 (status) and $4 (rows loaded) to evaluate 'SUCCESS', 'PARTIAL', or 'FAILED'.

-- Exception Handling: Catches runtime SQL errors at the source level, ensuring that one failing source does not abort the remaining feeds.
-- Batch Update & Sync: Updates AUDIT.LOAD_RUN with final counts and calls AUDIT.SP_SYNC_LEDGER(1).

USE ROLE LOADER_ROLE;
USE DATABASE ANALYSECOST;
USE SCHEMA AUDIT;

CREATE OR REPLACE PROCEDURE AUDIT.SP_LOAD_RAW_MANUAL(P_SOURCE VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
DECLARE
    res       RESULTSET;
    v_summary VARCHAR DEFAULT '';
BEGIN
    res := (
        SELECT source_name, source_prefix, file_pattern, file_format,
               target_table, target_columns, select_list
        FROM AUDIT.LOAD_SOURCE_CONFIG
        WHERE enabled
          AND load_mode = 'MANUAL'
          AND (UPPER(:P_SOURCE) = 'ALL' OR UPPER(source_name) = UPPER(:P_SOURCE))
    );

    FOR s IN res DO
        LET v_src       VARCHAR := s.source_name;
        LET v_prefix    VARCHAR := s.source_prefix;
        LET v_pattern   VARCHAR := s.file_pattern;
        LET v_format    VARCHAR := s.file_format;
        LET v_target    VARCHAR := s.target_table;
        LET v_cols      VARCHAR := s.target_columns;
        LET v_select    VARCHAR := s.select_list;
        LET v_run_id    VARCHAR := UUID_STRING();
        LET v_started   TIMESTAMP_NTZ := CURRENT_TIMESTAMP();
        LET v_copy_qid  VARCHAR := '';
        LET v_first_val VARCHAR := '';
        LET v_files     NUMBER DEFAULT 0;
        LET v_ok        NUMBER DEFAULT 0;
        LET v_bad       NUMBER DEFAULT 0;
        LET v_rows      NUMBER DEFAULT 0;
        LET v_status    VARCHAR DEFAULT 'RUNNING';
        LET v_err       VARCHAR DEFAULT '';
        LET v_sql       VARCHAR;

        INSERT INTO AUDIT.LOAD_RUN (run_id, source_name, target_table, load_path, started_at, status)
        VALUES (:v_run_id, :v_src, :v_target, 'MANUAL', :v_started, 'RUNNING');

        BEGIN
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
            -- Capture query ID immediately to prevent subsequent SELECTs from overwriting LAST_QUERY_ID()
            v_copy_qid := LAST_QUERY_ID();

            -- 1. Inspect first column of COPY result
            SELECT $1 INTO :v_first_val 
            FROM TABLE(RESULT_SCAN(:v_copy_qid)) 
            LIMIT 1;

            -- 2. Differentiate 0 files processed vs files loaded
            IF (v_first_val ILIKE '%0 files processed%' OR v_first_val IS NULL) THEN
                v_files  := 0;
                v_ok     := 0;
                v_bad    := 0;
                v_rows   := 0;
                v_status := 'NO_NEW_FILES';
            ELSE
                -- Positional columns bypass case sensitivity: $2 = status, $4 = rows_loaded
                SELECT 
                    COUNT(*),
                    COUNT_IF($2 = 'LOADED'),
                    COUNT_IF($2 NOT IN ('LOADED', 'LOAD SKIPPED')),
                    COALESCE(SUM($4), 0)
                INTO :v_files, :v_ok, :v_bad, :v_rows
                FROM TABLE(RESULT_SCAN(:v_copy_qid));

                v_status := CASE WHEN :v_bad = 0 THEN 'SUCCESS'
                                 WHEN :v_ok = 0  THEN 'FAILED'
                                 ELSE 'PARTIAL' END;
            END IF;

        EXCEPTION
            WHEN OTHER THEN
                v_err    := SQLERRM;
                v_status := 'ERROR';
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

    -- Push load history into ledger
    CALL AUDIT.SP_SYNC_LEDGER(1);

    IF (v_summary = '') THEN
        RETURN 'NO_MANUAL_SOURCES_EXECUTED (Verify P_SOURCE or check if source is managed by PIPE)';
    END IF;

    RETURN TRIM(v_summary);
END;


-- AUDIT.SP_RECONCILE
-- Purpose: Performs a 4-way data integrity reconciliation cross-checking Azure physical storage, the audit ledger, physical raw tables, and ingestion infrastructure health.
-- Execution Rights: EXECUTE AS CALLER.
-- Input Parameters:
-- P_SOURCE (VARCHAR, Default: 'ALL'): Target feed identifier or 'ALL'.
-- P_LOOKBACK_DAYS (NUMBER, Default: 3): Modification window for staged files (0 for all-time).

-- Step-by-Step Logic:
-- Storage Inventory (Layer 1): Scans DIRECTORY(@RAW.DEVCOSTDATA) and records files into AUDIT.STAGE_INVENTORY. Applies a 15-minute upper grace window to prevent in-flight files from causing false alerts.
-- Ledger Freshness (Layer 2): Flags LEDGER_STALE if the last synchronization in FILE_LEDGER was over 2 hours ago.
-- Continuous Pipe Health (Layer 3): When auditing ALL or PCF, inspects SYSTEM$PIPE_STATUS('RAW.PCF_PIPE'). Flags PIPE_NOT_RUNNING or PIPE_NOT_FOUND as blocking errors.
-- Missing Ingestions (Layer 4): Identifies files present in storage inventory that have no 'LOADED' or 'SKIPPED' entry in FILE_LEDGER. Flags MISSING_LOAD.
-- Unrecovered Failures: Checks if the most recent load attempt failed and has not been succeeded by a 'LOADED' entry. Flags FAILED_NOT_RECOVERED.
-- Row Count Cross-Check: Aggregates physical rows via AUDIT.V_RAW_FILE_ROWS and compares them to SUM(row_loaded) in AUDIT.FILE_LEDGER. Flags ROWCOUNT_MISMATCH if counts diverge.
-- Stage Hygiene: On full audits ('ALL'), flags staged files that match no configured prefix as UNMAPPED_FILE (warning).

-- Outcome & Return:
-- FAIL: One or more blocking exceptions exist (blocking > 0).
-- PASS_WITH_WARNINGS: Zero blocking exceptions, but unmapped files exist (warnings > 0).
-- PASS: Zero exceptions.
-- Records summary in AUDIT.RECONCILE_RUN and purges STAGE_INVENTORY snapshots older than 7 days.

USE ROLE LOADER_ROLE;
USE DATABASE ANALYSECOST;
USE SCHEMA AUDIT;

CREATE OR REPLACE PROCEDURE AUDIT.SP_RECONCILE(
    P_SOURCE VARCHAR DEFAULT 'ALL',
    P_LOOKBACK_DAYS NUMBER DEFAULT 3
)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
DECLARE
    v_rid          VARCHAR DEFAULT UUID_STRING();
    v_src          VARCHAR;
    v_days         NUMBER;
    v_is_all       BOOLEAN;
    v_prefix       VARCHAR DEFAULT '';
    v_target_tbl   VARCHAR DEFAULT '';
    v_files        NUMBER DEFAULT 0;
    v_stale_hours  NUMBER DEFAULT 0;
    v_pipe_exists  NUMBER DEFAULT 0;
    v_pipe_state   VARCHAR DEFAULT 'NOT_APPLICABLE';
    v_blocking     NUMBER DEFAULT 0;
    v_warn         NUMBER DEFAULT 0;
    v_status       VARCHAR;
    v_window_str   VARCHAR;
BEGIN
    v_src    := UPPER(COALESCE(:P_SOURCE, 'ALL'));
    v_days   := COALESCE(:P_LOOKBACK_DAYS, 3);
    v_is_all := (v_src = 'ALL');

    IF (NOT v_is_all) THEN
        SELECT source_prefix, target_table
          INTO :v_prefix, :v_target_tbl
        FROM AUDIT.LOAD_SOURCE_CONFIG
        WHERE UPPER(source_name) = :v_src;

        IF (v_target_tbl IS NULL OR v_target_tbl = '') THEN
            RETURN 'ERROR: Source "' || v_src || '" not found in AUDIT.LOAD_SOURCE_CONFIG';
        END IF;
    END IF;

    -- =========================================================================
    -- 1. INCREMENTAL STAGE INVENTORY VIA DIRECTORY TABLE (15-min grace window)
    -- =========================================================================
    INSERT INTO AUDIT.STAGE_INVENTORY (
        reconcile_id, file_key, size_bytes, last_modified_raw, source_name, target_table
    )
    SELECT 
        :v_rid,
        d.RELATIVE_PATH,
        d.SIZE,
        d.LAST_MODIFIED::VARCHAR,
        c.source_name,
        c.target_table
    FROM DIRECTORY(@RAW.DEVCOSTDATA) d
    LEFT JOIN AUDIT.LOAD_SOURCE_CONFIG c
      ON STARTSWITH(d.RELATIVE_PATH, c.source_prefix)
     AND c.enabled
    WHERE (:v_is_all OR UPPER(c.source_name) = :v_src)
      AND (:v_days = 0 OR d.LAST_MODIFIED >= DATEADD('day', -1 * :v_days, CURRENT_TIMESTAMP()))
      AND d.LAST_MODIFIED <= DATEADD('minute', -15, CURRENT_TIMESTAMP());

    SELECT COALESCE(COUNT(*), 0) 
      INTO :v_files 
    FROM AUDIT.STAGE_INVENTORY 
    WHERE reconcile_id = :v_rid;

    -- =========================================================================
    -- 2. LEDGER FRESHNESS CHECK
    -- =========================================================================
    SELECT COALESCE(DATEDIFF('hour', MAX(synced_at), CURRENT_TIMESTAMP()), 999)
      INTO :v_stale_hours 
    FROM AUDIT.FILE_LEDGER
    WHERE (:v_is_all OR target_table = :v_target_tbl);

    INSERT INTO AUDIT.RECONCILE_EXCEPTION (reconcile_id, exception_type, detail, recommended_action)
    SELECT :v_rid, 'LEDGER_STALE',
           'Last ledger sync ' || :v_stale_hours || ' hour(s) ago for ' || IFF(:v_is_all, 'ALL', :v_target_tbl),
           'Check AUDIT.T_LEDGER_SYNC task history; run CALL AUDIT.SP_SYNC_LEDGER(24)'
    WHERE :v_stale_hours > 2;

    -- =========================================================================
    -- 3. PIPE HEALTH (Checked only when evaluating ALL or PCF specifically)
    -- =========================================================================
    IF (v_is_all OR v_src = 'PCF') THEN
        SELECT COUNT(*) INTO :v_pipe_exists 
        FROM ANALYSECOST.INFORMATION_SCHEMA.PIPES 
        WHERE PIPE_SCHEMA = 'RAW' AND PIPE_NAME = 'PCF_PIPE';

        IF (:v_pipe_exists > 0) THEN
            v_pipe_state := PARSE_JSON(SYSTEM$PIPE_STATUS('RAW.PCF_PIPE')):executionState::VARCHAR;

            IF (:v_pipe_state <> 'RUNNING') THEN
                INSERT INTO AUDIT.RECONCILE_EXCEPTION (
                    reconcile_id, exception_type, source_name, file_name, detail, recommended_action
                )
                VALUES (
                    :v_rid, 'PIPE_NOT_RUNNING', 'PCF', 'RAW.PCF_PIPE',
                    'executionState=' || :v_pipe_state,
                    'Check Azure queue consent and run ALTER PIPE RAW.PCF_PIPE RESUME;'
                );
            END IF;
        ELSE
            INSERT INTO AUDIT.RECONCILE_EXCEPTION (
                reconcile_id, exception_type, source_name, file_name, detail, recommended_action
            )
            VALUES (
                :v_rid, 'PIPE_NOT_FOUND', 'PCF', 'RAW.PCF_PIPE',
                'Pipe object RAW.PCF_PIPE is missing in Snowflake catalog',
                'Run CREATE PIPE statement for RAW.PCF_PIPE'
            );
        END IF;
    END IF;

    -- =========================================================================
    -- 4. MISSING INGESTIONS IN WINDOW
    -- =========================================================================
    INSERT INTO AUDIT.RECONCILE_EXCEPTION (reconcile_id, exception_type, source_name, file_name, detail, recommended_action)
    SELECT :v_rid, 'MISSING_LOAD', i.source_name, i.file_key,
           'No LOADED ledger row for ' || i.target_table,
           IFF(c.load_mode = 'PIPE',
               'ALTER PIPE ' || COALESCE(c.pipe_name, 'RAW.PCF_PIPE') || ' REFRESH PREFIX = ''' || c.source_prefix || ''';',
               'CALL AUDIT.SP_LOAD_RAW_MANUAL(''' || c.source_name || ''');')
    FROM AUDIT.STAGE_INVENTORY i
    JOIN AUDIT.LOAD_SOURCE_CONFIG c ON c.source_name = i.source_name
    WHERE i.reconcile_id = :v_rid
      AND (:v_is_all OR UPPER(i.source_name) = :v_src)
      AND NOT EXISTS (
          SELECT 1 FROM AUDIT.FILE_LEDGER l
          WHERE l.target_table = i.target_table
            AND l.file_name    = i.file_key
            AND l.status IN ('LOADED', 'SKIPPED'));

    -- =========================================================================
    -- 5. FAILED ATTEMPTS IN WINDOW
    -- =========================================================================
    INSERT INTO AUDIT.RECONCILE_EXCEPTION (reconcile_id, exception_type, source_name, file_name, detail, recommended_action)
    SELECT :v_rid, 'FAILED_NOT_RECOVERED', c.source_name, l.file_name,
           'Latest attempt ' || l.status || ': ' || COALESCE(l.first_error_message, 'no message'),
           IFF(c.load_mode = 'PIPE',
               'Fix file syntax in Azure, then ALTER PIPE ' || COALESCE(c.pipe_name, 'RAW.PCF_PIPE') || ' REFRESH PREFIX = ''' || c.source_prefix || ''';',
               'Fix file, then CALL AUDIT.SP_LOAD_RAW_MANUAL(''' || c.source_name || ''');')
    FROM (
        SELECT x.*, ROW_NUMBER() OVER (PARTITION BY x.target_table, x.file_name ORDER BY x.attempted_at DESC) AS rn
        FROM AUDIT.FILE_LEDGER x
        WHERE (:v_is_all OR x.target_table = :v_target_tbl)
          AND (:v_days = 0 OR x.attempted_at >= DATEADD('day', -1 * :v_days, CURRENT_TIMESTAMP()))
    ) l
    JOIN AUDIT.LOAD_SOURCE_CONFIG c ON c.target_table = l.target_table
    WHERE l.rn = 1
      AND l.status NOT IN ('LOADED', 'SKIPPED', 'IN_PROGRESS')
      AND EXISTS (
          SELECT 1 FROM AUDIT.STAGE_INVENTORY i
          WHERE i.reconcile_id = :v_rid AND i.file_key = l.file_name AND i.target_table = l.target_table)
      AND NOT EXISTS (
          SELECT 1 FROM AUDIT.FILE_LEDGER ok
          WHERE ok.target_table = l.target_table AND ok.file_name = l.file_name AND ok.status IN ('LOADED', 'SKIPPED'));

    -- =========================================================================
    -- 6. ROWCOUNT DISCREPANCIES IN WINDOW
    -- =========================================================================
    INSERT INTO AUDIT.RECONCILE_EXCEPTION (reconcile_id, exception_type, source_name, file_name, detail, recommended_action)
    SELECT :v_rid, 'ROWCOUNT_MISMATCH', c.source_name, COALESCE(r.file_name, l.file_name),
           'RAW rows=' || COALESCE(r.raw_rows, 0) || ' ledger rows=' || COALESCE(l.ledger_rows, 0),
           'Check COPY_HISTORY against RAW for this file; re-run ledger sync; investigate before dbt builds'
    FROM (
        SELECT * FROM AUDIT.V_RAW_FILE_ROWS
        WHERE file_name IN (SELECT file_key FROM AUDIT.STAGE_INVENTORY WHERE reconcile_id = :v_rid)
    ) r
    FULL OUTER JOIN (
        SELECT target_table, file_name, SUM(row_loaded) AS ledger_rows
        FROM AUDIT.FILE_LEDGER
        WHERE status IN ('LOADED', 'PARTIAL')
          AND (:v_is_all OR target_table = :v_target_tbl)
          AND file_name IN (SELECT file_key FROM AUDIT.STAGE_INVENTORY WHERE reconcile_id = :v_rid)
        GROUP BY target_table, file_name
    ) l
      ON l.target_table = r.target_table AND l.file_name = r.file_name
    LEFT JOIN AUDIT.LOAD_SOURCE_CONFIG c
      ON c.target_table = COALESCE(r.target_table, l.target_table)
    WHERE COALESCE(r.raw_rows, 0) <> COALESCE(l.ledger_rows, 0);

    -- =========================================================================
    -- 7. UNMAPPED FILES (Full audits only)
    -- =========================================================================
    IF (v_is_all) THEN
        INSERT INTO AUDIT.RECONCILE_EXCEPTION (reconcile_id, exception_type, file_name, detail, recommended_action)
        SELECT :v_rid, 'UNMAPPED_FILE', file_key, 'No LOAD_SOURCE_CONFIG prefix matches',
               'Add a config row or move the file to a known prefix'
        FROM AUDIT.STAGE_INVENTORY
        WHERE reconcile_id = :v_rid AND source_name IS NULL;
    END IF;

    -- =========================================================================
    -- 8. TALLY STATUS & RETURN
    -- =========================================================================
    SELECT COALESCE(COUNT_IF(exception_type IN (
               'LEDGER_STALE',
               'PIPE_NOT_RUNNING',
               'PIPE_NOT_FOUND',
               'MISSING_LOAD',
               'FAILED_NOT_RECOVERED',
               'ROWCOUNT_MISMATCH'
           )), 0),
           COALESCE(COUNT_IF(exception_type = 'UNMAPPED_FILE'), 0)
      INTO :v_blocking, :v_warn
    FROM AUDIT.RECONCILE_EXCEPTION WHERE reconcile_id = :v_rid;

    v_status := IFF(COALESCE(v_blocking, 0) > 0, 'FAIL', 
                IFF(COALESCE(v_warn, 0) > 0, 'PASS_WITH_WARNINGS', 'PASS'));

    INSERT INTO AUDIT.RECONCILE_RUN (reconcile_id, azure_files, exception_count, blocking_count, warning_count, status)
    SELECT :v_rid, 
           COALESCE(:v_files, 0), 
           COALESCE(:v_blocking, 0) + COALESCE(:v_warn, 0), 
           COALESCE(:v_blocking, 0), 
           COALESCE(:v_warn, 0), 
           :v_status;

    -- Self-maintenance: Prune inventory snapshots older than 7 days
    DELETE FROM AUDIT.STAGE_INVENTORY WHERE scanned_at < DATEADD('day', -7, CURRENT_TIMESTAMP());

    v_window_str := IFF(v_days = 0, 'ALL', v_days::VARCHAR);

    RETURN COALESCE(v_status, 'PASS') 
        || ' blocking=' || COALESCE(v_blocking, 0) 
        || ' warnings=' || COALESCE(v_warn, 0) 
        || ' evaluated_files=' || COALESCE(v_files, 0) 
        || ' window_days=' || COALESCE(v_window_str, 'ALL') 
        || ' reconcile_id=' || COALESCE(v_rid, '');
END;

select * from AUDIT.V_RAW_FILE_ROWS;

select * from RAW.PCF_RAW;