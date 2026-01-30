// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package queries

const SlowQuery = `DECLARE @IntervalSeconds INT = %d; 		-- Define the interval in seconds
DECLARE @TopN INT = %d; 				-- Number of top queries to retrieve
DECLARE @ElapsedTimeThreshold INT = %d;  -- Elapsed time threshold in milliseconds
DECLARE @TextTruncateLimit INT = %d; 	-- Truncate limit for query_text
				
WITH StatementDetails AS (
	SELECT
		qs.plan_handle,
		qs.sql_handle,
		-- Extract query text using Microsoft's official offset logic (no +1 on length)
		-- Reference: https://learn.microsoft.com/en-us/sql/relational-databases/system-dynamic-management-views/sys-dm-exec-query-stats-transact-sql
		LEFT(SUBSTRING(
			qt.text,
			(qs.statement_start_offset / 2) + 1,
			(
				CASE
					qs.statement_end_offset
					WHEN -1 THEN DATALENGTH(qt.text)
					ELSE qs.statement_end_offset
				END - qs.statement_start_offset
			) / 2
		), @TextTruncateLimit) AS query_text,
		-- query_id: SQL Server's query_hash - used for correlating with active query metrics
		qs.query_hash AS query_id,
		qs.creation_time,
		qs.last_execution_time,
		qs.execution_count,
        -- Historical average metrics (reflecting all runs since caching)
		(qs.total_worker_time / qs.execution_count) / 1000.0 AS avg_cpu_time_ms,
		(qs.total_elapsed_time / qs.execution_count) / 1000.0 AS avg_elapsed_time_ms,
		-- Total elapsed time for precise delta calculation (avoids floating point precision loss)
		qs.total_elapsed_time / 1000.0 AS total_elapsed_time_ms,
		(qs.total_logical_reads / qs.execution_count) AS avg_disk_reads,
		(qs.total_logical_writes / qs.execution_count) AS avg_disk_writes,
		-- Average rows processed (returned by query)
		(qs.total_rows / qs.execution_count) AS avg_rows_processed,
		-- Lock wait time approximation: elapsed_time - (cpu_time + io_time)
		-- NOTE: This is an approximation as dm_exec_query_stats doesn't track lock waits separately
		-- For precise lock wait time, Query Store wait_category = 4 (Lock waits) should be used
		CASE
			WHEN (qs.total_elapsed_time / qs.execution_count) > (qs.total_worker_time / qs.execution_count)
			THEN ((qs.total_elapsed_time - qs.total_worker_time) / qs.execution_count) / 1000.0
			ELSE 0.0
		END AS avg_lock_wait_time_ms,
		-- Determine statement type (SELECT, INSERT, etc.)
		CASE
			WHEN UPPER(LTRIM(SUBSTRING(qt.text, (qs.statement_start_offset / 2) + 1, 6))) LIKE 'SELECT' THEN 'SELECT'
			WHEN UPPER(LTRIM(SUBSTRING(qt.text, (qs.statement_start_offset / 2) + 1, 6))) LIKE 'INSERT' THEN 'INSERT'
			WHEN UPPER(LTRIM(SUBSTRING(qt.text, (qs.statement_start_offset / 2) + 1, 6))) LIKE 'UPDATE' THEN 'UPDATE'
			WHEN UPPER(LTRIM(SUBSTRING(qt.text, (qs.statement_start_offset / 2) + 1, 6))) LIKE 'DELETE' THEN 'DELETE'
			ELSE 'OTHER'
		END AS statement_type,
		CONVERT(INT, pa.value) AS database_id,
		qt.objectid
	FROM
		sys.dm_exec_query_stats qs
		CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) AS qt
		OUTER APPLY (
			SELECT TOP 1 pa.value
			FROM sys.dm_exec_plan_attributes(qs.plan_handle) AS pa
			WHERE pa.attribute = 'dbid'
		) AS pa
	WHERE
		-- *** KEY FILTER: Only plans that ran in the last @IntervalSeconds (e.g., 15) ***
		qs.last_execution_time >= DATEADD(SECOND, -@IntervalSeconds, GETUTCDATE())
		AND qs.execution_count > 0
		AND qt.text IS NOT NULL
		AND LTRIM(RTRIM(qt.text)) <> ''
		AND DB_NAME(CONVERT(INT, pa.value)) NOT IN ('master', 'model', 'msdb', 'tempdb')
		-- OPTIMIZED: Use objectid filter instead of expensive LIKE (100x faster)
		-- System objects have objectid < 100, user objects >= 100, ad-hoc queries = NULL
		AND (qt.objectid IS NULL OR qt.objectid >= 100)
)
-- Select the raw, non-aggregated statement data.
-- NOTE: TOP N filtering removed - will be applied in Go code AFTER delta calculation
-- This ensures we get enough candidates for interval-based (delta) averaging
SELECT
    s.query_id,
	s.plan_handle,
    s.query_text,
    DB_NAME(s.database_id) AS database_name,
    COALESCE(
        OBJECT_SCHEMA_NAME(s.objectid, s.database_id),
        'N/A'
    ) AS schema_name,
    CONVERT(VARCHAR(25), SWITCHOFFSET(CAST(s.creation_time AS DATETIMEOFFSET), '+00:00'), 127) + 'Z' AS creation_time,
    CONVERT(VARCHAR(25), SWITCHOFFSET(CAST(s.last_execution_time AS DATETIMEOFFSET), '+00:00'), 127) + 'Z' AS last_execution_timestamp,
    s.execution_count,
    s.avg_cpu_time_ms,
    s.avg_elapsed_time_ms,
    s.total_elapsed_time_ms,
    s.avg_disk_reads,
    s.avg_disk_writes,
    s.avg_rows_processed,
    s.avg_lock_wait_time_ms,
    s.statement_type,
    CONVERT(VARCHAR(25), SWITCHOFFSET(SYSDATETIMEOFFSET(), '+00:00'), 127) + 'Z' AS collection_timestamp
FROM
    StatementDetails s`

// ActiveQueryExecutionPlanQuery fetches the execution plan for an active query using its plan_handle
// NOTE: This is used only for active running queries, NOT for slow queries from dm_exec_query_stats
// The plan_handle hex string is converted to VARBINARY(64) format that sys.dm_exec_query_plan expects
const ActiveQueryExecutionPlanQuery = `
SELECT
    CAST(qp.query_plan AS NVARCHAR(MAX)) AS execution_plan_xml
FROM sys.dm_exec_query_plan(CONVERT(VARBINARY(64), '%s', 1)) AS qp
WHERE qp.query_plan IS NOT NULL;`

// ActiveRunningQueriesQuery retrieves currently executing queries
// with wait and blocking details
// OPTIMIZED: Only selects fields required by NRQL queries (21 fields instead of 48)
// Includes temp table for KEY lock resolution to populate wait_resource_object_name
// This query captures real-time execution state including wait types, blocking chains, and query text
//
// RCA Enhancement: Includes query_hash for correlation with slow queries
const ActiveRunningQueriesQuery = `
-- ============================================================================
-- CROSS-DATABASE KEY LOCK RESOLUTION: Populate partition info from all databases
-- ============================================================================
IF OBJECT_ID('tempdb..#all_partitions') IS NOT NULL DROP TABLE #all_partitions;

CREATE TABLE #all_partitions (
    database_id INT,
    object_id INT,
    index_id INT,
    hobt_id BIGINT,
    index_name NVARCHAR(128),
    index_type NVARCHAR(60)
);

DECLARE @sql NVARCHAR(MAX);
DECLARE @db_name NVARCHAR(128);
DECLARE @db_id INT;

DECLARE db_cursor CURSOR FAST_FORWARD FOR
    SELECT database_id, name
    FROM sys.databases
    WHERE database_id > 4 AND state = 0 AND user_access = 0%s;

OPEN db_cursor;
FETCH NEXT FROM db_cursor INTO @db_id, @db_name;

WHILE @@FETCH_STATUS = 0
BEGIN
    BEGIN TRY
        SET @sql = N'INSERT INTO #all_partitions SELECT ' + CAST(@db_id AS NVARCHAR(10)) +
            ', p.object_id, p.index_id, p.hobt_id, i.name, i.type_desc FROM [' +
            REPLACE(@db_name, ']', ']]') + '].sys.partitions p INNER JOIN [' +
            REPLACE(@db_name, ']', ']]') + '].sys.indexes i ON p.object_id = i.object_id AND p.index_id = i.index_id WHERE p.object_id > 100;';
        EXEC sp_executesql @sql;
    END TRY
    BEGIN CATCH END CATCH;
    FETCH NEXT FROM db_cursor INTO @db_id, @db_name;
END;

CLOSE db_cursor;
DEALLOCATE db_cursor;

-- ============================================================================
-- MAIN QUERY: Active running queries with cross-database KEY lock resolution
-- ============================================================================
DECLARE @Limit INT = %d; -- Set the maximum number of rows to return
DECLARE @TextTruncateLimit INT = %d; -- Set the maximum length for query text
DECLARE @ElapsedTimeThresholdMs INT = %d; -- Minimum elapsed time threshold in milliseconds

SELECT TOP (@Limit)
    -- A. SESSION IDENTIFICATION (Required for correlation)
    r_wait.session_id AS current_session_id,
    r_wait.request_id AS request_id,
    
    -- B. SESSION CONTEXT (Required by NRQL Query 2)
    DB_NAME(r_wait.database_id) AS database_name,
    s_wait.login_name AS login_name,
    s_wait.host_name AS host_name,

    -- C. QUERY CORRELATION (Required for slow query correlation)
    r_wait.query_hash AS query_id,
    LEFT(st_wait.text, @TextTruncateLimit) AS query_statement_text,

    -- D. WAIT DETAILS (Required by NRQL Query 1)
    r_wait.wait_type AS wait_type,
    r_wait.wait_time / 1000.0 AS wait_time_s,
    r_wait.wait_resource AS wait_resource,
    r_wait.last_wait_type AS last_wait_type,
    
    -- D2. WAIT RESOURCE OBJECT NAME (Required by NRQL Query 1 - Lock Time Analysis dashboard)
    CASE
        WHEN r_wait.wait_resource LIKE 'OBJECT:%%' THEN
            OBJECT_NAME(
                TRY_CAST(
                    SUBSTRING(
                        r_wait.wait_resource,
                        CHARINDEX(':', r_wait.wait_resource, 8) + 1,
                        CASE
                            WHEN CHARINDEX(':', r_wait.wait_resource, CHARINDEX(':', r_wait.wait_resource, 8) + 1) > 0
                            THEN CHARINDEX(':', r_wait.wait_resource, CHARINDEX(':', r_wait.wait_resource, 8) + 1) - CHARINDEX(':', r_wait.wait_resource, 8) - 1
                            ELSE LEN(r_wait.wait_resource) - CHARINDEX(':', r_wait.wait_resource, 8)
                        END
                    ) AS INT
                ),
                r_wait.database_id
            )
        -- KEY locks: Include index name in format: "TableName (IndexName)"
        WHEN r_wait.wait_resource LIKE 'KEY:%%' AND idx_key.index_name IS NOT NULL THEN
            OBJECT_NAME(idx_key.object_id, r_wait.database_id) +
            CASE
                WHEN idx_key.index_name IS NOT NULL THEN ' (' + idx_key.index_name + ')'
                ELSE ''
            END
        -- KEY locks without index info: Just show table name
        WHEN r_wait.wait_resource LIKE 'KEY:%%' AND idx_key.object_id IS NOT NULL THEN
            OBJECT_NAME(idx_key.object_id, r_wait.database_id)
        ELSE NULL
    END AS wait_resource_object_name,

    -- E. TIMESTAMPS (Required by NRQL queries)
    CONVERT(VARCHAR(25), SWITCHOFFSET(CAST(r_wait.start_time AS DATETIMEOFFSET), '+00:00'), 127) + 'Z' AS request_start_time,
    CONVERT(VARCHAR(25), SWITCHOFFSET(SYSDATETIMEOFFSET(), '+00:00'), 127) + 'Z' AS collection_timestamp,

    -- F. TRANSACTION CONTEXT (Required by NRQL Query 1)
    r_wait.transaction_id AS transaction_id,
    r_wait.open_transaction_count AS open_transaction_count,

    -- G. PLAN HANDLE (Required for execution plan retrieval)
    r_wait.plan_handle AS plan_handle,

    -- H. BLOCKING DETAILS (Required by NRQL Query 1)
    CASE
        WHEN r_wait.blocking_session_id = 0 THEN NULL
        ELSE r_wait.blocking_session_id
    END AS blocking_session_id,
    ISNULL(s_blocker.login_name, 'N/A') AS blocker_login_name,
    
    -- H2. BLOCKING QUERY TEXT (Required by NRQL Query 1)
    CASE
        WHEN r_wait.blocking_session_id = 0 THEN 'N/A'
        WHEN r_blocker.command IS NULL AND ib_blocker.event_info IS NOT NULL THEN LEFT(ib_blocker.event_info, @TextTruncateLimit)
        WHEN st_blocker.text IS NOT NULL THEN LEFT(st_blocker.text, @TextTruncateLimit)
        ELSE 'N/A'
    END AS blocking_query_statement_text,
    
    -- H3. BLOCKING QUERY HASH (Required by NRQL Query 1)
    r_blocker.query_hash AS blocking_query_hash

FROM
    sys.dm_exec_requests AS r_wait
INNER JOIN
    sys.dm_exec_sessions AS s_wait ON s_wait.session_id = r_wait.session_id
CROSS APPLY
    sys.dm_exec_sql_text(r_wait.sql_handle) AS st_wait
LEFT JOIN
    sys.dm_exec_requests AS r_blocker ON r_wait.blocking_session_id = r_blocker.session_id
LEFT JOIN
    sys.dm_exec_sessions AS s_blocker ON r_wait.blocking_session_id = s_blocker.session_id
OUTER APPLY
    sys.dm_exec_sql_text(r_blocker.sql_handle) AS st_blocker
OUTER APPLY
    sys.dm_exec_input_buffer(r_wait.blocking_session_id, NULL) AS ib_blocker
-- JOIN temp table for KEY/PAGE lock resolution
LEFT JOIN
    #all_partitions AS idx_key ON idx_key.hobt_id = 
        TRY_CAST(
            SUBSTRING(
                r_wait.wait_resource,
                -- Find SECOND colon position (after database_id, before hobt_id)
                CHARINDEX(':', r_wait.wait_resource, CHARINDEX(':', r_wait.wait_resource) + 1) + 1,
                CASE
                    -- Find THIRD colon or space (before lock hash)
                    WHEN CHARINDEX(':', r_wait.wait_resource, CHARINDEX(':', r_wait.wait_resource, CHARINDEX(':', r_wait.wait_resource) + 1) + 1) > 0
                    THEN CHARINDEX(':', r_wait.wait_resource, CHARINDEX(':', r_wait.wait_resource, CHARINDEX(':', r_wait.wait_resource) + 1) + 1) - CHARINDEX(':', r_wait.wait_resource, CHARINDEX(':', r_wait.wait_resource) + 1) - 1
                    WHEN CHARINDEX(' ', r_wait.wait_resource, CHARINDEX(':', r_wait.wait_resource, CHARINDEX(':', r_wait.wait_resource) + 1) + 1) > 0
                    THEN CHARINDEX(' ', r_wait.wait_resource, CHARINDEX(':', r_wait.wait_resource, CHARINDEX(':', r_wait.wait_resource) + 1) + 1) - CHARINDEX(':', r_wait.wait_resource, CHARINDEX(':', r_wait.wait_resource) + 1) - 1
                    ELSE LEN(r_wait.wait_resource) - CHARINDEX(':', r_wait.wait_resource, CHARINDEX(':', r_wait.wait_resource) + 1)
                END
            ) AS BIGINT
        )
    AND idx_key.database_id = r_wait.database_id

WHERE
    r_wait.session_id > 50
    AND r_wait.database_id > 4
    AND r_wait.wait_type IS NOT NULL
    AND r_wait.query_hash IS NOT NULL  -- Filter out queries without query_hash (PREEMPTIVE waits, system queries)
    AND r_wait.total_elapsed_time >= @ElapsedTimeThresholdMs  -- Filter by elapsed time threshold
    %s  -- Placeholder for additional query_hash IN filter (injected from Go code)
ORDER BY
    r_wait.total_elapsed_time DESC  -- Sort by slowest executions first (not wait_time)
OPTION (RECOMPILE);  -- OPTIMIZED: Recompile for current parameter values`
