#!/usr/bin/env python3
"""
MySQL Monitoring Queries - Oracle Receiver Pattern with Interval-Based Delta Calculation

This script implements the same execution flow as the New Relic Oracle receiver:
1. Fetch slow queries with cumulative metrics (execution_count, total_elapsed_time_ms)
2. Calculate interval-based deltas using cached previous state
3. Filter and sort by interval averages (not cumulative)
4. Correlate each query with wait events, blocking sessions, and execution plans
5. Output metrics in JSON format

Key Features:
- Interval-based delta calculation (matches Oracle receiver's OracleIntervalCalculator)
- Per-query correlation (matches Oracle receiver's ScrapeSlowQueries)
- First scrape: uses cumulative averages as baseline
- Subsequent scrapes: calculates interval averages from deltas
- TTL-based state cleanup (CACHE_TTL_MINUTES)
"""

import json
import mysql.connector
import sys
import os
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone, timedelta
from decimal import Decimal
import time

# MySQL Connection Configuration
MYSQL_CONFIG = {
    'user': 'monitor',
    'password': 'monitorpass',
    'host': 'localhost',
    'port': 3306,
    'database': 'testdb',
}

# Collection Configuration (matches Oracle receiver config)
COLLECTION_INTERVAL = 60  # seconds - time window for fetching queries
TOP_N_QUERIES = 10  # Number of top slow queries after delta calculation
RESPONSE_TIME_THRESHOLD_MS = 0  # Filter threshold in ms (0 = no filtering)
STATE_FILE = '/tmp/mysql_metrics_state.json'  # State persistence for delta calculation
CACHE_TTL_MINUTES = 10  # TTL for inactive queries in state cache


def decimal_default(obj):
    """JSON serializer for Decimal objects"""
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, (datetime,)):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")


class IntervalCalculator:
    """
    Interval-based delta calculator for MySQL slow queries
    Matches the logic of Oracle receiver's OracleIntervalCalculator
    """
    
    def __init__(self, cache_ttl_minutes=10):
        self.cache_ttl = timedelta(minutes=cache_ttl_minutes)
        self.state_cache = {}  # query_key -> QueryState
        self.last_cleanup = datetime.now(timezone.utc)
    
    def calculate_metrics(self, query, now):
        """
        Calculate interval-based metrics for a query
        
        Returns:
        {
            'interval_avg_elapsed_time_ms': float,
            'interval_execution_count': int,
            'historical_avg_elapsed_time_ms': float,
            'is_first_scrape': bool,
            'has_new_executions': bool,
        }
        """
        query_id = query.get('query_id', '')
        database_name = query.get('database_name', '')
        query_key = f"{database_name}:{query_id}"
        
        # Current cumulative values
        current_exec_count = int(query.get('execution_count', 0))
        current_total_elapsed_ms = float(query.get('total_elapsed_time_ms', 0))
        current_avg_elapsed_ms = float(query.get('avg_elapsed_time_ms', 0))
        
        # Handle zero execution count
        if current_exec_count == 0:
            return None
        
        # Check if query exists in cache
        if query_key not in self.state_cache:
            # FIRST SCRAPE: Use cumulative average as baseline
            self.state_cache[query_key] = {
                'prev_execution_count': current_exec_count,
                'prev_total_elapsed_ms': current_total_elapsed_ms,
                'first_seen': now,
                'last_seen': now,
            }
            
            return {
                'interval_avg_elapsed_time_ms': current_avg_elapsed_ms,
                'interval_execution_count': current_exec_count,
                'historical_avg_elapsed_time_ms': current_avg_elapsed_ms,
                'is_first_scrape': True,
                'has_new_executions': True,
            }
        
        # SUBSEQUENT SCRAPES: Calculate delta
        state = self.state_cache[query_key]
        prev_exec_count = state['prev_execution_count']
        prev_total_elapsed_ms = state['prev_total_elapsed_ms']
        
        delta_exec_count = current_exec_count - prev_exec_count
        delta_elapsed_ms = current_total_elapsed_ms - prev_total_elapsed_ms
        
        # Update last seen timestamp
        state['last_seen'] = now
        
        # No new executions
        if delta_exec_count == 0:
            return {
                'interval_avg_elapsed_time_ms': 0,
                'interval_execution_count': 0,
                'historical_avg_elapsed_time_ms': current_avg_elapsed_ms,
                'is_first_scrape': False,
                'has_new_executions': False,
            }
        
        # Handle plan cache reset (negative delta) or counter wrap
        if delta_exec_count < 0 or delta_elapsed_ms < 0:
            # Reset baseline - treat as first scrape
            state['prev_execution_count'] = current_exec_count
            state['prev_total_elapsed_ms'] = current_total_elapsed_ms
            state['first_seen'] = now
            
            return {
                'interval_avg_elapsed_time_ms': current_avg_elapsed_ms,
                'interval_execution_count': current_exec_count,
                'historical_avg_elapsed_time_ms': current_avg_elapsed_ms,
                'is_first_scrape': True,  # Treat as first scrape
                'has_new_executions': True,
            }
        
        # Calculate interval average
        interval_avg_elapsed_ms = delta_elapsed_ms / delta_exec_count if delta_exec_count > 0 else 0
        
        # Update state for next scrape
        state['prev_execution_count'] = current_exec_count
        state['prev_total_elapsed_ms'] = current_total_elapsed_ms
        
        return {
            'interval_avg_elapsed_time_ms': interval_avg_elapsed_ms,
            'interval_execution_count': delta_exec_count,
            'historical_avg_elapsed_time_ms': current_avg_elapsed_ms,
            'is_first_scrape': False,
            'has_new_executions': True,
        }
    
    def cleanup_stale_entries(self, now):
        """Remove queries not seen within TTL (matches Oracle receiver's CleanupStaleEntries)"""
        if now - self.last_cleanup < timedelta(minutes=1):
            return  # Only cleanup once per minute
        
        stale_keys = []
        for query_key, state in self.state_cache.items():
            if now - state['last_seen'] > self.cache_ttl:
                stale_keys.append(query_key)
        
        for key in stale_keys:
            del self.state_cache[key]
        
        self.last_cleanup = now
    
    def get_cache_stats(self):
        """Get cache statistics for debugging"""
        return {
            'total_queries': len(self.state_cache),
            'cache_ttl_minutes': self.cache_ttl.total_seconds() / 60,
        }


def get_slow_queries(cursor, interval_seconds=60):
    """
    Fetch slow queries with cumulative metrics
    Matches Oracle receiver's GetSlowQueriesSQL() pattern
    
    Key differences from Oracle:
    - MySQL uses events_statements_summary_by_digest for completed queries
    - MySQL uses events_statements_current for currently executing queries
    - This combines both sources (like Oracle's V$SQLAREA)
    
    Returns cumulative metrics for delta calculation:
    - execution_count (total since digest created)
    - total_elapsed_time_ms (sum of all executions) - for delta calculation
    - avg_* metrics (cumulative averages) - matches Oracle pattern
    """
    # Query 1: Get completed queries from summary table
    completed_query = """
        SELECT
            UTC_TIMESTAMP() AS collection_timestamp,
            COALESCE(SCHEMA_NAME, 'NULL') AS database_name,
            DIGEST AS query_id,
            CASE
                WHEN CHAR_LENGTH(DIGEST_TEXT) > 4000 THEN CONCAT(LEFT(DIGEST_TEXT, 3997), '...')
                ELSE DIGEST_TEXT
            END AS query_text,
            COUNT_STAR AS execution_count,
            ROUND((SUM_TIMER_WAIT / NULLIF(COUNT_STAR, 0)) / 1000000000000, 3) AS avg_elapsed_time_ms,
            ROUND((SUM_CPU_TIME / NULLIF(COUNT_STAR, 0)) / 1000000000000, 3) AS avg_cpu_time_ms,
            ROUND((SUM_LOCK_TIME / NULLIF(COUNT_STAR, 0)) / 1000000000000, 3) AS avg_lock_time_ms,
            ROUND(SUM_ROWS_EXAMINED / NULLIF(COUNT_STAR, 0), 3) AS avg_rows_examined,
            ROUND(SUM_ROWS_AFFECTED / NULLIF(COUNT_STAR, 0), 3) AS avg_rows_affected,
            ROUND(SUM_TIMER_WAIT / 1000000000000, 3) AS total_elapsed_time_ms,
            LAST_SEEN AS last_active_time,
            'COMPLETED' AS query_source
        FROM performance_schema.events_statements_summary_by_digest
        WHERE LAST_SEEN >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL %s SECOND)
          AND SCHEMA_NAME IS NOT NULL
          AND SCHEMA_NAME NOT IN ('performance_schema', 'information_schema', 'mysql', 'sys')
          AND DIGEST_TEXT NOT LIKE '%performance_schema%'
          AND DIGEST_TEXT NOT LIKE '%information_schema%'
          AND DIGEST_TEXT NOT LIKE 'SHOW %'
          AND DIGEST_TEXT NOT LIKE 'SET %'
          AND DIGEST_TEXT NOT LIKE 'START TRANSACTION%'
          AND DIGEST_TEXT NOT LIKE 'BEGIN%'
          AND DIGEST_TEXT NOT LIKE 'COMMIT%'
          AND DIGEST_TEXT NOT LIKE 'ROLLBACK%'
          AND DIGEST_TEXT NOT LIKE 'EXPLAIN %'
          AND COUNT_STAR > 0
        ORDER BY SUM_TIMER_WAIT DESC
    """
    
    # Query 2: Get currently executing queries
    executing_query = """
        SELECT
            UTC_TIMESTAMP() AS collection_timestamp,
            COALESCE(esc.CURRENT_SCHEMA, 'NULL') AS database_name,
            esc.DIGEST AS query_id,
            CASE
                WHEN CHAR_LENGTH(esc.SQL_TEXT) > 4000 THEN CONCAT(LEFT(esc.SQL_TEXT, 3997), '...')
                ELSE esc.SQL_TEXT
            END AS query_text,
            1 AS execution_count,
            ROUND(esc.TIMER_WAIT / 1000000000000, 3) AS avg_elapsed_time_ms,
            ROUND(esc.CPU_TIME / 1000000000000, 3) AS avg_cpu_time_ms,
            ROUND(esc.LOCK_TIME / 1000000000000, 3) AS avg_lock_time_ms,
            esc.ROWS_EXAMINED AS avg_rows_examined,
            esc.ROWS_AFFECTED AS avg_rows_affected,
            ROUND(esc.TIMER_WAIT / 1000000000000, 3) AS total_elapsed_time_ms,
            esc.TIMER_START AS last_active_time,
            'EXECUTING' AS query_source
        FROM performance_schema.events_statements_current esc
        JOIN performance_schema.threads t ON esc.THREAD_ID = t.THREAD_ID
        WHERE esc.SQL_TEXT IS NOT NULL
          AND esc.CURRENT_SCHEMA IS NOT NULL
          AND esc.CURRENT_SCHEMA NOT IN ('performance_schema', 'information_schema', 'mysql', 'sys')
          AND esc.SQL_TEXT NOT LIKE '%performance_schema%'
          AND esc.SQL_TEXT NOT LIKE '%information_schema%'
          AND esc.SQL_TEXT NOT LIKE 'SHOW %'
          AND esc.SQL_TEXT NOT LIKE 'SET %'
          AND esc.SQL_TEXT NOT LIKE 'START TRANSACTION%'
          AND esc.SQL_TEXT NOT LIKE 'BEGIN%'
          AND esc.SQL_TEXT NOT LIKE 'COMMIT%'
          AND esc.SQL_TEXT NOT LIKE 'ROLLBACK%'
          AND esc.SQL_TEXT NOT LIKE 'EXPLAIN %'
          AND t.TYPE = 'FOREGROUND'
        ORDER BY esc.TIMER_WAIT DESC
    """
    
    # Execute completed queries
    cursor.execute(completed_query, (interval_seconds,))
    columns = [desc[0] for desc in cursor.description]
    completed_results = []
    
    for row in cursor.fetchall():
        result_dict = {}
        for i, value in enumerate(row):
            result_dict[columns[i]] = value
        completed_results.append(result_dict)
    
    # Execute currently executing queries
    cursor.execute(executing_query)
    columns = [desc[0] for desc in cursor.description]
    executing_results = []
    
    for row in cursor.fetchall():
        result_dict = {}
        for i, value in enumerate(row):
            result_dict[columns[i]] = value
        executing_results.append(result_dict)
    
    # Merge results - deduplicate by query_id
    # Priority: executing queries (current SQL_TEXT) > completed queries (DIGEST_TEXT)
    # This ensures we get the actual query text for currently blocked/executing queries
    results_map = {}
    
    # Add completed queries first (cumulative metrics are more accurate)
    for result in completed_results:
        query_id = result.get('query_id', '')
        if query_id:
            results_map[query_id] = result
    
    # Add executing queries (will overwrite with actual SQL_TEXT for in-progress queries)
    # This is important because executing queries show the REAL query text (e.g., "- 3" vs "- 5")
    # while completed queries only show the parameterized DIGEST_TEXT
    for result in executing_results:
        query_id = result.get('query_id', '')
        if query_id:
            # If query already exists in results_map (from completed), merge the data
            if query_id in results_map:
                # Keep cumulative metrics from completed, but use actual query_text from executing
                existing = results_map[query_id]
                result['execution_count'] = existing.get('execution_count', result['execution_count'])
                result['total_elapsed_time_ms'] = existing.get('total_elapsed_time_ms', result['total_elapsed_time_ms'])
                # Use executing query's avg metrics if higher (shows current performance)
                if result.get('avg_elapsed_time_ms', 0) < existing.get('avg_elapsed_time_ms', 0):
                    result['avg_elapsed_time_ms'] = existing.get('avg_elapsed_time_ms')
                    result['avg_cpu_time_ms'] = existing.get('avg_cpu_time_ms')
                    result['avg_lock_time_ms'] = existing.get('avg_lock_time_ms')
            results_map[query_id] = result
    
    return list(results_map.values())


def get_wait_events_for_query(cursor, query_digest, database_name):
    """Get wait events correlated to a specific query by digest"""
    query_digest_short = query_digest[:32] if query_digest else ''
    
    if not query_digest_short:
        return []
    
    query = """
        SELECT
            UTC_TIMESTAMP() AS collection_timestamp,
            esc.CURRENT_SCHEMA AS database_name,
            esc.DIGEST AS query_id,
            CASE 
                WHEN ew.EVENT_NAME LIKE 'wait/io/table%%' THEN 'Table I/O'
                WHEN ew.EVENT_NAME LIKE 'wait/io/file%%' THEN 'File I/O'
                WHEN ew.EVENT_NAME LIKE 'wait/lock%%' THEN 'Lock'
                WHEN ew.EVENT_NAME LIKE 'wait/synch%%' THEN 'Synchronization'
                WHEN ew.EVENT_NAME LIKE 'wait/io/socket%%' THEN 'Network I/O'
                ELSE 'Other'
            END AS wait_category,
            ew.EVENT_NAME AS wait_event_name,
            SUM(ew.TIMER_WAIT) / 1000000000000 AS total_wait_time_ms,
            COUNT(*) AS wait_event_count,
            AVG(ew.TIMER_WAIT) / 1000000000000 AS avg_wait_time_ms
        FROM performance_schema.events_waits_current ew
        JOIN performance_schema.threads t ON ew.THREAD_ID = t.THREAD_ID
        JOIN performance_schema.events_statements_current esc ON ew.THREAD_ID = esc.THREAD_ID
        WHERE ew.EVENT_NAME IS NOT NULL
          AND LEFT(COALESCE(esc.DIGEST, ''), 32) = %s
        GROUP BY 
            esc.CURRENT_SCHEMA,
            esc.DIGEST,
            wait_category,
            ew.EVENT_NAME
        ORDER BY total_wait_time_ms DESC
        LIMIT 10
    """
    
    try:
        cursor.execute(query, (query_digest_short,))
        columns = [desc[0] for desc in cursor.description]
        results = []
        
        for row in cursor.fetchall():
            result_dict = {}
            for i, value in enumerate(row):
                result_dict[columns[i]] = value
            results.append(result_dict)
        
        return results
    except Exception as e:
        return []


def get_blocking_sessions_for_query(cursor, query_text, schema_name, query_digest=''):
    """Get blocking sessions correlated to a specific query"""
    query = """
        SELECT 
            UTC_TIMESTAMP() AS collection_timestamp,
            dlr.OBJECT_SCHEMA AS database_name,
            esc.DIGEST AS query_id,
            r.trx_id AS blocked_txn_id,
            r.trx_mysql_thread_id AS blocked_session_id,
            tr.PROCESSLIST_USER AS blocked_user,
            tr.PROCESSLIST_HOST AS blocked_host,
            r.trx_query AS blocked_query,
            r.trx_started AS blocked_txn_start_time,
            TIMESTAMPDIFF(SECOND, r.trx_started, NOW()) AS blocked_wait_duration_seconds,
            b.trx_id AS blocking_txn_id,
            b.trx_mysql_thread_id AS blocking_session_id,
            tb.PROCESSLIST_USER AS blocking_user,
            tb.PROCESSLIST_HOST AS blocking_host,
            COALESCE(
                b.trx_query,
                (SELECT SQL_TEXT
                 FROM performance_schema.events_statements_history
                 WHERE THREAD_ID = (SELECT THREAD_ID FROM performance_schema.threads WHERE PROCESSLIST_ID = b.trx_mysql_thread_id)
                   AND SQL_TEXT NOT IN ('START TRANSACTION', 'BEGIN', 'COMMIT', 'ROLLBACK')
                   AND SQL_TEXT NOT LIKE 'SET %'
                   AND SQL_TEXT NOT LIKE 'SHOW %'
                 ORDER BY EVENT_ID DESC
                 LIMIT 1)
            ) AS blocking_query,
            b.trx_started AS blocking_txn_start_time,
            dlr.OBJECT_SCHEMA AS locked_schema,
            dlr.OBJECT_NAME AS locked_table,
            dlr.INDEX_NAME AS locked_index,
            dlr.LOCK_TYPE,
            dlr.LOCK_MODE AS blocked_lock_mode,
            dlb.LOCK_MODE AS blocking_lock_mode
        FROM performance_schema.data_lock_waits dlw
        JOIN information_schema.innodb_trx r ON r.trx_id = dlw.REQUESTING_ENGINE_TRANSACTION_ID
        JOIN information_schema.innodb_trx b ON b.trx_id = dlw.BLOCKING_ENGINE_TRANSACTION_ID
        JOIN performance_schema.data_locks dlr ON dlw.REQUESTING_ENGINE_LOCK_ID = dlr.ENGINE_LOCK_ID
        JOIN performance_schema.data_locks dlb ON dlw.BLOCKING_ENGINE_LOCK_ID = dlb.ENGINE_LOCK_ID
        LEFT JOIN performance_schema.events_statements_current esc 
            ON r.trx_mysql_thread_id = esc.THREAD_ID
        LEFT JOIN performance_schema.threads tr 
            ON r.trx_mysql_thread_id = tr.PROCESSLIST_ID
        LEFT JOIN performance_schema.threads tb 
            ON b.trx_mysql_thread_id = tb.PROCESSLIST_ID
        WHERE dlr.OBJECT_SCHEMA = %s
        ORDER BY blocked_wait_duration_seconds DESC
        LIMIT 10
    """
    
    try:
        cursor.execute(query, (schema_name,))
        columns = [desc[0] for desc in cursor.description]
        results = []
        
        for row in cursor.fetchall():
            result_dict = {}
            for i, value in enumerate(row):
                result_dict[columns[i]] = value
            results.append(result_dict)
        
        return results
    except Exception as e:
        return []


def get_execution_plan_for_query(cursor, query_text, database_name):
    """
    Get execution plan for a specific query
    Handles parameterized queries by replacing ? with NULL for EXPLAIN purposes
    """
    if not query_text or query_text.strip() == '':
        return None
    
    # Replace parameterized placeholders with NULL for EXPLAIN
    # This allows us to get execution plans even for queries from the digest table
    explain_query = query_text.replace('?', 'NULL')
    
    try:
        # Switch to the correct database context
        if database_name and database_name != 'NULL':
            cursor.execute(f"USE `{database_name}`")
        
        cursor.execute(f"EXPLAIN FORMAT=JSON {explain_query}")
        result = cursor.fetchone()
        
        if result and result[0]:
            plan = json.loads(result[0])
            return plan.get('query_block')
    except Exception as e:
        # If EXPLAIN fails (syntax errors, etc.), return None
        # Uncomment for debugging: print(f"EXPLAIN error for query: {e}")
        return None
    
    return None


def scrape_slow_queries(cursor, interval_calculator, interval_seconds, top_n, threshold_ms):
    """
    Scrape slow queries with interval-based delta calculation
    Matches Oracle receiver's ScrapeSlowQueries() pattern
    
    Execution flow:
    1. Fetch all slow queries from MySQL (no filtering in SQL)
    2. Calculate interval metrics for each query
    3. Filter by threshold (interval average, not cumulative)
    4. Sort by interval average (not cumulative)
    5. Take top N queries
    6. Correlate with wait events, blocking sessions, execution plans
    """
    now = datetime.now(timezone.utc)
    
    # Step 1: Fetch slow queries with cumulative metrics
    slow_queries = get_slow_queries(cursor, interval_seconds)
    
    # Step 2: Calculate interval metrics and filter
    queries_to_process = []
    
    for query in slow_queries:
        # Calculate interval metrics
        interval_metrics = interval_calculator.calculate_metrics(query, now)
        
        if interval_metrics is None:
            continue
        
        # Skip queries with no new executions
        if not interval_metrics['has_new_executions']:
            continue
        
        # Apply interval-based threshold filtering
        if interval_metrics['interval_avg_elapsed_time_ms'] < threshold_ms:
            continue
        
        # Store interval metrics in query object
        query['interval_avg_elapsed_time_ms'] = interval_metrics['interval_avg_elapsed_time_ms']
        query['interval_execution_count'] = interval_metrics['interval_execution_count']
        query['is_first_scrape'] = interval_metrics['is_first_scrape']
        
        queries_to_process.append(query)
    
    # Step 3: Sort by interval average (descending) and take top N
    queries_to_process.sort(key=lambda q: q.get('interval_avg_elapsed_time_ms', 0), reverse=True)
    queries_to_process = queries_to_process[:top_n]
    
    # Step 4: Cleanup stale state entries
    interval_calculator.cleanup_stale_entries(now)
    
    # Step 5: Correlate each query with wait events, blocking sessions, execution plans
    correlated_results = []
    
    for query in queries_to_process:
        query_id = query.get('query_id', '')
        database_name = query.get('database_name', '')
        query_text = query.get('query_text', '')
        
        # Get correlated metrics
        wait_events = get_wait_events_for_query(cursor, query_id, database_name)
        blocking_sessions = get_blocking_sessions_for_query(cursor, query_text, database_name, query_id)
        execution_plan = get_execution_plan_for_query(cursor, query_text, database_name)
        
        # Build result matching Oracle receiver output structure
        result = {
            'query_id': query_id,
            'database_name': database_name,
            'query_text': query_text,
            'collection_timestamp': str(query.get('collection_timestamp')),
            'last_active_time': str(query.get('last_active_time')),
            
            # Cumulative metrics (historical, from MySQL digest table)
            'cumulative_metrics': {
                'execution_count': float(query.get('execution_count', 0)),
                'total_elapsed_time_ms': float(query.get('total_elapsed_time_ms', 0)),
            },
            
            # Interval metrics (delta-based, calculated by interval calculator)
            'interval_metrics': {
                'interval_avg_elapsed_time_ms': query.get('interval_avg_elapsed_time_ms', 0),
                'interval_execution_count': query.get('interval_execution_count', 0),
                'is_first_scrape': query.get('is_first_scrape', False),
            },
            
            # Average metrics (historical cumulative averages - matches Oracle pattern)
            'average_metrics': {
                'avg_elapsed_time_ms': float(query.get('avg_elapsed_time_ms', 0)),
                'avg_cpu_time_ms': float(query.get('avg_cpu_time_ms', 0)),
                'avg_lock_time_ms': float(query.get('avg_lock_time_ms', 0)),
                'avg_rows_examined': float(query.get('avg_rows_examined', 0)),
                'avg_rows_affected': float(query.get('avg_rows_affected', 0)),
            },
            
            # Correlated data (per-query correlation like Oracle receiver)
            'wait_events': wait_events,
            'blocking_sessions': blocking_sessions,
            'execution_plan': execution_plan,
        }
        
        correlated_results.append(result)
    
    # Get cache statistics
    cache_stats = interval_calculator.get_cache_stats()
    
    return {
        'timestamp': now.isoformat(),
        'collection_interval_seconds': interval_seconds,
        'query_count': len(correlated_results),
        'cache_stats': cache_stats,
        'queries': correlated_results,
    }


def main():
    """Main execution function"""
    try:
        # Initialize interval calculator (matches Oracle receiver's NewOracleIntervalCalculator)
        interval_calculator = IntervalCalculator(cache_ttl_minutes=CACHE_TTL_MINUTES)
        
        # Connect to MySQL
        connection = mysql.connector.connect(**MYSQL_CONFIG)
        cursor = connection.cursor()
        
        # Scrape slow queries with interval-based delta calculation
        # Matches Oracle receiver's ScrapeSlowQueries pattern
        result = scrape_slow_queries(
            cursor=cursor,
            interval_calculator=interval_calculator,
            interval_seconds=COLLECTION_INTERVAL,
            top_n=TOP_N_QUERIES,
            threshold_ms=RESPONSE_TIME_THRESHOLD_MS,
        )
        
        # Output JSON
        print(json.dumps(result, default=decimal_default, indent=2))
        
        cursor.close()
        connection.close()
        
        return 0
        
    except mysql.connector.Error as err:
        print(json.dumps({
            'error': f'MySQL Error: {err}',
            'timestamp': datetime.now(timezone.utc).isoformat(),
        }, indent=2), file=sys.stderr)
        return 1
    
    except Exception as e:
        print(json.dumps({
            'error': f'Unexpected error: {e}',
            'timestamp': datetime.now(timezone.utc).isoformat(),
        }, indent=2), file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())
