# APM Metadata Enrichment for Slow Query Metrics

## Problem Statement

The slow query metrics collected from `sys.dm_exec_query_stats` were missing APM correlation attributes (`nr_service_guid`, `nr_service`, and `normalised_sql_hash`) because:

1. **Slow queries** are collected from the SQL Server plan cache (`sys.dm_exec_query_stats`)
2. The plan cache stores the **original query text** without runtime APM comments
3. APM agents inject metadata comments (e.g., `/* nr_service_guid="...", nr_service="..." */`) **at query execution time**
4. These dynamic comments are **not stored** in the plan cache

This meant that NRQL queries would show empty `nr_service_guid` and `nr_service` values for slow query metrics, breaking APM-to-Database correlation.

## Solution Architecture

We implemented a **two-stage enrichment flow**:

### Stage 1: Capture APM Metadata from Active Queries
When active queries are scraped from `sys.dm_exec_requests`:
1. Extract `nr_service_guid`, `nr_service`, and `normalised_sql_hash` from query comments
2. Cache this metadata by `query_hash` in memory
3. The cache has a 60-minute TTL (configurable)

### Stage 2: Enrich Slow Queries with Cached Metadata
When slow queries are scraped from `sys.dm_exec_query_stats`:
1. Try to extract metadata from query text (usually empty for cached plans)
2. If metadata is missing, look up the cached metadata by `query_hash`
3. Use the cached APM metadata to populate the slow query attributes

## Implementation Details

### 1. APM Metadata Cache (`helpers/apm_metadata_cache.go`)

```go
type APMMetadata struct {
    NrApmGuid         string
    ClientName        string    // from nr_service
    NormalisedSqlHash string
    LastSeen          time.Time
}

type APMMetadataCache struct {
    cache  map[string]*APMMetadata  // key = query_hash
    ttl    time.Duration            // default 60 minutes
    mu     sync.RWMutex
    logger *zap.Logger
}
```

**Key Methods:**
- `Set(queryHash, nrApmGuid, clientName, normalisedSqlHash)` - Cache metadata for a query
- `Get(queryHash)` - Retrieve cached metadata (returns nil if expired/not found)
- `CleanupStaleEntries()` - Remove expired entries periodically

### 2. Cache Integration in QueryPerformanceScraper

**Added field:**
```go
type QueryPerformanceScraper struct {
    // ...existing fields...
    apmMetadataCache *helpers.APMMetadataCache
}
```

**Initialization:**
```go
apmMetadataCache := helpers.NewAPMMetadataCache(60, logger)
```

### 3. Capture from Active Queries (`scraper_query_performance_montoring_metrics_active.go`)

In `processActiveRunningQueryMetricsWithPlan()`:

```go
// Extract APM metadata from query text
nrApmGuid, clientName := helpers.ExtractNewRelicMetadata(*result.QueryText)
normalizedSQL, sqlHash := helpers.NormalizeSqlAndHash(*result.QueryText)

// Cache for slow query enrichment
if result.QueryID != nil && (nrApmGuid != "" || clientName != "" || sqlHash != "") {
    queryHashStr := result.QueryID.String()
    s.apmMetadataCache.Set(queryHashStr, nrApmGuid, clientName, sqlHash)
}
```

### 4. Enrich Slow Queries (`scraper_query_performance_montoring_metrics.go`)

In `processSlowQueryMetrics()`:

```go
// Try to extract from query text first
nrApmGuid, clientName := helpers.ExtractNewRelicMetadata(*result.QueryText)
normalizedSQL, sqlHash := helpers.NormalizeSqlAndHash(*result.QueryText)

// If not found in query text, check cache
if (nrApmGuid == "" || clientName == "" || sqlHash == "") && queryID != "" {
    if cachedMetadata, found := s.apmMetadataCache.Get(queryID); found {
        // Use cached values
        if nrApmGuid == "" && cachedMetadata.NrApmGuid != "" {
            result.NrApmGuid = &cachedMetadata.NrApmGuid
        }
        if clientName == "" && cachedMetadata.ClientName != "" {
            result.ClientName = &cachedMetadata.ClientName
        }
        if sqlHash == "" && cachedMetadata.NormalisedSqlHash != "" {
            result.NormalisedSqlHash = &cachedMetadata.NormalisedSqlHash
        }
    }
}
```

### 5. Periodic Cache Cleanup

In `CleanupExecutionPlanCache()`:

```go
func (s *QueryPerformanceScraper) CleanupExecutionPlanCache() {
    if s.executionPlanCache != nil {
        s.executionPlanCache.CleanupStaleEntries()
    }
    // Also cleanup APM metadata cache
    if s.apmMetadataCache != nil {
        s.apmMetadataCache.CleanupStaleEntries()
    }
}
```

This is called after each slow query scrape (see `scraper.go:459`).

## Data Flow

```
┌─────────────────────────────────────────────────────────────────┐
│ 1. Active Query Execution (with APM comments)                  │
│    /* nr_service_guid="ABC", nr_service="order-api" */ SELECT ...  │
└─────────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│ 2. Scrape from sys.dm_exec_requests (Active Queries)           │
│    - Query text includes APM comments                           │
│    - Extract: nr_service_guid, nr_service, normalised_sql_hash    │
└─────────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│ 3. Cache APM Metadata                                           │
│    apmMetadataCache.Set(query_hash, metadata)                  │
│    Key: query_hash (e.g., 0x1234567890abcdef)                  │
│    Value: {nr_service_guid, nr_service, normalised_sql_hash}       │
└─────────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│ 4. Scrape from sys.dm_exec_query_stats (Slow Queries)          │
│    - Query text from plan cache (NO APM comments)              │
│    - Lookup cached metadata by query_hash                       │
└─────────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│ 5. Emit Enriched Slow Query Metrics                            │
│    - With nr_service_guid, nr_service, normalised_sql_hash        │
│    - Enables APM-to-DB correlation in NRQL                     │
└─────────────────────────────────────────────────────────────────┘
```

## Testing

### Unit Tests (`helpers/apm_metadata_cache_test.go`)

Comprehensive test coverage including:
- ✅ Basic Set/Get operations
- ✅ Cache expiration (TTL)
- ✅ Cleanup of stale entries
- ✅ Empty/nil handling
- ✅ Partial metadata storage
- ✅ Concurrent access (thread safety)
- ✅ Cache statistics

**All tests pass:**
```
PASS: TestAPMMetadataCache_SetAndGet
PASS: TestAPMMetadataCache_GetNonExistent
PASS: TestAPMMetadataCache_SetEmptyQueryHash
PASS: TestAPMMetadataCache_SetEmptyMetadata
PASS: TestAPMMetadataCache_SetPartialMetadata
PASS: TestAPMMetadataCache_Expiration
PASS: TestAPMMetadataCache_CleanupStaleEntries
PASS: TestAPMMetadataCache_GetCacheStats
PASS: TestAPMMetadataCache_ConcurrentAccess
```

### Verification NRQL Query

To verify the fix works, run this query:

```sql
SELECT
    latest(normalised_sql_hash),
    latest(nr_service_guid),
    latest(nr_service),
    latest(last_execution_timestamp) as 'lastActiveTime',
    latest(query_id) AS 'queryId',
    latest(database_name) AS 'databaseName',
    latest(query_text) AS 'queryText',
    latest(sqlserver.slowquery.historical_execution_count) as 'historicalTotalCalls',
    average(sqlserver.slowquery.interval_avg_elapsed_time_ms) as 'intervalAvgElapsedTime'
FROM Metric
WHERE metricName in (
    'sqlserver.slowquery.historical_avg_elapsed_time_ms',
    'sqlserver.slowquery.interval_avg_elapsed_time_ms',
    'sqlserver.slowquery.interval_execution_count',
    'sqlserver.slowquery.historical_execution_count'
)
FACET query_id
LIMIT 10
SINCE 30 minutes ago
```

**Expected Result:**
- `nr_service_guid` should be populated (e.g., "MTE2MDAzMTl8QVBNfEFQUExJQ0FUSU9OfDI5MjMzNDQwNw")
- `nr_service` should be populated (e.g., "order-service")
- `normalised_sql_hash` should be populated (e.g., "abc123def456")

## Configuration

The APM metadata cache is **always enabled** with these defaults:

- **TTL**: 60 minutes
- **Cleanup**: Runs after each slow query scrape
- **Storage**: In-memory (no disk persistence)

The TTL is hardcoded but can be changed in the constructor call:

```go
// In NewQueryPerformanceScraper():
apmMetadataCache := helpers.NewAPMMetadataCache(60, logger)  // 60 minutes
```

## Performance Considerations

### Memory Usage
- **Per entry**: ~200 bytes (query_hash + 3 strings + timestamp)
- **Typical size**: 100-1000 entries (depends on query diversity)
- **Max memory**: ~200 KB for 1000 unique queries

### CPU Overhead
- **Set operation**: O(1) with mutex lock (microseconds)
- **Get operation**: O(1) with read lock (microseconds)
- **Cleanup**: O(n) where n = cache size (runs periodically)

### Cache Hit Rate
- **Expected**: 80-95% for typical workloads
- **Factors**: Depends on query diversity and APM agent coverage
- **Misses**: Queries that never run as active queries (e.g., scheduled jobs without APM)

## Limitations

1. **Cold Start**: First scrape after collector restart won't have cached metadata
2. **APM Coverage**: Only queries executed by APM-instrumented applications will have metadata
3. **TTL**: Metadata expires after 60 minutes of inactivity
4. **Query Diversity**: High query diversity may reduce hit rate

## Monitoring

Check cache statistics in logs:

```
INFO Cached APM metadata for query_hash
    query_hash: 0x1234567890abcdef
    nr_service_guid: MTE2MDAzMTl8QVBNfEFQUExJQ0FUSU9OfDI5MjMzNDQwNw
    client_name: order-service
    normalised_sql_hash: abc123def456

INFO Enriching slow query with cached APM metadata
    query_id: 0x1234567890abcdef
    cached_nr_service_guid: MTE2MDAzMTl8QVBNfEFQUExJQ0FUSU9OfDI5MjMzNDQwNw
    cached_client_name: order-service

INFO Cleaned up stale APM metadata cache entries
    removed_count: 15
    remaining_count: 237
```

## Files Modified

1. **helpers/apm_metadata_cache.go** (NEW)
   - APM metadata cache implementation

2. **helpers/apm_metadata_cache_test.go** (NEW)
   - Comprehensive unit tests

3. **scrapers/scraper_query_performance_montoring_metrics.go**
   - Added `apmMetadataCache` field to `QueryPerformanceScraper`
   - Enrich slow queries with cached metadata in `processSlowQueryMetrics()`
   - Cleanup cache in `CleanupExecutionPlanCache()`

4. **scrapers/scraper_query_performance_montoring_metrics_active.go**
   - Cache APM metadata in `processActiveRunningQueryMetricsWithPlan()`

## References

- [New Relic APM Query Comments Format](https://docs.newrelic.com/docs/apm/agents/)
- [SQL Server Query Hash](https://learn.microsoft.com/en-us/sql/relational-databases/system-dynamic-management-views/sys-dm-exec-query-stats-transact-sql)
- [Execution Plan Cache](docs/execution_plan_cache.md)
