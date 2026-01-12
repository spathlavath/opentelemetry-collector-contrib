# How the Extension Pattern Works - Detailed Explanation

## Your Questions Answered

### Question 1: How is the Metrics Pipeline Executing?

The metrics pipeline runs **independently** every `collection_interval` (60 seconds in your config).

#### Execution Flow:

```
Time: T=0s (Collector starts both pipelines)
  ↓
Metrics Pipeline Thread:
  ├─ T=0s:  Start scrape() function
  ├─ T=0s:  Query dm_exec_query_stats → Get slow queries
  ├─ T=2s:  Query dm_exec_requests → Get active queries
  ├─ T=4s:  Process data, extract plan handles
  ├─ T=5s:  ✅ Write to extension cache
  ├─ T=6s:  Emit metrics to processor → exporter
  ├─ T=60s: Sleep until next interval
  └─ T=60s: Repeat (start next scrape)

Logs Pipeline Thread (SEPARATE):
  ├─ T=0s:  Start ScrapeLogs() function
  ├─ T=0s:  ✅ Read from extension cache (INSTANT)
  ├─ T=0s:  Cache empty? Skip and wait
  ├─ T=60s: Start ScrapeLogs() again
  ├─ T=60s: ✅ Read from cache (NOW HAS DATA!)
  ├─ T=61s: Query dm_exec_query_plan → Get XML only
  ├─ T=63s: Emit logs to processor → exporter
  └─ T=120s: Repeat
```

#### Code Location: `scraper.go:740-748`

```go
// After querying database for slow queries and active queries
if s.cacheExtension != nil && (s.config.EnableQueryMonitoring || s.config.EnableActiveRunningQueries) {
    cache := s.cacheExtension.GetOrCreateCache(s.settings.ID)
    cache.Update(slowQueryIDs, slowQueryPlanDataMap, activeQueriesForCache)  // ← WRITE
    s.logger.Info("✅ Cached query performance data in extension for logs pipeline",
        zap.Int("slow_query_ids", len(slowQueryIDs)),
        zap.Int("plan_data_entries", len(slowQueryPlanDataMap)),
        zap.Int("active_queries", len(activeQueriesForCache)))
}
```

**What gets written to cache:**
1. `slowQueryIDs` - Array of query hashes: `["0x123ABC...", "0x456DEF..."]`
2. `slowQueryPlanDataMap` - Map of query data:
   ```go
   {
     "0x123ABC": {
       PlanHandle: "0xAB12CD34",
       DatabaseName: "AdventureWorks",
       QueryText: "SELECT * FROM Orders...",
       ExecutionCount: 1500,
       TotalElapsedTime: 45000
     }
   }
   ```
3. `activeQueriesForCache` - Currently running queries with wait stats

---

### Question 2: Does Logs Pipeline Wait for Cached Data?

**Answer: NO! Logs pipeline does NOT wait. It checks instantly and skips if data is not available.**

#### Execution Flow:

```go
// scraper.go:229-241
if s.cacheExtension == nil {
    s.logger.Error("❌ Extension REQUIRED")
    return logs, nil  // ← Exit immediately
}

cache := s.cacheExtension.GetCache(s.settings.ID)
if cache == nil {
    s.logger.Info("⚠️  Cache not available - metrics pipeline may not have run yet. Skipping log collection.")
    return logs, nil  // ← Exit immediately, NO waiting!
}

// If we reach here, cache HAS data
slowQueryIDs, slowQueryPlanDataMap, activeQueries = cache.GetAll()  // ← Read (instant)
```

#### Detailed Timeline:

**First Collection Cycle (T=0s):**

```
T=0s: Both pipelines start simultaneously

Metrics Pipeline:
├─ T=0s:  Query database
├─ T=5s:  Write to cache ← Cache NOW populated
└─ T=6s:  Emit metrics

Logs Pipeline (running in parallel):
├─ T=0s:  Check cache → EMPTY!
├─ T=0s:  Log: "⚠️  Cache not available"
└─ T=0s:  EXIT (no logs emitted)
```

**Second Collection Cycle (T=60s):**

```
T=60s: Both pipelines start again

Metrics Pipeline:
├─ T=60s: Query database
├─ T=65s: Update cache with NEW data
└─ T=66s: Emit metrics

Logs Pipeline:
├─ T=60s: Check cache → HAS DATA! ✅
├─ T=60s: Read from cache (instant)
├─ T=61s: Query only dm_exec_query_plan
└─ T=63s: Emit logs
```

**Key Points:**

1. ❌ **NO blocking/waiting** - Logs pipeline never blocks
2. ✅ **Instant check** - `GetCache()` is a simple map lookup (microseconds)
3. ⚠️ **First run skipped** - Expected behavior on startup
4. ✅ **Second run works** - Cache has data from first metrics run

---

### Question 3: What is the Extension Doing Exactly?

The extension is a **thread-safe in-memory storage** that acts as a bridge between pipelines.

#### Extension Code: `extension/querycache/extension.go`

```go
type Extension struct {
    mu       sync.RWMutex  // Thread-safe lock
    caches   map[component.ID]*QueryPerformanceCache  // Storage: receiverID → cache
    settings component.TelemetrySettings
}

// Called by metrics pipeline
func (e *Extension) GetOrCreateCache(receiverID component.ID) *QueryPerformanceCache {
    e.mu.Lock()
    defer e.mu.Unlock()

    cache, exists := e.caches[receiverID]
    if !exists {
        cache = NewQueryPerformanceCache()  // Create new cache
        e.caches[receiverID] = cache        // Store in map
    }
    return cache
}

// Called by logs pipeline
func (e *Extension) GetCache(receiverID component.ID) *QueryPerformanceCache {
    e.mu.RLock()
    defer e.mu.RUnlock()

    return e.caches[receiverID]  // Simple map lookup
}
```

#### What Extension Stores:

```
Extension (Singleton in Collector)
  │
  ├─ caches: map[component.ID]*Cache
  │    │
  │    ├─ "newrelicsqlserver" → Cache Instance 1
  │    │                           ├─ slowQueryIDs: []string
  │    │                           ├─ slowQueryPlanDataMap: map[string]SlowQueryPlanData
  │    │                           ├─ activeQueries: []ActiveRunningQuery
  │    │                           └─ lastUpdateTime: time.Time
  │    │
  │    └─ "newrelicsqlserver/db2" → Cache Instance 2 (if monitoring multiple DBs)
  │                                   └─ ... (separate data)
```

#### Extension Lifecycle:

```
Collector Startup:
  ├─ Extension.Start() called
  │   └─ Initialize empty caches map
  │
  ├─ Metrics Receiver.Start() called
  │   └─ Lookup extension: host.GetExtensions()
  │       └─ Type assertion: ext.(*querycache.Extension)
  │           └─ Store reference: s.cacheExtension = ext
  │
  └─ Logs Receiver.Start() called
      └─ Lookup extension (same process)

Every 60 seconds (Metrics):
  ├─ scrape() function runs
  ├─ Query database
  ├─ cache = extension.GetOrCreateCache(receiverID)
  └─ cache.Update(data)  ← WRITE

Every 60 seconds (Logs):
  ├─ ScrapeLogs() function runs
  ├─ cache = extension.GetCache(receiverID)
  └─ data = cache.GetAll()  ← READ

Collector Shutdown:
  └─ Extension.Shutdown() called
      └─ Clear all caches from memory
```

---

## Complete Data Flow Diagram

```
┌─────────────────────────────────────────────────────────────────────┐
│                     OpenTelemetry Collector                          │
│                                                                      │
│  ┌────────────────────────────────────────────────────────────┐    │
│  │  Extension: querycache (SINGLETON)                         │    │
│  │  Lifecycle: Managed by collector                           │    │
│  │                                                             │    │
│  │  Storage: map[receiverID]*QueryPerformanceCache            │    │
│  │    {                                                        │    │
│  │      "newrelicsqlserver": {                                │    │
│  │        slowQueryIDs: ["0x123", "0x456"],                   │    │
│  │        slowQueryPlanDataMap: {                             │    │
│  │          "0x123": {PlanHandle, QueryText, Stats...}        │    │
│  │        },                                                   │    │
│  │        activeQueries: [...],                               │    │
│  │        lastUpdateTime: 2026-01-12T15:00:00Z                │    │
│  │      }                                                      │    │
│  │    }                                                        │    │
│  └────────────────────────────────────────────────────────────┘    │
│                         ▲                  │                        │
│                         │                  │                        │
│                    WRITE│             READ │                        │
│                         │                  ▼                        │
│  ┌──────────────────────┴─────┐    ┌──────┴────────────────┐      │
│  │  Metrics Pipeline          │    │  Logs Pipeline         │      │
│  │  (Receiver Instance 1)     │    │  (Receiver Instance 2) │      │
│  │                            │    │                        │      │
│  │  Every 60s:                │    │  Every 60s:            │      │
│  │  1. scrape()               │    │  1. ScrapeLogs()       │      │
│  │  2. Query dm_exec_*        │    │  2. GetCache()         │      │
│  │     (2 queries)            │    │  3. cache.GetAll()     │      │
│  │  3. GetOrCreateCache()     │    │     (instant read)     │      │
│  │  4. cache.Update()         │    │  4. Query plans only   │      │
│  │  5. Emit metrics           │    │     (1 query)          │      │
│  │                            │    │  5. Emit logs          │      │
│  └────────────────────────────┘    └────────────────────────┘      │
│           │                                    │                    │
│           ▼                                    ▼                    │
│  ┌────────────────┐                  ┌────────────────┐           │
│  │  Processors    │                  │  Processors    │           │
│  │  (batch, etc)  │                  │  (batch, etc)  │           │
│  └────────┬───────┘                  └────────┬───────┘           │
│           ▼                                    ▼                    │
│  ┌────────────────┐                  ┌────────────────┐           │
│  │  Exporters     │                  │  Exporters     │           │
│  │  (otlphttp)    │                  │  (otlphttp)    │           │
│  └────────────────┘                  └────────────────┘           │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Memory Access Pattern

**Thread-Safety Guaranteed:**

```go
// Metrics Pipeline (Thread 1) - WRITE
extension.GetOrCreateCache(receiverID)
  ↓
cache.Update(data)
  ↓
cache.mu.Lock()           // ← Acquire WRITE lock
cache.slowQueryIDs = data // ← Safe write
cache.mu.Unlock()         // ← Release lock

// Logs Pipeline (Thread 2) - READ (concurrent!)
extension.GetCache(receiverID)
  ↓
cache.GetAll()
  ↓
cache.mu.RLock()          // ← Acquire READ lock (doesn't block other reads)
data = cache.slowQueryIDs // ← Safe read
cache.mu.RUnlock()        // ← Release lock
```

**Multiple readers can read simultaneously, but writers block everything.**

---

## Performance Impact

### Before Extension:

```
Every 60s:
  Metrics: dm_exec_query_stats + dm_exec_requests + emit metrics
  Logs:    dm_exec_query_stats + dm_exec_requests + dm_exec_query_plan + emit logs

  Total: 5 queries per cycle
```

### After Extension:

```
Every 60s:
  Metrics: dm_exec_query_stats + dm_exec_requests + cache.Update() + emit metrics
  Logs:    cache.GetAll() (NO QUERY!) + dm_exec_query_plan + emit logs

  Total: 3 queries per cycle
```

**Cache operations take microseconds (µs), database queries take milliseconds (ms).**

---

## Summary

1. **Metrics pipeline executes independently** - Queries database every 60s, writes to extension cache
2. **Logs pipeline does NOT wait** - Checks cache instantly, skips if empty (first run), reads if available (subsequent runs)
3. **Extension is thread-safe shared memory** - Acts as a bridge storing query data in a map, accessed by both pipelines

**Key Insight:** The extension eliminates the need for logs pipeline to query `dm_exec_query_stats` and `dm_exec_requests` because metrics pipeline already did it and stored the results!
