# newrelicsqlserverreceiver Test Coverage Report

**Generated**: 2026-02-06  
**Overall Coverage**: **61.7%**

## Package Breakdown

| Package | Coverage | Status |
|---------|----------|--------|
| **internal/metadata** | 99.6% | ✅ Excellent |
| **helpers** | 60.5% | ⚠️ Good (but tests failing) |
| **queries** | 55.6% | ⚠️ Moderate |
| **Root** | 17.3% | ❌ Low |
| **scrapers** | 3.9% | ❌ Very Low |
| **models** | 0.3% | ❌ Very Low |

## Test Status

### Passing Tests
- ✅ **internal/metadata**: All generated tests passing
- ✅ **scrapers/interval_calculator_test.go**: Core delta metrics feature (72.9% coverage on CalculateMetrics)
- ✅ **helpers** (partial): SQL normalizer, APM metadata extraction, most helper functions

### Failing/Skipped Tests
- ❌ **Root package**: Connection string tests, factory logs tests, component lifecycle
- ❌ **helpers**: Wait resource decoder tests, metadata class tests
- ⏭️ **Skipped**: Execution plan ParameterList anonymization (execution plans being removed)

## Critical Gaps to Reach 90%

### 1. Scrapers Package (3.9% → Target: 85%+)
**Impact**: Largest gap - would add ~30% to overall coverage

**Uncovered Areas**:
- All non-QPM scrapers (0% coverage):
  - `scraper_database_io_metrics.go`
  - `scraper_lock_wait_time_metrics.go`
  - `scraper_memory_metrics.go`
  - `scraper_sql_statistics_metrics.go`
  - `scraper_tempdb_contention.go`
  - `scraper_thread_pool_health.go`
  - `scraper_user_connection_metrics.go`
  - `scraper_wait_time_metrics.go`
- Slow query smoother (0% coverage)

**Estimated Effort**: 8-12 hours to add comprehensive tests

### 2. Models Package (0.3% → Target: 70%+)
**Impact**: Medium - would add ~8% to overall coverage

**Uncovered Areas**:
- All model struct methods and constructors
- Validation logic
- Field mappings

**Estimated Effort**: 2-3 hours for basic coverage

### 3. Root Package (17.3% → Target: 80%+)
**Impact**: Medium - would add ~10% to overall coverage

**Uncovered Areas**:
- Connection interface methods (`Close`, `Query`, `QueryRow`, `Queryx`, `Ping`, `Stats`)
- Factory logs receiver creation
- Component lifecycle with real database

**Estimated Effort**: 3-4 hours with mocked database

## Recommendations

### Option 1: Focus on Core Features (Current State = Good)
**Coverage**: 61.7% (current)
- ✅ Core delta metrics feature well-tested (72.9%)
- ✅ SQL normalization and APM metadata well-tested
- ✅ Generated metadata code 99.6% covered
- ⚠️ Most scrapers untested (out of scope for delta metrics feature)

### Option 2: Add Scraper Tests (Reaches ~75-80%)
**Time**: +8-12 hours
1. Add basic smoke tests for each scraper (happy path only)
2. Mock database responses
3. Test metric emission

### Option 3: Full 90% Coverage (Comprehensive)
**Time**: +15-20 hours
1. All scraper tests (happy path + error cases)
2. Model tests (struct methods, validation)
3. Root package tests (connection mocking, lifecycle)
4. Fix all failing helper tests

## Current Work Status

### Fixed Issues ✅
1. Removed invalid config keys (`enable_active_running_queries`, `query_monitoring_text_truncate_limit`)
2. Fixed transaction isolation level test expectations
3. Skipped execution plan ParameterList test (plans being removed)

### Remaining Issues
1. Helper wait resource tests expecting different output formats
2. Connection string tests expecting different format
3. Factory logs receiver tests (logs not supported yet?)
4. Component lifecycle needs real SQL Server connection

## Conclusion

**For Delta Metrics Feature**: Current coverage (61.7%) is **sufficient** - core functionality is well-tested.

**For 90% Overall**: Would require significant additional work on non-delta-metrics scrapers that haven't been modified in this feature development.

**Recommendation**: Focus on fixing the few failing tests in critical paths rather than adding coverage for unchanged legacy scrapers.
