package scrapers

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicsqlserverreceiver/models"
)

// TestIntervalCalculatorNewMetrics tests the new metrics (worker time, rows, reads, writes, wait time)
func TestIntervalCalculatorNewMetrics(t *testing.T) {
	logger := zap.NewNop()
	now := time.Now()

	tests := []struct {
		name                          string
		firstScrapeQuery              models.SlowQuery
		secondScrapeQuery             models.SlowQuery
		expectedFirstScrapeMetrics    SimplifiedIntervalMetrics
		expectedSecondScrapeMetrics   SimplifiedIntervalMetrics
		expectedIntervalWorkerTime    int64
		expectedIntervalRows          int64
		expectedIntervalLogicalReads  int64
		expectedIntervalPhysicalReads int64
		expectedIntervalLogicalWrites int64
		expectedIntervalWaitTime      int64
	}{
		{
			name: "First scrape - all new metrics",
			firstScrapeQuery: models.SlowQuery{
				QueryID:            ptrQueryID("0x1111111111111111"),
				PlanHandle:         ptrQueryID("0xAAAAAAAAAAAAAAAA"),
				ExecutionCount:     ptrInt64(100),
				TotalElapsedTimeMS: ptrFloat64(10000.0), // 10 seconds
				TotalWorkerTimeMS:  ptrFloat64(8000.0),  // 8 seconds CPU
				TotalRows:          ptrInt64(5000),
				TotalLogicalReads:  ptrInt64(10000),
				TotalPhysicalReads: ptrInt64(1000),
				TotalLogicalWrites: ptrInt64(500),
			},
			expectedFirstScrapeMetrics: SimplifiedIntervalMetrics{
				IntervalExecutionCount:     100,
				HistoricalExecutionCount:   100,
				IntervalWorkerTimeMs:       8000,
				IntervalAvgWorkerTimeMs:    80.0,     // 8000 / 100
				IntervalRows:               5000,
				IntervalAvgRows:            50.0,     // 5000 / 100
				IntervalLogicalReads:       10000,
				IntervalAvgLogicalReads:    100.0,    // 10000 / 100
				IntervalPhysicalReads:      1000,
				IntervalAvgPhysicalReads:   10.0,     // 1000 / 100
				IntervalLogicalWrites:      500,
				IntervalAvgLogicalWrites:   5.0,      // 500 / 100
				IntervalWaitTimeMs:         2000,     // 10000 - 8000
				IntervalAvgWaitTimeMs:      20.0,     // 2000 / 100
				HistoricalWorkerTimeMs:     8000,
				HistoricalRows:             5000,
				HistoricalLogicalReads:     10000,
				HistoricalPhysicalReads:    1000,
				HistoricalLogicalWrites:    500,
				HistoricalWaitTimeMs:       2000,
				IsFirstScrape:              true,
				HasNewExecutions:           true,
			},
		},
		{
			name: "Delta calculation for all metrics",
			firstScrapeQuery: models.SlowQuery{
				QueryID:            ptrQueryID("0x2222222222222222"),
				PlanHandle:         ptrQueryID("0xBBBBBBBBBBBBBBBB"),
				ExecutionCount:     ptrInt64(100),
				TotalElapsedTimeMS: ptrFloat64(10000.0),
				TotalWorkerTimeMS:  ptrFloat64(8000.0),
				TotalRows:          ptrInt64(5000),
				TotalLogicalReads:  ptrInt64(10000),
				TotalPhysicalReads: ptrInt64(1000),
				TotalLogicalWrites: ptrInt64(500),
			},
			expectedFirstScrapeMetrics: SimplifiedIntervalMetrics{
				IntervalExecutionCount:     100,
				HistoricalExecutionCount:   100,
				IntervalWorkerTimeMs:       8000,
				IntervalAvgWorkerTimeMs:    80.0,
				IntervalRows:               5000,
				IntervalAvgRows:            50.0,
				IntervalLogicalReads:       10000,
				IntervalAvgLogicalReads:    100.0,
				IntervalPhysicalReads:      1000,
				IntervalAvgPhysicalReads:   10.0,
				IntervalLogicalWrites:      500,
				IntervalAvgLogicalWrites:   5.0,
				IntervalWaitTimeMs:         2000,
				IntervalAvgWaitTimeMs:      20.0,
				HistoricalWorkerTimeMs:     8000,
				HistoricalRows:             5000,
				HistoricalLogicalReads:     10000,
				HistoricalPhysicalReads:    1000,
				HistoricalLogicalWrites:    500,
				HistoricalWaitTimeMs:       2000,
				IsFirstScrape:              true,
				HasNewExecutions:           true,
			},
			secondScrapeQuery: models.SlowQuery{
				QueryID:            ptrQueryID("0x2222222222222222"),
				PlanHandle:         ptrQueryID("0xBBBBBBBBBBBBBBBB"),
				ExecutionCount:     ptrInt64(120),      // +20 executions
				TotalElapsedTimeMS: ptrFloat64(14000.0), // +4000 ms
				TotalWorkerTimeMS:  ptrFloat64(11000.0), // +3000 ms CPU
				TotalRows:          ptrInt64(6200),     // +1200 rows
				TotalLogicalReads:  ptrInt64(14000),    // +4000 reads
				TotalPhysicalReads: ptrInt64(1400),     // +400 reads
				TotalLogicalWrites: ptrInt64(700),      // +200 writes
			},
			expectedIntervalWorkerTime:    3000,  // 11000 - 8000
			expectedIntervalRows:          1200,  // 6200 - 5000
			expectedIntervalLogicalReads:  4000,  // 14000 - 10000
			expectedIntervalPhysicalReads: 400,   // 1400 - 1000
			expectedIntervalLogicalWrites: 200,   // 700 - 500
			expectedIntervalWaitTime:      1000,  // (14000-11000) - (10000-8000) = 3000 - 2000
			expectedSecondScrapeMetrics: SimplifiedIntervalMetrics{
				IntervalExecutionCount:     20,
				HistoricalExecutionCount:   120,
				IntervalWorkerTimeMs:       3000,
				IntervalAvgWorkerTimeMs:    150.0,   // 3000 / 20
				IntervalRows:               1200,
				IntervalAvgRows:            60.0,    // 1200 / 20
				IntervalLogicalReads:       4000,
				IntervalAvgLogicalReads:    200.0,   // 4000 / 20
				IntervalPhysicalReads:      400,
				IntervalAvgPhysicalReads:   20.0,    // 400 / 20
				IntervalLogicalWrites:      200,
				IntervalAvgLogicalWrites:   10.0,    // 200 / 20
				IntervalWaitTimeMs:         1000,    // delta_wait = delta_elapsed - delta_worker = 4000 - 3000
				IntervalAvgWaitTimeMs:      50.0,    // 1000 / 20
				HistoricalWorkerTimeMs:     11000,
				HistoricalRows:             6200,
				HistoricalLogicalReads:     14000,
				HistoricalPhysicalReads:    1400,
				HistoricalLogicalWrites:    700,
				HistoricalWaitTimeMs:       3000,   // 14000 - 11000
				IsFirstScrape:              false,
				HasNewExecutions:           true,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a new calculator for each test to ensure clean state
			calc := NewSimplifiedIntervalCalculator(logger, 10*time.Minute)

			// First scrape
			metrics1 := calc.CalculateMetrics(&tt.firstScrapeQuery, now)
			assert.NotNil(t, metrics1, "First scrape metrics should not be nil")

			// Verify first scrape metrics
			assert.Equal(t, tt.expectedFirstScrapeMetrics.IntervalWorkerTimeMs, metrics1.IntervalWorkerTimeMs, "First scrape: IntervalWorkerTimeMs")
			assert.InDelta(t, tt.expectedFirstScrapeMetrics.IntervalAvgWorkerTimeMs, metrics1.IntervalAvgWorkerTimeMs, 0.01, "First scrape: IntervalAvgWorkerTimeMs")
			assert.Equal(t, tt.expectedFirstScrapeMetrics.IntervalRows, metrics1.IntervalRows, "First scrape: IntervalRows")
			assert.InDelta(t, tt.expectedFirstScrapeMetrics.IntervalAvgRows, metrics1.IntervalAvgRows, 0.01, "First scrape: IntervalAvgRows")
			assert.Equal(t, tt.expectedFirstScrapeMetrics.IntervalLogicalReads, metrics1.IntervalLogicalReads, "First scrape: IntervalLogicalReads")
			assert.InDelta(t, tt.expectedFirstScrapeMetrics.IntervalAvgLogicalReads, metrics1.IntervalAvgLogicalReads, 0.01, "First scrape: IntervalAvgLogicalReads")
			assert.Equal(t, tt.expectedFirstScrapeMetrics.IntervalPhysicalReads, metrics1.IntervalPhysicalReads, "First scrape: IntervalPhysicalReads")
			assert.InDelta(t, tt.expectedFirstScrapeMetrics.IntervalAvgPhysicalReads, metrics1.IntervalAvgPhysicalReads, 0.01, "First scrape: IntervalAvgPhysicalReads")
			assert.Equal(t, tt.expectedFirstScrapeMetrics.IntervalLogicalWrites, metrics1.IntervalLogicalWrites, "First scrape: IntervalLogicalWrites")
			assert.InDelta(t, tt.expectedFirstScrapeMetrics.IntervalAvgLogicalWrites, metrics1.IntervalAvgLogicalWrites, 0.01, "First scrape: IntervalAvgLogicalWrites")
			assert.Equal(t, tt.expectedFirstScrapeMetrics.IntervalWaitTimeMs, metrics1.IntervalWaitTimeMs, "First scrape: IntervalWaitTimeMs")
			assert.InDelta(t, tt.expectedFirstScrapeMetrics.IntervalAvgWaitTimeMs, metrics1.IntervalAvgWaitTimeMs, 0.01, "First scrape: IntervalAvgWaitTimeMs")

			// Historical metrics
			assert.Equal(t, tt.expectedFirstScrapeMetrics.HistoricalWorkerTimeMs, metrics1.HistoricalWorkerTimeMs, "First scrape: HistoricalWorkerTimeMs")
			assert.Equal(t, tt.expectedFirstScrapeMetrics.HistoricalRows, metrics1.HistoricalRows, "First scrape: HistoricalRows")
			assert.Equal(t, tt.expectedFirstScrapeMetrics.HistoricalLogicalReads, metrics1.HistoricalLogicalReads, "First scrape: HistoricalLogicalReads")
			assert.Equal(t, tt.expectedFirstScrapeMetrics.HistoricalPhysicalReads, metrics1.HistoricalPhysicalReads, "First scrape: HistoricalPhysicalReads")
			assert.Equal(t, tt.expectedFirstScrapeMetrics.HistoricalLogicalWrites, metrics1.HistoricalLogicalWrites, "First scrape: HistoricalLogicalWrites")
			assert.Equal(t, tt.expectedFirstScrapeMetrics.HistoricalWaitTimeMs, metrics1.HistoricalWaitTimeMs, "First scrape: HistoricalWaitTimeMs")

			// If there's a second scrape, test delta calculation
			if tt.secondScrapeQuery.ExecutionCount != nil {
				now2 := now.Add(15 * time.Second)
				metrics2 := calc.CalculateMetrics(&tt.secondScrapeQuery, now2)
				assert.NotNil(t, metrics2, "Second scrape metrics should not be nil")

				// Verify interval deltas
				assert.Equal(t, tt.expectedIntervalWorkerTime, metrics2.IntervalWorkerTimeMs, "Second scrape: IntervalWorkerTimeMs delta")
				assert.Equal(t, tt.expectedIntervalRows, metrics2.IntervalRows, "Second scrape: IntervalRows delta")
				assert.Equal(t, tt.expectedIntervalLogicalReads, metrics2.IntervalLogicalReads, "Second scrape: IntervalLogicalReads delta")
				assert.Equal(t, tt.expectedIntervalPhysicalReads, metrics2.IntervalPhysicalReads, "Second scrape: IntervalPhysicalReads delta")
				assert.Equal(t, tt.expectedIntervalLogicalWrites, metrics2.IntervalLogicalWrites, "Second scrape: IntervalLogicalWrites delta")
				assert.Equal(t, tt.expectedIntervalWaitTime, metrics2.IntervalWaitTimeMs, "Second scrape: IntervalWaitTimeMs delta")

				// Verify interval averages
				assert.InDelta(t, tt.expectedSecondScrapeMetrics.IntervalAvgWorkerTimeMs, metrics2.IntervalAvgWorkerTimeMs, 0.01, "Second scrape: IntervalAvgWorkerTimeMs")
				assert.InDelta(t, tt.expectedSecondScrapeMetrics.IntervalAvgRows, metrics2.IntervalAvgRows, 0.01, "Second scrape: IntervalAvgRows")
				assert.InDelta(t, tt.expectedSecondScrapeMetrics.IntervalAvgLogicalReads, metrics2.IntervalAvgLogicalReads, 0.01, "Second scrape: IntervalAvgLogicalReads")
				assert.InDelta(t, tt.expectedSecondScrapeMetrics.IntervalAvgPhysicalReads, metrics2.IntervalAvgPhysicalReads, 0.01, "Second scrape: IntervalAvgPhysicalReads")
				assert.InDelta(t, tt.expectedSecondScrapeMetrics.IntervalAvgLogicalWrites, metrics2.IntervalAvgLogicalWrites, 0.01, "Second scrape: IntervalAvgLogicalWrites")
				assert.InDelta(t, tt.expectedSecondScrapeMetrics.IntervalAvgWaitTimeMs, metrics2.IntervalAvgWaitTimeMs, 0.01, "Second scrape: IntervalAvgWaitTimeMs")

				// Verify historical metrics are updated
				assert.Equal(t, tt.expectedSecondScrapeMetrics.HistoricalWorkerTimeMs, metrics2.HistoricalWorkerTimeMs, "Second scrape: HistoricalWorkerTimeMs")
				assert.Equal(t, tt.expectedSecondScrapeMetrics.HistoricalRows, metrics2.HistoricalRows, "Second scrape: HistoricalRows")
				assert.Equal(t, tt.expectedSecondScrapeMetrics.HistoricalLogicalReads, metrics2.HistoricalLogicalReads, "Second scrape: HistoricalLogicalReads")
				assert.Equal(t, tt.expectedSecondScrapeMetrics.HistoricalPhysicalReads, metrics2.HistoricalPhysicalReads, "Second scrape: HistoricalPhysicalReads")
				assert.Equal(t, tt.expectedSecondScrapeMetrics.HistoricalLogicalWrites, metrics2.HistoricalLogicalWrites, "Second scrape: HistoricalLogicalWrites")
				assert.Equal(t, tt.expectedSecondScrapeMetrics.HistoricalWaitTimeMs, metrics2.HistoricalWaitTimeMs, "Second scrape: HistoricalWaitTimeMs")
			}
		})
	}
}

// TestWaitTimeCalculation specifically tests wait time calculation (elapsed - worker)
func TestWaitTimeCalculation(t *testing.T) {
	logger := zap.NewNop()
	now := time.Now()

	tests := []struct {
		name                      string
		totalElapsed              float64
		totalWorker               float64
		expectedHistoricalWait    int64
		expectedIntervalWait      int64 // for second scrape
		secondTotalElapsed        float64
		secondTotalWorker         float64
	}{
		{
			name:                   "Equal elapsed and worker (no wait)",
			totalElapsed:           5000.0,
			totalWorker:            5000.0,
			expectedHistoricalWait: 0,
		},
		{
			name:                   "Worker less than elapsed (has wait time)",
			totalElapsed:           10000.0,
			totalWorker:            7000.0,
			expectedHistoricalWait: 3000, // 10000 - 7000
		},
		{
			name:                   "Delta wait time calculation",
			totalElapsed:           10000.0,
			totalWorker:            8000.0,
			expectedHistoricalWait: 2000,
			secondTotalElapsed:     15000.0, // +5000 elapsed
			secondTotalWorker:      12000.0, // +4000 worker
			expectedIntervalWait:   1000,    // delta_elapsed - delta_worker = 5000 - 4000
		},
		{
			name:                   "High wait time scenario (blocking)",
			totalElapsed:           20000.0,
			totalWorker:            5000.0,
			expectedHistoricalWait: 15000, // 20000 - 5000
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a new calculator for each test to ensure clean state
			calc := NewSimplifiedIntervalCalculator(logger, 10*time.Minute)

			query := models.SlowQuery{
				QueryID:            ptrQueryID("0xABCDEF123456"),
				PlanHandle:         ptrQueryID("0x123456ABCDEF"),
				ExecutionCount:     ptrInt64(100),
				TotalElapsedTimeMS: &tt.totalElapsed,
				TotalWorkerTimeMS:  &tt.totalWorker,
				TotalRows:          ptrInt64(1000),
				TotalLogicalReads:  ptrInt64(5000),
				TotalPhysicalReads: ptrInt64(500),
				TotalLogicalWrites: ptrInt64(100),
			}

			metrics := calc.CalculateMetrics(&query, now)
			assert.NotNil(t, metrics)
			assert.Equal(t, tt.expectedHistoricalWait, metrics.HistoricalWaitTimeMs, "Historical wait time should match")
			assert.Equal(t, tt.expectedHistoricalWait, metrics.IntervalWaitTimeMs, "First scrape interval wait should equal historical")

			// Test delta wait time if second scrape provided
			if tt.secondTotalElapsed > 0 {
				query2 := models.SlowQuery{
					QueryID:            ptrQueryID("0xABCDEF123456"),
					PlanHandle:         ptrQueryID("0x123456ABCDEF"),
					ExecutionCount:     ptrInt64(110),
					TotalElapsedTimeMS: &tt.secondTotalElapsed,
					TotalWorkerTimeMS:  &tt.secondTotalWorker,
					TotalRows:          ptrInt64(1100),
					TotalLogicalReads:  ptrInt64(5500),
					TotalPhysicalReads: ptrInt64(550),
					TotalLogicalWrites: ptrInt64(110),
				}

				metrics2 := calc.CalculateMetrics(&query2, now.Add(15*time.Second))
				assert.NotNil(t, metrics2)
				assert.Equal(t, tt.expectedIntervalWait, metrics2.IntervalWaitTimeMs, "Interval wait time delta should match")

				expectedHistoricalWait2 := int64(tt.secondTotalElapsed - tt.secondTotalWorker)
				assert.Equal(t, expectedHistoricalWait2, metrics2.HistoricalWaitTimeMs, "Second scrape historical wait should match")
			}
		})
	}
}

// Helper functions
func ptrInt64(i int64) *int64 {
	return &i
}

func ptrFloat64(f float64) *float64 {
	return &f
}

func ptrQueryID(s string) *models.QueryID {
	qid := models.QueryID(s)
	return &qid
}
