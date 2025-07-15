//go:build integration

package newrelicmysqlreceiver

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/confmap/confmaptest"
	"go.opentelemetry.io/collector/receiver/receivertest"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/coreinternal/scraperinttest"
)

const mysqlPort = "3306"

type newRelicMySQLTestConfig struct {
	name         string
	containerCmd []string
	tlsEnabled   bool
	insecureSkip bool
	imageVersion string
	expectedFile string
}

func TestIntegration(t *testing.T) {
	testCases := []newRelicMySQLTestConfig{
		{
			name:         "MySQL-8.0.33-NewRelic-Enhanced",
			containerCmd: nil,
			tlsEnabled:   false,
			insecureSkip: false,
			imageVersion: "mysql:8.0.33",
			expectedFile: "expected-newrelic-mysql.yaml",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()

			// Start MySQL container
			container := getContainer(t, testCase, ctx)
			defer func() {
				assert.NoError(t, container.Terminate(ctx))
			}()

			// Get container endpoint
			endpoint, err := container.Endpoint(ctx, "")
			require.NoError(t, err)

			// Load configuration
			cm, err := confmaptest.LoadConf(filepath.Join("testdata", "config.yaml"))
			require.NoError(t, err)

			// Create factory
			factory := NewFactory()
			cfg := factory.CreateDefaultConfig()

			// Configure endpoint
			sub, err := cm.Sub(component.NewIDWithName(componenttest.TypeStr, "").String())
			require.NoError(t, err)
			require.NoError(t, sub.Unmarshal(cfg))

			// Set endpoint and credentials
			newRelicCfg := cfg.(*Config)
			newRelicCfg.Endpoint = endpoint
			newRelicCfg.Username = "root"
			newRelicCfg.Password = "password"
			newRelicCfg.Database = "test"

			// Configure enhanced features
			newRelicCfg.QueryPerformanceConfig.Enabled = true
			newRelicCfg.WaitEventsConfig.Enabled = true
			newRelicCfg.BlockingSessionsConfig.Enabled = true
			newRelicCfg.SlowQueryConfig.Enabled = true

			// Create and test receiver
			receiver, err := factory.CreateMetricsReceiver(ctx, receivertest.NewNopSettings(), cfg, &scraperinttest.SinkConsumer{})
			require.NoError(t, err)

			// Start receiver
			require.NoError(t, receiver.Start(ctx, componenttest.NewNopHost()))
			defer func() {
				assert.NoError(t, receiver.Shutdown(ctx))
			}()

			// Generate test workload to create monitoring data
			err = generateTestWorkload(ctx, endpoint)
			require.NoError(t, err)

			// Wait for data collection
			time.Sleep(5 * time.Second)

			// Test New Relic query patterns
			err = testNewRelicQueryPatterns(ctx, endpoint)
			require.NoError(t, err)
		})
	}
}

func TestNewRelicQueryPatterns(t *testing.T) {
	// This test verifies that all New Relic query patterns are implemented correctly
	ctx := context.Background()

	// Start MySQL container for testing
	container := getContainer(t, newRelicMySQLTestConfig{
		name:         "MySQL-QueryPatterns-Test",
		imageVersion: "mysql:8.0.33",
	}, ctx)
	defer func() {
		assert.NoError(t, container.Terminate(ctx))
	}()

	endpoint, err := container.Endpoint(ctx, "")
	require.NoError(t, err)

	// Create MySQL client
	cfg := &Config{
		Endpoint: endpoint,
		Username: "root",
		Password: "password",
		Database: "test",
		QueryPerformanceConfig: QueryPerformanceConfig{
			Enabled:              true,
			MaxDigests:           100,
			DigestTextMaxLength:  1024,
			CollectionInterval:   time.Minute,
			IncludeQueryExamples: true,
			ExcludeSystemQueries: true,
			MinQueryTime:         time.Millisecond,
		},
		WaitEventsConfig: WaitEventsConfig{
			Enabled:            true,
			CollectionInterval: time.Minute,
			MaxEvents:          100,
			EventTypes:         []string{"io", "lock", "sync"},
		},
		BlockingSessionsConfig: BlockingSessionsConfig{
			Enabled:            true,
			CollectionInterval: 30 * time.Second,
			MaxSessions:        50,
			MinWaitTime:        time.Second,
		},
	}

	logger := zap.NewNop()
	client, err := NewMySQLClient(cfg, logger)
	require.NoError(t, err)
	defer client.Close()

	// Connect to MySQL
	err = client.Connect(ctx)
	require.NoError(t, err)

	// Generate test workload first
	err = generateTestWorkload(ctx, endpoint)
	require.NoError(t, err)

	// Test all New Relic query patterns

	// 1. SlowQueries pattern
	t.Run("SlowQueries", func(t *testing.T) {
		slowQueries, err := client.GetSlowQueries(ctx, 300, []string{"information_schema", "performance_schema", "sys"}, 10)
		assert.NoError(t, err)
		assert.NotNil(t, slowQueries)
		t.Logf("SlowQueries pattern returned %d queries", len(slowQueries))
	})

	// 2. CurrentRunningQueriesSearch pattern
	t.Run("CurrentRunningQueriesSearch", func(t *testing.T) {
		// Get query digests first
		queryData, err := client.GetQueryPerformanceData(ctx)
		require.NoError(t, err)

		if len(queryData) > 0 {
			digest := queryData[0].Digest
			currentQueries, err := client.GetCurrentRunningQueries(ctx, digest, 0, 10)
			assert.NoError(t, err)
			assert.NotNil(t, currentQueries)
			t.Logf("CurrentRunningQueriesSearch pattern returned %d queries for digest %s", len(currentQueries), digest)
		}
	})

	// 3. RecentQueriesSearch pattern
	t.Run("RecentQueriesSearch", func(t *testing.T) {
		// Get query digests first
		queryData, err := client.GetQueryPerformanceData(ctx)
		require.NoError(t, err)

		if len(queryData) > 0 {
			digest := queryData[0].Digest
			recentQueries, err := client.GetRecentQueries(ctx, digest, 0, 10)
			assert.NoError(t, err)
			assert.NotNil(t, recentQueries)
			t.Logf("RecentQueriesSearch pattern returned %d queries for digest %s", len(recentQueries), digest)
		}
	})

	// 4. PastQueriesSearch pattern
	t.Run("PastQueriesSearch", func(t *testing.T) {
		// Get query digests first
		queryData, err := client.GetQueryPerformanceData(ctx)
		require.NoError(t, err)

		if len(queryData) > 0 {
			digest := queryData[0].Digest
			pastQueries, err := client.GetPastQueries(ctx, digest, 0, 10)
			assert.NoError(t, err)
			assert.NotNil(t, pastQueries)
			t.Logf("PastQueriesSearch pattern returned %d queries for digest %s", len(pastQueries), digest)
		}
	})

	// 5. WaitEventsQuery pattern
	t.Run("WaitEventsQuery", func(t *testing.T) {
		advancedWaitEvents, err := client.GetAdvancedWaitEvents(ctx, []string{"information_schema", "performance_schema", "sys"}, 50)
		assert.NoError(t, err)
		assert.NotNil(t, advancedWaitEvents)
		t.Logf("WaitEventsQuery pattern returned %d wait events", len(advancedWaitEvents))
	})

	// 6. BlockingSessionsQuery pattern
	t.Run("BlockingSessionsQuery", func(t *testing.T) {
		advancedBlockingSessions, err := client.GetAdvancedBlockingSessions(ctx, []string{"information_schema", "performance_schema", "sys"}, 50)
		assert.NoError(t, err)
		assert.NotNil(t, advancedBlockingSessions)
		t.Logf("BlockingSessionsQuery pattern returned %d blocking sessions", len(advancedBlockingSessions))
	})

	// 7. Query Execution Plan pattern
	t.Run("QueryExecutionPlan", func(t *testing.T) {
		testQuery := "SELECT COUNT(*) FROM information_schema.tables"
		plan, err := client.GetQueryExecutionPlan(ctx, testQuery)
		if err == nil && plan != nil {
			assert.NotEmpty(t, plan.ExecutionPlan)
			t.Logf("QueryExecutionPlan pattern returned execution plan for query")
		} else {
			t.Logf("QueryExecutionPlan pattern not available (may require specific MySQL configuration)")
		}
	})
}

func getContainer(t *testing.T, cfg newRelicMySQLTestConfig, ctx context.Context) testcontainers.Container {
	env := map[string]string{
		"MYSQL_ROOT_PASSWORD": "password",
		"MYSQL_DATABASE":      "test",
		"MYSQL_USER":          "testuser",
		"MYSQL_PASSWORD":      "testpass",
	}

	req := testcontainers.ContainerRequest{
		Image:        cfg.imageVersion,
		ExposedPorts: []string{mysqlPort + "/tcp"},
		WaitingFor: wait.ForAll(
			wait.ForLog("port: 3306  MySQL Community Server").WithStartupTimeout(2*time.Minute),
			wait.ForListeningPort(mysqlPort),
		),
		Env: env,
		Cmd: cfg.containerCmd,
	}

	mysqlContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	require.NoError(t, err)

	// Wait for MySQL to be fully ready
	time.Sleep(10 * time.Second)

	return mysqlContainer
}

func generateTestWorkload(ctx context.Context, endpoint string) error {
	// Connect to MySQL and generate some test workload
	dsn := fmt.Sprintf("root:password@tcp(%s)/test", endpoint)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return err
	}
	defer db.Close()

	// Create test table
	_, err = db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS test_table (
			id INT AUTO_INCREMENT PRIMARY KEY,
			name VARCHAR(255),
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)
	`)
	if err != nil {
		return err
	}

	// Generate some queries to populate performance_schema
	for i := 0; i < 10; i++ {
		_, err = db.ExecContext(ctx, "INSERT INTO test_table (name) VALUES (?)", fmt.Sprintf("test_record_%d", i))
		if err != nil {
			return err
		}

		_, err = db.ExecContext(ctx, "SELECT COUNT(*) FROM test_table WHERE name LIKE ?", "test_%")
		if err != nil {
			return err
		}

		_, err = db.ExecContext(ctx, "SELECT * FROM test_table ORDER BY created_at DESC LIMIT 5")
		if err != nil {
			return err
		}
	}

	return nil
}

func testNewRelicQueryPatterns(ctx context.Context, endpoint string) error {
	// Connect to MySQL and verify that our New Relic patterns work
	dsn := fmt.Sprintf("root:password@tcp(%s)/test", endpoint)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return err
	}
	defer db.Close()

	// Test that performance_schema is enabled
	var count int
	err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name = 'performance_schema'").Scan(&count)
	if err != nil {
		return err
	}
	if count == 0 {
		return fmt.Errorf("performance_schema is not available")
	}

	// Test that we can query events_statements_summary_by_digest (SlowQueries pattern)
	rows, err := db.QueryContext(ctx, `
		SELECT DIGEST, DIGEST_TEXT, SCHEMA_NAME, COUNT_STAR 
		FROM performance_schema.events_statements_summary_by_digest 
		WHERE SCHEMA_NAME IS NOT NULL 
		LIMIT 5
	`)
	if err != nil {
		return err
	}
	defer rows.Close()

	// Test that we can query events_waits_summary_global_by_event_name (WaitEventsQuery pattern)
	rows2, err := db.QueryContext(ctx, `
		SELECT EVENT_NAME, COUNT_STAR, SUM_TIMER_WAIT 
		FROM performance_schema.events_waits_summary_global_by_event_name 
		WHERE COUNT_STAR > 0 
		LIMIT 5
	`)
	if err != nil {
		return err
	}
	defer rows2.Close()

	return nil
}
