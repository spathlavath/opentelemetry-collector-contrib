// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package newrelicmysqlreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicmysqlreceiver"

import (
	"context"
	"errors"
	"strconv"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/receiver"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicmysqlreceiver/internal/metadata"
)

type newRelicMySQLScraper struct {
	sqlclient client
	logger    *zap.Logger
	config    *Config
	mb        *metadata.MetricsBuilder
}

func newNewRelicMySQLScraper(
	settings receiver.Settings,
	config *Config,
) *newRelicMySQLScraper {
	return &newRelicMySQLScraper{
		logger: settings.Logger,
		config: config,
		mb:     metadata.NewMetricsBuilder(config.MetricsBuilderConfig, settings),
	}
}

// start starts the scraper by initializing the database client connection.
func (n *newRelicMySQLScraper) start(_ context.Context, _ component.Host) error {
	sqlclient, err := newMySQLClient(n.config)
	if err != nil {
		return err
	}

	err = sqlclient.Connect()
	if err != nil {
		return err
	}
	n.sqlclient = sqlclient

	return nil
}

// shutdown closes the database connection.
func (n *newRelicMySQLScraper) shutdown(context.Context) error {
	if n.sqlclient == nil {
		return nil
	}
	return n.sqlclient.Close()
}

// scrape scrapes the MySQL database metrics and transforms them.
func (n *newRelicMySQLScraper) scrape(ctx context.Context) (pmetric.Metrics, error) {
	if n.sqlclient == nil {
		return pmetric.Metrics{}, errors.New("failed to connect to database client")
	}

	n.logger.Info("Scraping New Relic MySQL metrics")

	// 1. Get the data from the MySQL client
	stats, err := n.sqlclient.getGlobalStats()
	if err != nil {
		n.logger.Error("Failed to fetch global stats", zap.Error(err))
		return pmetric.Metrics{}, err
	}

	now := pcommon.NewTimestampFromTime(time.Now())

	// 2. Helper function to parse MySQL strings to int64
	getValue := func(key string) int64 {
		if val, ok := stats[key]; ok {
			i, _ := strconv.ParseInt(val, 10, 64)
			return i
		}
		return 0
	}

	// 3. Record core metrics for testing
	// The method names below are generated based on your metadata.yaml

	// Network Metrics
	n.mb.RecordNewrelicmysqlBytesReceivedDataPoint(now, getValue("Bytes_received"))
	n.mb.RecordNewrelicmysqlBytesSentDataPoint(now, getValue("Bytes_sent"))

	// Connection Metrics
	n.mb.RecordNewrelicmysqlConnectionsDataPoint(now, getValue("Threads_connected"))
	n.mb.RecordNewrelicmysqlConnectionsMaxDataPoint(now, getValue("Max_used_connections"))

	// Query Metrics
	n.mb.RecordNewrelicmysqlQueriesDataPoint(now, getValue("Questions"))
	n.mb.RecordNewrelicmysqlSlowQueriesDataPoint(now, getValue("Slow_queries"))

	// Command Metrics (Switching for standard attributes)
	n.mb.RecordNewrelicmysqlCommandsDataPoint(now, getValue("Com_select"), metadata.AttributeCommandSelect)
	n.mb.RecordNewrelicmysqlCommandsDataPoint(now, getValue("Com_insert"), metadata.AttributeCommandInsert)
	n.mb.RecordNewrelicmysqlCommandsDataPoint(now, getValue("Com_update"), metadata.AttributeCommandUpdate)
	n.mb.RecordNewrelicmysqlCommandsDataPoint(now, getValue("Com_delete"), metadata.AttributeCommandDelete)
	n.mb.RecordNewrelicmysqlCommandsDataPoint(now, getValue("Com_commit"), metadata.AttributeCommandCommit)
	n.mb.RecordNewrelicmysqlCommandsDataPoint(now, getValue("Com_rollback"), metadata.AttributeCommandRollback)

	// Thread Metrics
	n.mb.RecordNewrelicmysqlThreadsRunningDataPoint(now, getValue("Threads_running"))

	// InnoDB Buffer Pool Metrics
	n.mb.RecordNewrelicmysqlInnodbBufferPoolPagesDataDataPoint(now, getValue("Innodb_buffer_pool_pages_data"))
	n.mb.RecordNewrelicmysqlInnodbBufferPoolPagesFreeDataPoint(now, getValue("Innodb_buffer_pool_pages_free"))
	n.mb.RecordNewrelicmysqlInnodbBufferPoolReadRequestsDataPoint(now, getValue("Innodb_buffer_pool_read_requests"))
	n.mb.RecordNewrelicmysqlInnodbBufferPoolReadsDataPoint(now, getValue("Innodb_buffer_pool_reads"))

	// InnoDB Row Lock Metrics
	n.mb.RecordNewrelicmysqlInnodbRowLockTimeDataPoint(now, getValue("Innodb_row_lock_time"))
	n.mb.RecordNewrelicmysqlInnodbRowLockWaitsDataPoint(now, getValue("Innodb_row_lock_waits"))

	n.logger.Info("Successfully scraped all 16 New Relic MySQL metrics")

	// 4. Emit the collected metrics
	return n.mb.Emit(), nil
}
