// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/models"
)

// MockClient is a mock implementation of OracleClient for testing.
type MockClient struct {
	ExecutionPlans  map[string][]models.ExecutionPlan
	SlowQueries     []models.SlowQuery
	BlockingQueries []models.BlockingQuery
	WaitEvents      []models.WaitEvent

	// Connection metrics
	TotalSessions    int64
	ActiveSessions   int64
	InactiveSessions int64

	SessionStatusList            []models.SessionStatus
	SessionTypeList              []models.SessionType
	LogonStatsList               []models.LogonStat
	SessionResourcesList         []models.SessionResource
	CurrentWaitEventsList        []models.CurrentWaitEvent
	BlockingSessionsList         []models.BlockingSession
	WaitEventSummaryList         []models.WaitEventSummary
	ConnectionPoolMetricsList    []models.ConnectionPoolMetric
	SessionLimitsList            []models.SessionLimit
	ConnectionQualityMetricsList []models.ConnectionQualityMetric

	// Container metrics
	CheckCDBFeatureResult       int64
	CheckPDBCapabilityResult    int64
	CheckCurrentContainerResult models.ContainerContext
	ContainerStatusList         []models.ContainerStatus
	PDBStatusList               []models.PDBStatus
	CDBTablespaceUsageList      []models.CDBTablespaceUsage
	CDBDataFilesList            []models.CDBDataFile
	CDBServicesList             []models.CDBService

	// Disk I/O metrics
	DiskIOMetricsList []models.DiskIOMetrics

	// Instance metrics
	LockedAccountsList     []models.LockedAccountsMetric
	GlobalNameList         []models.GlobalNameMetric
	DBIDList               []models.DBIDMetric
	LongRunningQueriesList []models.LongRunningQueriesMetric

	// Memory metrics
	PGAMetricsList                    []models.PGAMetric
	SGAUGATotalMemoryList             []models.SGAUGATotalMemoryMetric
	SGASharedPoolLibraryCacheList     []models.SGASharedPoolLibraryCacheMetric
	SGASharedPoolLibraryCacheUserList []models.SGASharedPoolLibraryCacheUserMetric
	SGAMetricsList                    []models.SGAMetric

	// Performance metrics
	SysstatMetricsList          []models.SysstatMetric
	RollbackSegmentsMetricsList []models.RollbackSegmentsMetric
	RedoLogWaitsMetricsList     []models.RedoLogWaitsMetric

	// SGA Performance metrics
	SGASharedPoolLibraryCacheReloadRatioList []models.SGASharedPoolLibraryCacheReloadRatioMetric
	SGASharedPoolLibraryCacheHitRatioList    []models.SGASharedPoolLibraryCacheHitRatioMetric
	SGASharedPoolDictCacheMissRatioList      []models.SGASharedPoolDictCacheMissRatioMetric
	SGALogBufferSpaceWaitsList               []models.SGALogBufferSpaceWaitsMetric
	SGALogAllocRetriesList                   []models.SGALogAllocRetriesMetric
	SGAHitRatioList                          []models.SGAHitRatioMetric

	ConnectErr error
	CloseErr   error
	PingErr    error
	QueryErr   error
}

// NewMockClient creates a new mock client for testing.
func NewMockClient() *MockClient {
	return &MockClient{
		ExecutionPlans:  make(map[string][]models.ExecutionPlan),
		SlowQueries:     []models.SlowQuery{},
		BlockingQueries: []models.BlockingQuery{},
		WaitEvents:      []models.WaitEvent{},
	}
}

func (m *MockClient) Connect() error {
	return m.ConnectErr
}

func (m *MockClient) Close() error {
	return m.CloseErr
}

func (m *MockClient) Ping(ctx context.Context) error {
	return m.PingErr
}

func (m *MockClient) QueryExecutionPlans(ctx context.Context, sqlID string) ([]models.ExecutionPlan, error) {
	if m.QueryErr != nil {
		return nil, m.QueryErr
	}

	if plans, ok := m.ExecutionPlans[sqlID]; ok {
		return plans, nil
	}

	return []models.ExecutionPlan{}, nil
}

func (m *MockClient) QuerySlowQueries(ctx context.Context, responseTimeThreshold, countThreshold int) ([]models.SlowQuery, error) {
	if m.QueryErr != nil {
		return nil, m.QueryErr
	}
	return m.SlowQueries, nil
}

func (m *MockClient) QueryBlockingQueries(ctx context.Context, countThreshold int) ([]models.BlockingQuery, error) {
	if m.QueryErr != nil {
		return nil, m.QueryErr
	}
	return m.BlockingQueries, nil
}

func (m *MockClient) QueryWaitEvents(ctx context.Context, countThreshold int) ([]models.WaitEvent, error) {
	if m.QueryErr != nil {
		return nil, m.QueryErr
	}
	return m.WaitEvents, nil
}

func (m *MockClient) QueryTotalSessions(ctx context.Context) (int64, error) {
	if m.QueryErr != nil {
		return 0, m.QueryErr
	}
	return m.TotalSessions, nil
}

func (m *MockClient) QueryActiveSessions(ctx context.Context) (int64, error) {
	if m.QueryErr != nil {
		return 0, m.QueryErr
	}
	return m.ActiveSessions, nil
}

func (m *MockClient) QueryInactiveSessions(ctx context.Context) (int64, error) {
	if m.QueryErr != nil {
		return 0, m.QueryErr
	}
	return m.InactiveSessions, nil
}

func (m *MockClient) QuerySessionStatus(ctx context.Context) ([]models.SessionStatus, error) {
	if m.QueryErr != nil {
		return nil, m.QueryErr
	}
	return m.SessionStatusList, nil
}

func (m *MockClient) QuerySessionTypes(ctx context.Context) ([]models.SessionType, error) {
	if m.QueryErr != nil {
		return nil, m.QueryErr
	}
	return m.SessionTypeList, nil
}

func (m *MockClient) QueryLogonStats(ctx context.Context) ([]models.LogonStat, error) {
	if m.QueryErr != nil {
		return nil, m.QueryErr
	}
	return m.LogonStatsList, nil
}

func (m *MockClient) QuerySessionResources(ctx context.Context) ([]models.SessionResource, error) {
	if m.QueryErr != nil {
		return nil, m.QueryErr
	}
	return m.SessionResourcesList, nil
}

func (m *MockClient) QueryCurrentWaitEvents(ctx context.Context) ([]models.CurrentWaitEvent, error) {
	if m.QueryErr != nil {
		return nil, m.QueryErr
	}
	return m.CurrentWaitEventsList, nil
}

func (m *MockClient) QueryBlockingSessions(ctx context.Context) ([]models.BlockingSession, error) {
	if m.QueryErr != nil {
		return nil, m.QueryErr
	}
	return m.BlockingSessionsList, nil
}

func (m *MockClient) QueryWaitEventSummary(ctx context.Context) ([]models.WaitEventSummary, error) {
	if m.QueryErr != nil {
		return nil, m.QueryErr
	}
	return m.WaitEventSummaryList, nil
}

func (m *MockClient) QueryConnectionPoolMetrics(ctx context.Context) ([]models.ConnectionPoolMetric, error) {
	if m.QueryErr != nil {
		return nil, m.QueryErr
	}
	return m.ConnectionPoolMetricsList, nil
}

func (m *MockClient) QuerySessionLimits(ctx context.Context) ([]models.SessionLimit, error) {
	if m.QueryErr != nil {
		return nil, m.QueryErr
	}
	return m.SessionLimitsList, nil
}

func (m *MockClient) QueryConnectionQuality(ctx context.Context) ([]models.ConnectionQualityMetric, error) {
	if m.QueryErr != nil {
		return nil, m.QueryErr
	}
	return m.ConnectionQualityMetricsList, nil
}

// CheckCDBFeature mock
func (m *MockClient) CheckCDBFeature(ctx context.Context) (int64, error) {
	if m.QueryErr != nil {
		return 0, m.QueryErr
	}
	return m.CheckCDBFeatureResult, nil
}

// CheckPDBCapability mock
func (m *MockClient) CheckPDBCapability(ctx context.Context) (int64, error) {
	if m.QueryErr != nil {
		return 0, m.QueryErr
	}
	return m.CheckPDBCapabilityResult, nil
}

// CheckCurrentContainer mock
func (m *MockClient) CheckCurrentContainer(ctx context.Context) (models.ContainerContext, error) {
	if m.QueryErr != nil {
		return models.ContainerContext{}, m.QueryErr
	}
	return m.CheckCurrentContainerResult, nil
}

// QueryContainerStatus mock
func (m *MockClient) QueryContainerStatus(ctx context.Context) ([]models.ContainerStatus, error) {
	if m.QueryErr != nil {
		return nil, m.QueryErr
	}
	return m.ContainerStatusList, nil
}

// QueryPDBStatus mock
func (m *MockClient) QueryPDBStatus(ctx context.Context) ([]models.PDBStatus, error) {
	if m.QueryErr != nil {
		return nil, m.QueryErr
	}
	return m.PDBStatusList, nil
}

// QueryCDBTablespaceUsage mock
func (m *MockClient) QueryCDBTablespaceUsage(ctx context.Context) ([]models.CDBTablespaceUsage, error) {
	if m.QueryErr != nil {
		return nil, m.QueryErr
	}
	return m.CDBTablespaceUsageList, nil
}

// QueryCDBDataFiles mock
func (m *MockClient) QueryCDBDataFiles(ctx context.Context) ([]models.CDBDataFile, error) {
	if m.QueryErr != nil {
		return nil, m.QueryErr
	}
	return m.CDBDataFilesList, nil
}

// QueryCDBServices mock
func (m *MockClient) QueryCDBServices(ctx context.Context) ([]models.CDBService, error) {
	if m.QueryErr != nil {
		return nil, m.QueryErr
	}
	return m.CDBServicesList, nil
}

// QueryDiskIOMetrics mock
func (m *MockClient) QueryDiskIOMetrics(ctx context.Context) ([]models.DiskIOMetrics, error) {
	if m.QueryErr != nil {
		return nil, m.QueryErr
	}
	return m.DiskIOMetricsList, nil
}

// QueryLockedAccounts mock
func (m *MockClient) QueryLockedAccounts(ctx context.Context) ([]models.LockedAccountsMetric, error) {
	if m.QueryErr != nil {
		return nil, m.QueryErr
	}
	return m.LockedAccountsList, nil
}

// QueryGlobalName mock
func (m *MockClient) QueryGlobalName(ctx context.Context) ([]models.GlobalNameMetric, error) {
	if m.QueryErr != nil {
		return nil, m.QueryErr
	}
	return m.GlobalNameList, nil
}

// QueryDBID mock
func (m *MockClient) QueryDBID(ctx context.Context) ([]models.DBIDMetric, error) {
	if m.QueryErr != nil {
		return nil, m.QueryErr
	}
	return m.DBIDList, nil
}

// QueryLongRunningQueries mock
func (m *MockClient) QueryLongRunningQueries(_ context.Context) ([]models.LongRunningQueriesMetric, error) {
	if m.QueryErr != nil {
		return nil, m.QueryErr
	}
	return m.LongRunningQueriesList, nil
}

func (m *MockClient) QueryPGAMetrics(_ context.Context) ([]models.PGAMetric, error) {
	if m.QueryErr != nil {
		return nil, m.QueryErr
	}
	return m.PGAMetricsList, nil
}

func (m *MockClient) QuerySGAUGATotalMemory(_ context.Context) ([]models.SGAUGATotalMemoryMetric, error) {
	if m.QueryErr != nil {
		return nil, m.QueryErr
	}
	return m.SGAUGATotalMemoryList, nil
}

func (m *MockClient) QuerySGASharedPoolLibraryCache(_ context.Context) ([]models.SGASharedPoolLibraryCacheMetric, error) {
	if m.QueryErr != nil {
		return nil, m.QueryErr
	}
	return m.SGASharedPoolLibraryCacheList, nil
}

func (m *MockClient) QuerySGASharedPoolLibraryCacheUser(_ context.Context) ([]models.SGASharedPoolLibraryCacheUserMetric, error) {
	if m.QueryErr != nil {
		return nil, m.QueryErr
	}
	return m.SGASharedPoolLibraryCacheUserList, nil
}

func (m *MockClient) QuerySGAMetrics(_ context.Context) ([]models.SGAMetric, error) {
	if m.QueryErr != nil {
		return nil, m.QueryErr
	}
	return m.SGAMetricsList, nil
}

func (m *MockClient) QuerySysstatMetrics(_ context.Context) ([]models.SysstatMetric, error) {
	if m.QueryErr != nil {
		return nil, m.QueryErr
	}
	return m.SysstatMetricsList, nil
}

func (m *MockClient) QueryRollbackSegmentsMetrics(_ context.Context) ([]models.RollbackSegmentsMetric, error) {
	if m.QueryErr != nil {
		return nil, m.QueryErr
	}
	return m.RollbackSegmentsMetricsList, nil
}

func (m *MockClient) QueryRedoLogWaitsMetrics(_ context.Context) ([]models.RedoLogWaitsMetric, error) {
	if m.QueryErr != nil {
		return nil, m.QueryErr
	}
	return m.RedoLogWaitsMetricsList, nil
}

func (m *MockClient) QuerySGASharedPoolLibraryCacheReloadRatio(_ context.Context) ([]models.SGASharedPoolLibraryCacheReloadRatioMetric, error) {
	if m.QueryErr != nil {
		return nil, m.QueryErr
	}
	return m.SGASharedPoolLibraryCacheReloadRatioList, nil
}

func (m *MockClient) QuerySGASharedPoolLibraryCacheHitRatio(_ context.Context) ([]models.SGASharedPoolLibraryCacheHitRatioMetric, error) {
	if m.QueryErr != nil {
		return nil, m.QueryErr
	}
	return m.SGASharedPoolLibraryCacheHitRatioList, nil
}

func (m *MockClient) QuerySGASharedPoolDictCacheMissRatio(_ context.Context) ([]models.SGASharedPoolDictCacheMissRatioMetric, error) {
	if m.QueryErr != nil {
		return nil, m.QueryErr
	}
	return m.SGASharedPoolDictCacheMissRatioList, nil
}

func (m *MockClient) QuerySGALogBufferSpaceWaits(_ context.Context) ([]models.SGALogBufferSpaceWaitsMetric, error) {
	if m.QueryErr != nil {
		return nil, m.QueryErr
	}
	return m.SGALogBufferSpaceWaitsList, nil
}

func (m *MockClient) QuerySGALogAllocRetries(_ context.Context) ([]models.SGALogAllocRetriesMetric, error) {
	if m.QueryErr != nil {
		return nil, m.QueryErr
	}
	return m.SGALogAllocRetriesList, nil
}

func (m *MockClient) QuerySGAHitRatio(_ context.Context) ([]models.SGAHitRatioMetric, error) {
	if m.QueryErr != nil {
		return nil, m.QueryErr
	}
	return m.SGAHitRatioList, nil
}
