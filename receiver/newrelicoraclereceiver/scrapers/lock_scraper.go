// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package scrapers

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/client"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/internal/metadata"
)

// LockScraper collects Oracle lock-related metrics
type LockScraper struct {
	logger       *zap.Logger
	client       client.OracleClient
	mb           *metadata.MetricsBuilder
	instanceName string
}

// NewLockScraper creates a new lock scraper
func NewLockScraper(logger *zap.Logger, client client.OracleClient, mb *metadata.MetricsBuilder, instanceName string) *LockScraper {
	return &LockScraper{
		logger:       logger,
		client:       client,
		mb:           mb,
		instanceName: instanceName,
	}
}

// ScrapeLocks collects lock-related metrics
func (s *LockScraper) ScrapeLocks(ctx context.Context) []error {
	var errors []error
	now := pcommon.NewTimestampFromTime(time.Now())

	// Collect lock counts by type and mode (newrelicoracledb.locks.count)
	if err := s.scrapeLockCounts(ctx, now); err != nil {
		s.logger.Error("Failed to scrape lock counts", zap.Error(err))
		errors = append(errors, err)
	}

	// Collect detailed lock counts with instance ID (newrelicoracledb.lock.count)
	if err := s.scrapeDetailedLockCounts(ctx, now); err != nil {
		s.logger.Error("Failed to scrape detailed lock counts", zap.Error(err))
		errors = append(errors, err)
	}

	// Collect session counts holding locks (newrelicoracledb.lock.session_count)
	if err := s.scrapeLockSessionCounts(ctx, now); err != nil {
		s.logger.Error("Failed to scrape lock session counts", zap.Error(err))
		errors = append(errors, err)
	}

	// Collect locked object counts (newrelicoracledb.lock.object_count)
	if err := s.scrapeLockObjectCounts(ctx, now); err != nil {
		s.logger.Error("Failed to scrape lock object counts", zap.Error(err))
		errors = append(errors, err)
	}

	// Collect blocked sessions by lock type (newrelicoracledb.locks.blocked_sessions)
	if err := s.scrapeBlockedSessions(ctx, now); err != nil {
		s.logger.Error("Failed to scrape blocked sessions by locks", zap.Error(err))
		errors = append(errors, err)
	}

	// Collect deadlock count (newrelicoracledb.locks.deadlock_count)
	if err := s.scrapeDeadlockCount(ctx, now); err != nil {
		s.logger.Error("Failed to scrape deadlock count", zap.Error(err))
		errors = append(errors, err)
	}

	return errors
}

// scrapeLockCounts collects lock counts by type and mode
func (s *LockScraper) scrapeLockCounts(ctx context.Context, now pcommon.Timestamp) error {
	lockCounts, err := s.client.QueryLockCounts(ctx)
	if err != nil {
		return err
	}

	for _, lc := range lockCounts {
		if lc.GetCount() >= 0 {
			s.mb.RecordNewrelicoracledbLocksCountDataPoint(
				now,
				lc.GetCount(),
				s.instanceName,
				lc.GetLockType(),
				lc.GetLockMode(),
			)
		}
	}

	return nil
}

// scrapeDetailedLockCounts collects detailed lock counts with instance ID (newrelicoracledb.lock.count)
func (s *LockScraper) scrapeDetailedLockCounts(ctx context.Context, now pcommon.Timestamp) error {
	lockCounts, err := s.client.QueryLockCounts(ctx)
	if err != nil {
		return err
	}

	for _, lc := range lockCounts {
		if lc.GetCount() >= 0 {
			instanceID := getInstanceIDString(lc.InstID)
			s.mb.RecordNewrelicoracledbLockCountDataPoint(
				now,
				lc.GetCount(),
				s.instanceName,
				instanceID,
				lc.GetLockType(),
				lc.GetLockMode(),
			)
		}
	}

	return nil
}

// scrapeLockSessionCounts collects session counts holding locks (newrelicoracledb.lock.session_count)
func (s *LockScraper) scrapeLockSessionCounts(ctx context.Context, now pcommon.Timestamp) error {
	sessionCounts, err := s.client.QueryLockSessionCounts(ctx)
	if err != nil {
		return err
	}

	for _, lsc := range sessionCounts {
		if lsc.GetSessionCount() >= 0 {
			instanceID := getInstanceIDString(lsc.InstID)
			s.mb.RecordNewrelicoracledbLockSessionCountDataPoint(
				now,
				lsc.GetSessionCount(),
				s.instanceName,
				instanceID,
				lsc.GetLockType(),
			)
		}
	}

	return nil
}

// scrapeLockObjectCounts collects locked object counts (newrelicoracledb.lock.object_count)
func (s *LockScraper) scrapeLockObjectCounts(ctx context.Context, now pcommon.Timestamp) error {
	objectCounts, err := s.client.QueryLockedObjectCounts(ctx)
	if err != nil {
		return err
	}

	for _, oc := range objectCounts {
		if oc.GetObjectCount() >= 0 {
			instanceID := getInstanceIDString(oc.InstID)
			s.mb.RecordNewrelicoracledbLockObjectCountDataPoint(
				now,
				oc.GetObjectCount(),
				s.instanceName,
				instanceID,
				oc.GetLockType(),
				oc.GetObjectType(),
			)
		}
	}

	return nil
}

// scrapeBlockedSessions collects blocked sessions by lock type
func (s *LockScraper) scrapeBlockedSessions(ctx context.Context, now pcommon.Timestamp) error {
	objectCounts, err := s.client.QueryLockedObjectCounts(ctx)
	if err != nil {
		return err
	}

	for _, oc := range objectCounts {
		if oc.GetObjectCount() >= 0 {
			s.mb.RecordNewrelicoracledbLocksBlockedSessionsDataPoint(
				now,
				oc.GetObjectCount(),
				s.instanceName,
				oc.GetLockType(),
				oc.GetObjectType(),
			)
		}
	}

	return nil
}

// scrapeDeadlockCount collects cumulative deadlock count
func (s *LockScraper) scrapeDeadlockCount(ctx context.Context, now pcommon.Timestamp) error {
	deadlockCount, err := s.client.QueryDeadlockCount(ctx)
	if err != nil {
		return err
	}

	if deadlockCount != nil {
		s.mb.RecordNewrelicoracledbLocksDeadlockCountDataPoint(
			now,
			deadlockCount.GetCount(),
			s.instanceName,
		)
	}

	return nil
}
