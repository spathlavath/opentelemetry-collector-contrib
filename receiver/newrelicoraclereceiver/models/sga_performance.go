// Copyright New Relic, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package models // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/models"

import "database/sql"

// SGASharedPoolLibraryCacheReloadRatioMetric represents library cache reload ratio
type SGASharedPoolLibraryCacheReloadRatioMetric struct {
	Ratio  float64
	InstID any
}

// SGASharedPoolLibraryCacheHitRatioMetric represents library cache hit ratio
type SGASharedPoolLibraryCacheHitRatioMetric struct {
	Ratio  float64
	InstID any
}

// SGASharedPoolDictCacheMissRatioMetric represents dictionary cache miss ratio
type SGASharedPoolDictCacheMissRatioMetric struct {
	Ratio  float64
	InstID any
}

// SGALogBufferSpaceWaitsMetric represents log buffer space waits
type SGALogBufferSpaceWaitsMetric struct {
	Count  int64
	InstID any
}

// SGALogAllocRetriesMetric represents log allocation retry ratio
type SGALogAllocRetriesMetric struct {
	Ratio  sql.NullFloat64
	InstID any
}

// SGAHitRatioMetric represents SGA buffer hit ratio
type SGAHitRatioMetric struct {
	InstID any
	Ratio  sql.NullFloat64
}
