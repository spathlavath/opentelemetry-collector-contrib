// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package scrapers

import (
	"go.opentelemetry.io/collector/scraper/scrapererror"
)

// ConvertReplicationThreadStatus converts replication thread status string to numeric code.
// Returns: 0 = No/Stopped, 1 = Yes/Running, 2 = Connecting (IO thread only)
func ConvertReplicationThreadStatus(status string) int64 {
	switch status {
	case "Yes":
		return 1
	case "Connecting":
		return 2
	case "No", "":
		return 0
	default:
		return 0
	}
}

// AddPartialIfError adds a partial error to the scrape errors collection if err is not nil.
func AddPartialIfError(errors *scrapererror.ScrapeErrors, err error) {
	if err != nil {
		errors.AddPartial(1, err)
	}
}
