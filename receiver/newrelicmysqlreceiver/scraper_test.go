// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package newrelicmysqlreceiver

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/receiver/receivertest"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicmysqlreceiver/internal/metadata"
)

func TestNewMySQLScraper(t *testing.T) {
	cfg := &Config{
		Endpoint: "root:password@tcp(localhost:3306)/mysql",
	}

	scraper, err := newMySQLScraper(receivertest.NewNopSettings(metadata.Type), cfg)
	assert.NoError(t, err)
	assert.NotNil(t, scraper)
}

func TestNewMySQLScraperWithEmptyEndpoint(t *testing.T) {
	cfg := &Config{
		Endpoint: "",
	}

	// The scraper itself should be created, but validation will happen later
	scraper, err := newMySQLScraper(receivertest.NewNopSettings(metadata.Type), cfg)
	assert.NoError(t, err)    // Scraper creation should succeed
	assert.NotNil(t, scraper) // But we should still get a scraper
}
