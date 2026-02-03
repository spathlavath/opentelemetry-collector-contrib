// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package common

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/scraper/scrapererror"
)

func TestConvertReplicationThreadStatus(t *testing.T) {
	tests := []struct {
		name     string
		status   string
		expected int64
	}{
		{
			name:     "yes",
			status:   "Yes",
			expected: 1,
		},
		{
			name:     "connecting",
			status:   "Connecting",
			expected: 2,
		},
		{
			name:     "no",
			status:   "No",
			expected: 0,
		},
		{
			name:     "empty string",
			status:   "",
			expected: 0,
		},
		{
			name:     "unknown status defaults to 0",
			status:   "InvalidStatus",
			expected: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ConvertReplicationThreadStatus(tt.status)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestAddPartialIfError(t *testing.T) {
	t.Run("nil error does not panic", func(t *testing.T) {
		errs := &scrapererror.ScrapeErrors{}
		assert.NotPanics(t, func() {
			AddPartialIfError(errs, nil)
		})
	})

	t.Run("non-nil error adds to scrape errors", func(t *testing.T) {
		errs := &scrapererror.ScrapeErrors{}
		testErr := errors.New("test error")
		assert.NotPanics(t, func() {
			AddPartialIfError(errs, testErr)
		})
	})

	t.Run("multiple errors accumulate", func(t *testing.T) {
		errs := &scrapererror.ScrapeErrors{}
		assert.NotPanics(t, func() {
			AddPartialIfError(errs, errors.New("error 1"))
			AddPartialIfError(errs, errors.New("error 2"))
			AddPartialIfError(errs, nil) // This should not increment
			AddPartialIfError(errs, errors.New("error 3"))
		})
	})
}
