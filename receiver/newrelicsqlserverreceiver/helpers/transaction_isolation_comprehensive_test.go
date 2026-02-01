package helpers

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetIsolationLevelDescriptionComprehensive(t *testing.T) {
	tests := []struct {
		name                string
		isolationLevel      int64
		expectedDescription string
	}{
		{
			name:                "READ UNCOMMITTED",
			isolationLevel:      1,
			expectedDescription: "READ UNCOMMITTED",
		},
		{
			name:                "READ COMMITTED",
			isolationLevel:      2,
			expectedDescription: "READ COMMITTED",
		},
		{
			name:                "REPEATABLE READ",
			isolationLevel:      3,
			expectedDescription: "REPEATABLE READ",
		},
		{
			name:                "SERIALIZABLE",
			isolationLevel:      4,
			expectedDescription: "SERIALIZABLE",
		},
		{
			name:                "SNAPSHOT",
			isolationLevel:      5,
			expectedDescription: "SNAPSHOT",
		},
		{
			name:                "Unknown level",
			isolationLevel:      99,
			expectedDescription: "UNKNOWN",
		},
		{
			name:                "Negative level",
			isolationLevel:      -1,
			expectedDescription: "UNKNOWN",
		},
		{
			name:                "Zero level",
			isolationLevel:      0,
			expectedDescription: "UNKNOWN",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			description := GetIsolationLevelDescription(tt.isolationLevel)
			assert.Equal(t, tt.expectedDescription, description)
		})
	}
}

func TestGetIsolationLevelLockingBehaviorComprehensive(t *testing.T) {
	tests := []struct {
		name             string
		isolationLevel   int64
		shouldContain    []string
		shouldNotContain []string
	}{
		{
			name:           "READ UNCOMMITTED - Dirty reads allowed",
			isolationLevel: 1,
			shouldContain:  []string{"dirty", "read"},
		},
		{
			name:           "READ COMMITTED - Prevents dirty reads",
			isolationLevel: 2,
			shouldContain:  []string{"prevent", "dirty"},
		},
		{
			name:           "REPEATABLE READ - Prevents non-repeatable reads",
			isolationLevel: 3,
			shouldContain:  []string{"repeat", "prevent"},
		},
		{
			name:           "SERIALIZABLE - Strictest isolation",
			isolationLevel: 4,
			shouldContain:  []string{"phantom", "prevent"},
		},
		{
			name:           "SNAPSHOT - Version-based",
			isolationLevel: 5,
			shouldContain:  []string{"version", "snapshot"},
		},
		{
			name:           "UNKNOWN - Returns unknown message",
			isolationLevel: 99,
			shouldContain:  []string{"Unknown"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			behavior := GetIsolationLevelLockingBehavior(tt.isolationLevel)
			assert.NotEmpty(t, behavior, "Locking behavior should not be empty")

			for _, expected := range tt.shouldContain {
				assert.Contains(t, behavior, expected,
					"Locking behavior should contain '%s' for isolation level %d", expected, tt.isolationLevel)
			}
		})
	}
}

func TestGetIsolationLevelRiskComprehensive(t *testing.T) {
	tests := []struct {
		name           string
		isolationLevel int64
		expectedRisk   string
		riskKeywords   []string
	}{
		{
			name:           "READ UNCOMMITTED - High risk",
			isolationLevel: 1,
			expectedRisk:   "HIGH",
			riskKeywords:   []string{"dirty", "uncommitted"},
		},
		{
			name:           "READ COMMITTED - Low risk",
			isolationLevel: 2,
			expectedRisk:   "LOW",
			riskKeywords:   []string{"default", "balance"},
		},
		{
			name:           "REPEATABLE READ - Medium risk",
			isolationLevel: 3,
			expectedRisk:   "MEDIUM",
			riskKeywords:   []string{"lock", "deadlock"},
		},
		{
			name:           "SERIALIZABLE - Very High risk",
			isolationLevel: 4,
			expectedRisk:   "VERY HIGH",
			riskKeywords:   []string{"contention", "performance"},
		},
		{
			name:           "SNAPSHOT - Low risk",
			isolationLevel: 5,
			expectedRisk:   "LOW",
			riskKeywords:   []string{"version", "tempdb"},
		},
		{
			name:           "UNKNOWN - Unknown risk",
			isolationLevel: 99,
			expectedRisk:   "UNKNOWN",
			riskKeywords:   []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			risk := GetIsolationLevelRisk(tt.isolationLevel)
			assert.NotEmpty(t, risk, "Risk assessment should not be empty")
			assert.Contains(t, risk, tt.expectedRisk,
				"Risk should contain '%s' for isolation level %d", tt.expectedRisk, tt.isolationLevel)

			for _, keyword := range tt.riskKeywords {
				assert.Contains(t, risk, keyword,
					"Risk should mention '%s' for isolation level %d", keyword, tt.isolationLevel)
			}
		})
	}
}

func TestIsBlockingIsolationLevelComprehensive(t *testing.T) {
	tests := []struct {
		name           string
		isolationLevel int64
		expectedBlock  bool
	}{
		{
			name:           "READ UNCOMMITTED - No blocking",
			isolationLevel: 1,
			expectedBlock:  false,
		},
		{
			name:           "READ COMMITTED - Some blocking",
			isolationLevel: 2,
			expectedBlock:  true,
		},
		{
			name:           "REPEATABLE READ - Blocking",
			isolationLevel: 3,
			expectedBlock:  true,
		},
		{
			name:           "SERIALIZABLE - High blocking",
			isolationLevel: 4,
			expectedBlock:  true,
		},
		{
			name:           "SNAPSHOT - No blocking (optimistic)",
			isolationLevel: 5,
			expectedBlock:  false,
		},
		{
			name:           "UNKNOWN - Assume blocking",
			isolationLevel: 99,
			expectedBlock:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isBlocking := IsBlockingIsolationLevel(tt.isolationLevel)
			assert.Equal(t, tt.expectedBlock, isBlocking,
				"Blocking behavior incorrect for isolation level %d", tt.isolationLevel)
		})
	}
}

func TestIsolationLevelEdgeCases(t *testing.T) {
	t.Run("Large negative number", func(t *testing.T) {
		level := int64(-999999)
		desc := GetIsolationLevelDescription(level)
		assert.Equal(t, "UNKNOWN", desc)

		behavior := GetIsolationLevelLockingBehavior(level)
		assert.Contains(t, behavior, "Unknown")

		risk := GetIsolationLevelRisk(level)
		assert.Contains(t, risk, "UNKNOWN")

		isBlocking := IsBlockingIsolationLevel(level)
		assert.False(t, isBlocking)
	})

	t.Run("Large positive number", func(t *testing.T) {
		level := int64(999999)
		desc := GetIsolationLevelDescription(level)
		assert.Equal(t, "UNKNOWN", desc)

		behavior := GetIsolationLevelLockingBehavior(level)
		assert.Contains(t, behavior, "Unknown")

		risk := GetIsolationLevelRisk(level)
		assert.Contains(t, risk, "UNKNOWN")

		isBlocking := IsBlockingIsolationLevel(level)
		assert.False(t, isBlocking)
	})

	t.Run("All valid levels should have consistent data", func(t *testing.T) {
		validLevels := []int64{1, 2, 3, 4, 5}

		for _, level := range validLevels {
			// All functions should return non-empty results for valid levels
			desc := GetIsolationLevelDescription(level)
			assert.NotEqual(t, "UNKNOWN", desc, "Valid level %d should not return UNKNOWN", level)

			behavior := GetIsolationLevelLockingBehavior(level)
			assert.NotContains(t, behavior, "Unknown", "Valid level %d should not have Unknown behavior", level)

			risk := GetIsolationLevelRisk(level)
			assert.NotContains(t, risk, "UNKNOWN", "Valid level %d should not have UNKNOWN risk", level)
		}
	})
}

func TestDecodeTransactionIsolationLevelConsistency(t *testing.T) {
	// Test that DecodeTransactionIsolationLevel is consistent with GetIsolationLevelDescription
	tests := []int64{1, 2, 3, 4, 5, 0, -1, 99}

	for _, level := range tests {
		decoded := DecodeTransactionIsolationLevel(level)
		described := GetIsolationLevelDescription(level)

		// Both should return the same or equivalent values
		assert.Equal(t, described, decoded,
			"DecodeTransactionIsolationLevel and GetIsolationLevelDescription should match for level %d", level)
	}
}
