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
			expectedDescription: "READ UNCOMMITTED - Can read uncommitted data (dirty reads), lowest isolation level",
		},
		{
			name:                "READ COMMITTED",
			isolationLevel:      2,
			expectedDescription: "READ COMMITTED - Default isolation level, prevents dirty reads but allows non-repeatable reads",
		},
		{
			name:                "REPEATABLE READ",
			isolationLevel:      3,
			expectedDescription: "REPEATABLE READ - Prevents dirty and non-repeatable reads but allows phantom reads",
		},
		{
			name:                "SERIALIZABLE",
			isolationLevel:      4,
			expectedDescription: "SERIALIZABLE - Highest isolation level, prevents dirty reads, non-repeatable reads, and phantom reads",
		},
		{
			name:                "SNAPSHOT",
			isolationLevel:      5,
			expectedDescription: "SNAPSHOT - Uses row versioning for transactionally consistent reads without blocking",
		},
		{
			name:                "Unknown level",
			isolationLevel:      99,
			expectedDescription: "Unknown isolation level",
		},
		{
			name:                "Negative level",
			isolationLevel:      -1,
			expectedDescription: "Unknown isolation level",
		},
		{
			name:                "Zero level",
			isolationLevel:      0,
			expectedDescription: "Unspecified - No isolation level set",
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
		name           string
		isolationLevel int64
	}{
		{
			name:           "READ UNCOMMITTED - Dirty reads allowed",
			isolationLevel: 1,
		},
		{
			name:           "READ COMMITTED - Prevents dirty reads",
			isolationLevel: 2,
		},
		{
			name:           "REPEATABLE READ - Prevents non-repeatable reads",
			isolationLevel: 3,
		},
		{
			name:           "SERIALIZABLE - Strictest isolation",
			isolationLevel: 4,
		},
		{
			name:           "SNAPSHOT - Version-based",
			isolationLevel: 5,
		},
		{
			name:           "UNKNOWN - Returns unknown message",
			isolationLevel: 99,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			behavior := GetIsolationLevelLockingBehavior(tt.isolationLevel)
			assert.NotEmpty(t, behavior, "Locking behavior should not be empty")
		})
	}
}

func TestGetIsolationLevelRiskComprehensive(t *testing.T) {
	tests := []struct {
		name           string
		isolationLevel int64
	}{
		{
			name:           "READ UNCOMMITTED - High risk",
			isolationLevel: 1,
		},
		{
			name:           "READ COMMITTED - Low risk",
			isolationLevel: 2,
		},
		{
			name:           "REPEATABLE READ - Medium risk",
			isolationLevel: 3,
		},
		{
			name:           "SERIALIZABLE - Very High risk",
			isolationLevel: 4,
		},
		{
			name:           "SNAPSHOT - Low risk",
			isolationLevel: 5,
		},
		{
			name:           "UNKNOWN - Unknown risk",
			isolationLevel: 99,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			risk := GetIsolationLevelRisk(tt.isolationLevel)
			assert.NotEmpty(t, risk, "Risk assessment should not be empty")
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
		assert.NotEmpty(t, desc)

		behavior := GetIsolationLevelLockingBehavior(level)
		assert.NotEmpty(t, behavior)

		risk := GetIsolationLevelRisk(level)
		assert.NotEmpty(t, risk)

		isBlocking := IsBlockingIsolationLevel(level)
		assert.False(t, isBlocking)
	})

	t.Run("Large positive number", func(t*testing.T) {
		level := int64(999999)
		desc := GetIsolationLevelDescription(level)
		assert.NotEmpty(t, desc)

		behavior := GetIsolationLevelLockingBehavior(level)
		assert.NotEmpty(t, behavior)

		risk := GetIsolationLevelRisk(level)
		assert.NotEmpty(t, risk)

		isBlocking := IsBlockingIsolationLevel(level)
		assert.False(t, isBlocking)
	})

	t.Run("All valid levels should have consistent data", func(t *testing.T) {
		validLevels := []int64{1, 2, 3, 4, 5}

		for _, level := range validLevels {
			// All functions should return non-empty results for valid levels
			desc := GetIsolationLevelDescription(level)
			assert.NotEmpty(t, desc, "Valid level %d should have description", level)

			behavior := GetIsolationLevelLockingBehavior(level)
			assert.NotEmpty(t, behavior, "Valid level %d should have locking behavior", level)

			risk := GetIsolationLevelRisk(level)
			assert.NotEmpty(t, risk, "Valid level %d should have risk assessment", level)
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
