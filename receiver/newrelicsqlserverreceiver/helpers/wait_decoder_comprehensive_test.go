package helpers

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDecodeWaitResourceComprehensive(t *testing.T) {
	tests := []struct {
		name         string
		waitResource string
		wantType     string
		wantDesc     string
	}{
		// KEY locks - already tested but adding more edge cases
		{
			name:         "KEY lock with large HOBT ID",
			waitResource: "KEY: 5:72057594049986560 (abc123def456)",
			wantType:     "Key Lock",
			wantDesc:     "Database ID: 5 | HOBT: 72057594049986560 | Key Hash: abc123def456",
		},
		{
			name:         "KEY lock with short hash",
			waitResource: "KEY: 1:123 (ab)",
			wantType:     "Key Lock",
			wantDesc:     "Database ID: 1 | HOBT: 123 | Key Hash: ab",
		},

		// PAGE locks - more variations
		{
			name:         "PAGE lock with high page number",
			waitResource: "PAGE: 10:5:999999",
			wantType:     "Page Lock",
			wantDesc:     "Database ID: 10 | File: 5 | Page: 999999",
		},

		// RID locks - more variations
		{
			name:         "RID lock with high row number",
			waitResource: "RID: 5:1:104:9999",
			wantType:     "Row Lock",
			wantDesc:     "Database ID: 5 | File: 1 | Page: 104 | Row: 9999",
		},

		// OBJECT locks - more variations
		{
			name:         "OBJECT lock with partition",
			waitResource: "OBJECT: 5:245575913:3",
			wantType:     "Object Lock",
			wantDesc:     "Database ID: 5 | Object ID: 245575913 | Partition: 3",
		},

		// DATABASE locks
		{
			name:         "DATABASE lock with high DB ID",
			waitResource: "DATABASE: 999",
			wantType:     "Database Lock",
			wantDesc:     "Database ID: 999",
		},

		// FILE locks
		{
			name:         "FILE lock with multiple files",
			waitResource: "FILE: 5:10",
			wantType:     "File Lock",
			wantDesc:     "Database ID: 5 | File: 10",
		},

		// EXTENT locks
		{
			name:         "EXTENT lock with high page",
			waitResource: "EXTENT: 5:1:8888",
			wantType:     "Extent Lock",
			wantDesc:     "Database ID: 5 | File: 1 | First Page: 8888",
		},

		// HOBT locks
		{
			name:         "HOBT lock with large ID",
			waitResource: "HOBT: 999999999999999",
			wantType:     "Heap/B-Tree Lock",
			wantDesc:     "HOBT ID: 999999999999999",
		},

		// APPLICATION locks
		{
			name:         "APPLICATION lock with complex name",
			waitResource: "APPLICATION: 2:My_App_Lock:(hash123abc)",
			wantType:     "Application Lock",
			wantDesc:     "Principal: 2 | Lock Name: My_App_Lock | Hash: hash123abc",
		},

		// Edge cases
		{
			name:         "Malformed KEY lock - missing hash",
			waitResource: "KEY: 5:123",
			wantType:     "Unknown",
			wantDesc:     "KEY: 5:123",
		},
		{
			name:         "Unknown lock type",
			waitResource: "NEWLOCKTYPE: 1:2:3",
			wantType:     "Unknown",
			wantDesc:     "NEWLOCKTYPE: 1:2:3",
		},
		{
			name:         "Lock with extra spaces",
			waitResource: "KEY:  5 : 123  ( abc )",
			wantType:     "Unknown",
			wantDesc:     "KEY:  5 : 123  ( abc )",
		},
		{
			name:         "Very short wait resource",
			waitResource: "X",
			wantType:     "Unknown",
			wantDesc:     "X",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotType, gotDesc := DecodeWaitResource(tt.waitResource)
			assert.Equal(t, tt.wantType, gotType, "Wait resource type mismatch")
			assert.Equal(t, tt.wantDesc, gotDesc, "Wait resource description mismatch")
		})
	}
}

func TestParseHOBTIDFromWaitResource(t *testing.T) {
	tests := []struct {
		name         string
		waitResource string
		expectedID   int64
	}{
		{
			name:         "Valid KEY wait resource",
			waitResource: "KEY: 5:72057594049986560 (abc123)",
			expectedID:   72057594049986560,
		},
		{
			name:         "Small HOBT ID",
			waitResource: "KEY: 1:123 (hash)",
			expectedID:   123,
		},
		{
			name:         "Very large HOBT ID",
			waitResource: "KEY: 10:9223372036854775807 (xyz)",
			expectedID:   9223372036854775807,
		},
		{
			name:         "Not a KEY wait resource",
			waitResource: "PAGE: 5:1:104",
			expectedID:   0,
		},
		{
			name:         "Malformed KEY - missing HOBT ID",
			waitResource: "KEY: 5",
			expectedID:   0,
		},
		{
			name:         "Empty wait resource",
			waitResource: "",
			expectedID:   0,
		},
		{
			name:         "KEY with non-numeric HOBT ID",
			waitResource: "KEY: 5:abc (hash)",
			expectedID:   0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hobtID := ParseHOBTIDFromWaitResource(tt.waitResource)
			assert.Equal(t, tt.expectedID, hobtID, "HOBT ID mismatch")
		})
	}
}

func TestGetMetadataClassName(t *testing.T) {
	tests := []struct {
		name          string
		waitResource  string
		expectedClass string
	}{
		{
			name:          "METADATA wait for DATABASE",
			waitResource:  "METADATA: database_id = 5",
			expectedClass: "DATABASE",
		},
		{
			name:          "METADATA wait for TABLE",
			waitResource:  "METADATA: table_id = 123",
			expectedClass: "TABLE",
		},
		{
			name:          "METADATA wait for INDEX",
			waitResource:  "METADATA: index_id = 456",
			expectedClass: "INDEX",
		},
		{
			name:          "Unknown METADATA class",
			waitResource:  "METADATA: unknown_id = 789",
			expectedClass: "UNKNOWN",
		},
		{
			name:          "Not a METADATA wait",
			waitResource:  "KEY: 5:123 (abc)",
			expectedClass: "",
		},
		{
			name:          "Empty wait resource",
			waitResource:  "",
			expectedClass: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			className := getMetadataClassName(tt.waitResource)
			assert.Equal(t, tt.expectedClass, className,
				"Metadata class name mismatch for: %s", tt.waitResource)
		})
	}
}

func TestDecodeWaitResourceAllTypes(t *testing.T) {
	// Comprehensive test of all lock types
	lockTypes := map[string]struct {
		resource     string
		expectedType string
	}{
		"KEY":         {"KEY: 5:123 (abc)", "Key Lock"},
		"PAGE":        {"PAGE: 5:1:104", "Page Lock"},
		"RID":         {"RID: 5:1:104:0", "Row Lock"},
		"OBJECT":      {"OBJECT: 5:245575913:0", "Object Lock"},
		"DATABASE":    {"DATABASE: 5", "Database Lock"},
		"FILE":        {"FILE: 5:1", "File Lock"},
		"EXTENT":      {"EXTENT: 5:1:104", "Extent Lock"},
		"HOBT":        {"HOBT: 72057594049986560", "Heap/B-Tree Lock"},
		"APPLICATION": {"APPLICATION: 1:MyLock:(abc)", "Application Lock"},
	}

	for lockType, testCase := range lockTypes {
		t.Run(lockType, func(t *testing.T) {
			resType, resDesc := DecodeWaitResource(testCase.resource)
			assert.Equal(t, testCase.expectedType, resType,
				"Lock type mismatch for %s", lockType)
			assert.NotEmpty(t, resDesc, "Description should not be empty for %s", lockType)
		})
	}
}

func TestDecodeWaitResourceConsistency(t *testing.T) {
	t.Run("Same resource returns same result", func(t *testing.T) {
		resource := "KEY: 5:123 (abc123)"

		type1, desc1 := DecodeWaitResource(resource)
		type2, desc2 := DecodeWaitResource(resource)

		assert.Equal(t, type1, type2, "Type should be consistent")
		assert.Equal(t, desc1, desc2, "Description should be consistent")
	})

	t.Run("Unknown resources return Unknown", func(t *testing.T) {
		unknownResources := []string{
			"INVALID: 1:2:3",
			"RANDOM_TEXT",
			"123:456:789",
			"::",
		}

		for _, resource := range unknownResources {
			resType, resDesc := DecodeWaitResource(resource)
			assert.Equal(t, "Unknown", resType, "Unknown resource should return Unknown type")
			assert.Equal(t, resource, resDesc, "Unknown resource description should be the resource itself")
		}
	})
}

func TestWaitResourceEdgeCases(t *testing.T) {
	t.Run("Wait resource with special characters", func(t *testing.T) {
		resource := "APPLICATION: 1:My-App_Lock.v2:(hash#123)"
		resType, resDesc := DecodeWaitResource(resource)
		assert.Equal(t, "Application Lock", resType)
		assert.Contains(t, resDesc, "My-App_Lock.v2")
	})

	t.Run("Wait resource with very long values", func(t *testing.T) {
		resource := "HOBT: 999999999999999999"
		resType, resDesc := DecodeWaitResource(resource)
		assert.Equal(t, "Heap/B-Tree Lock", resType)
		assert.Contains(t, resDesc, "999999999999999999")
	})

	t.Run("Wait resource with zero values", func(t *testing.T) {
		resource := "PAGE: 0:0:0"
		resType, resDesc := DecodeWaitResource(resource)
		assert.Equal(t, "Page Lock", resType)
		assert.Contains(t, resDesc, "Database ID: 0")
		assert.Contains(t, resDesc, "File: 0")
		assert.Contains(t, resDesc, "Page: 0")
	})
}
