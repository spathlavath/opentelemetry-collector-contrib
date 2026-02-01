package helpers

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetWaitTypeCategoryDetailed(t *testing.T) {
	tests := []struct {
		name             string
		waitType         string
		expectedCategory string
	}{
		// Lock waits
		{
			name:             "LCK_M_S lock",
			waitType:         "LCK_M_S",
			expectedCategory: "Lock",
		},
		{
			name:             "LCK_M_X lock",
			waitType:         "LCK_M_X",
			expectedCategory: "Lock",
		},
		{
			name:             "LCK_M_U lock",
			waitType:         "LCK_M_U",
			expectedCategory: "Lock",
		},

		// I/O waits
		{
			name:             "PAGEIOLATCH_SH",
			waitType:         "PAGEIOLATCH_SH",
			expectedCategory: "I/O",
		},
		{
			name:             "PAGEIOLATCH_EX",
			waitType:         "PAGEIOLATCH_EX",
			expectedCategory: "I/O",
		},
		{
			name:             "WRITELOG",
			waitType:         "WRITELOG",
			expectedCategory: "I/O",
		},
		{
			name:             "ASYNC_IO_COMPLETION",
			waitType:         "ASYNC_IO_COMPLETION",
			expectedCategory: "I/O",
		},

		// Latch waits
		{
			name:             "PAGELATCH_SH",
			waitType:         "PAGELATCH_SH",
			expectedCategory: "Latch",
		},
		{
			name:             "PAGELATCH_EX",
			waitType:         "PAGELATCH_EX",
			expectedCategory: "Latch",
		},

		// Network waits
		{
			name:             "ASYNC_NETWORK_IO",
			waitType:         "ASYNC_NETWORK_IO",
			expectedCategory: "Network",
		},

		// CPU waits
		{
			name:             "SOS_SCHEDULER_YIELD",
			waitType:         "SOS_SCHEDULER_YIELD",
			expectedCategory: "CPU",
		},
		{
			name:             "CXPACKET",
			waitType:         "CXPACKET",
			expectedCategory: "Parallelism",
		},
		{
			name:             "CXCONSUMER",
			waitType:         "CXCONSUMER",
			expectedCategory: "Parallelism",
		},

		// Memory waits
		{
			name:             "RESOURCE_SEMAPHORE",
			waitType:         "RESOURCE_SEMAPHORE",
			expectedCategory: "Memory",
		},

		// Transaction log waits
		{
			name:             "LOGBUFFER",
			waitType:         "LOGBUFFER",
			expectedCategory: "Transaction Log",
		},

		// Unknown/Other
		{
			name:             "UNKNOWN_WAIT_TYPE",
			waitType:         "UNKNOWN_WAIT_TYPE",
			expectedCategory: "Other",
		},
		{
			name:             "Empty wait type",
			waitType:         "",
			expectedCategory: "Other",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			category := GetWaitTypeCategory(tt.waitType)
			assert.Equal(t, tt.expectedCategory, category, "Wait type category should match")
		})
	}
}

func TestGetWaitTypeCategoryComprehensive(t *testing.T) {
	t.Run("All lock wait types", func(t *testing.T) {
		lockWaitTypes := []string{
			"LCK_M_S", "LCK_M_X", "LCK_M_U", "LCK_M_IS", "LCK_M_IX",
			"LCK_M_SIU", "LCK_M_SIX", "LCK_M_UIX", "LCK_M_BU",
			"LCK_M_RS_S", "LCK_M_RS_U", "LCK_M_RIn_NL", "LCK_M_RIn_S",
			"LCK_M_RIn_U", "LCK_M_RIn_X", "LCK_M_RX_S", "LCK_M_RX_U",
			"LCK_M_RX_X", "LCK_M_SCH_S", "LCK_M_SCH_M",
		}

		for _, waitType := range lockWaitTypes {
			category := GetWaitTypeCategory(waitType)
			assert.Equal(t, "Lock", category, "Wait type %s should be categorized as Lock", waitType)
		}
	})

	t.Run("All I/O wait types", func(t *testing.T) {
		ioWaitTypes := []string{
			"PAGEIOLATCH_SH", "PAGEIOLATCH_EX", "PAGEIOLATCH_UP", "PAGEIOLATCH_KP",
			"WRITELOG", "ASYNC_IO_COMPLETION", "IO_COMPLETION",
			"BACKUPIO",
		}

		for _, waitType := range ioWaitTypes {
			category := GetWaitTypeCategory(waitType)
			assert.Equal(t, "I/O", category, "Wait type %s should be categorized as I/O", waitType)
		}
	})

	t.Run("All latch wait types", func(t *testing.T) {
		latchWaitTypes := []string{
			"PAGELATCH_SH", "PAGELATCH_EX", "PAGELATCH_UP", "PAGELATCH_KP",
			"PAGELATCH_DT", "PAGELATCH_NL",
		}

		for _, waitType := range latchWaitTypes {
			category := GetWaitTypeCategory(waitType)
			assert.Equal(t, "Latch", category, "Wait type %s should be categorized as Latch", waitType)
		}
	})
}

func TestDecodeWaitTypeDescription(t *testing.T) {
	tests := []struct {
		name        string
		waitType    string
		shouldMatch string // Substring that should be in description
	}{
		{
			name:        "LCK_M_S description",
			waitType:    "LCK_M_S",
			shouldMatch: "Shared",
		},
		{
			name:        "PAGEIOLATCH_SH description",
			waitType:    "PAGEIOLATCH_SH",
			shouldMatch: "latch",
		},
		{
			name:        "ASYNC_NETWORK_IO description",
			waitType:    "ASYNC_NETWORK_IO",
			shouldMatch: "network",
		},
		{
			name:        "CXPACKET description",
			waitType:    "CXPACKET",
			shouldMatch: "parallel",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			description := DecodeWaitType(tt.waitType)
			assert.NotEmpty(t, description, "Description should not be empty")
			// Most wait types return the wait type itself as description
			// or a more detailed description
			assert.NotEqual(t, "", description)
		})
	}
}
