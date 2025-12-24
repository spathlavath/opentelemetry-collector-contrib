package commonutils

import (
	"fmt"
	"math"
	"strconv"
	"time"

	"go.uber.org/zap"
)

const TimestampFormat = "2006-01-02 15:04:05"

// FormatTimestamp formats a time.Time to standard timestamp string format
// Returns empty string if time is zero
func FormatTimestamp(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.Format(TimestampFormat)
}

// FormatInt64 converts an int64 to string
func FormatInt64(val int64) string {
	return strconv.FormatInt(val, 10)
}

// GenerateSQLIdentifierKey creates a unique key from SQL_ID and child number
func GenerateSQLIdentifierKey(sqlID string, childNumber int64) string {
	return fmt.Sprintf("%s#%d", sqlID, childNumber)
}

// ParseIntSafe safely parses a string to int64, handling overflow cases.
// Returns -1 for empty strings or parsing errors.
// For very large numbers that exceed int64 max, returns math.MaxInt64.
func ParseIntSafe(value string, logger *zap.Logger) int64 {
	if value == "" {
		return -1
	}

	var result int64
	_, err := fmt.Sscanf(value, "%d", &result)
	if err != nil {
		// If parsing fails, it's likely a very large number that exceeds int64 max
		// Oracle can return values that exceed int64 max (e.g., 18446744073709551615)
		if logger != nil {
			logger.Debug("Failed to parse large numeric value, using int64 max",
				zap.String("value", value),
				zap.Error(err))
		}
		return math.MaxInt64
	}

	return result
}
