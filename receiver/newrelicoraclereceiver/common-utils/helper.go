package commonutils

import (
	"fmt"
	"strconv"
	"time"
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
