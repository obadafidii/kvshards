package toolkit

import (
	"strconv"
	"time"
)

func StringToTime(s string) (time.Time, error) {
	// Convert string "200" to int64
	sec, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return time.Time{}, err
	}

	// Return as time.Time
	return time.Unix(sec, 0), nil
}
