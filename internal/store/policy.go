package store

// Policy determines the sync mode of the WAL
type Policy string

const (
	Manual          Policy = "manual"
	TimeBasedPolicy Policy = "time-based"
	BufferedPolicy  Policy = "buffered"
	BothPolicy      Policy = "both"
)
