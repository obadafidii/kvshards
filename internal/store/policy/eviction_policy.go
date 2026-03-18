package policy

type EvictionPolicy string

var (
	TimeBasedPolicy EvictionPolicy = "time_based"
	LastUsedPolicy  EvictionPolicy = "last_used"
)
