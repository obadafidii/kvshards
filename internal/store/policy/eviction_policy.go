package policy

type EvictionPolicy string

func (ep EvictionPolicy) String() string {
	return string(ep)
}

var (
	TimeBasedPolicy EvictionPolicy = "time_based"
	LastUsedPolicy  EvictionPolicy = "last_used"
)
