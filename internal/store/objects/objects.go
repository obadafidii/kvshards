package objects

import (
	"fmt"
	"time"
)

// Object: improve the Data in Store, so that we can be tracking time to live.
type Object struct {
	id  string // this the key
	val any    // this is actual value
	ttl int64  // time configuration strategy
}

func New(id string, val any, ttl int64) *Object {
	return &Object{
		id:  id,
		val: val,
		ttl: ttl,
	}
}

func (o *Object) String() string {
	return fmt.Sprint(o.val)
}

func (o *Object) ID() string { return o.id }

func (o *Object) Value() any { return o.val }

func (o *Object) TTL() int64 {
	return o.ttl
}

// IsExpired reports whether the object's absolute Unix expiry has passed.
// A non-positive TTL means that the object does not expire.
func (o *Object) IsExpired(now time.Time) bool {
	return o.ttl > 0 && now.Unix() >= o.ttl
}
