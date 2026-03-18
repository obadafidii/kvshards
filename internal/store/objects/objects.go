package objects

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

func (o *Object) TTL() int64 {
	return o.ttl
}

// func (o *Object) IsExpired() bool {
// 	return time.Now().After(o.ttl)
// }
