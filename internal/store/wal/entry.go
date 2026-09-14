package wal

import "fmt"

type Operation string

const (
	PutOperation    Operation = "put"
	DeleteOperation Operation = "delete"
)

// Entry is one durable mutation in a shard's log.
type Entry struct {
	Operation Operation `json:"operation"`
	Key       string    `json:"key"`
	Value     string    `json:"value,omitempty"`
	TTL       int64     `json:"ttl,omitempty"`
}

func (e Entry) validate() error {
	if e.Key == "" {
		return fmt.Errorf("WAL key cannot be empty")
	}
	if e.Operation != PutOperation && e.Operation != DeleteOperation {
		return fmt.Errorf("unknown WAL operation %q", e.Operation)
	}
	return nil
}
