package store

import (
	"fmt"
	"log/slog"
	"testing"
)

func TestStore(t *testing.T) {
	tests := map[string]struct {
		operation string
	}{
		"put - returns ok":             {},
		"put - updates value in store": {},
		"delete - remove values":       {},
		"get - non-existing key":       {},
		"get - existing key":           {},
	}

	for name, _ := range tests {
		t.Run(name, func(t *testing.T) {
			store := NewStore(t.Context(), 10, slog.Default())
			fmt.Printf("id=%d", store.shardID)
		})
	}
}
