package objects

import (
	"testing"
	"time"
)

func TestObjectAccessorsAndExpiration(t *testing.T) {
	now := time.Unix(100, 0)
	tests := []struct {
		name    string
		ttl     int64
		expired bool
	}{
		{name: "no expiry", ttl: 0},
		{name: "negative means no expiry", ttl: -1},
		{name: "future", ttl: 101},
		{name: "exact boundary", ttl: 100, expired: true},
		{name: "past", ttl: 99, expired: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			obj := New("key", "value", test.ttl)
			if obj.ID() != "key" || obj.Value() != "value" || obj.TTL() != test.ttl || obj.String() != "value" {
				t.Fatalf("unexpected object accessors: %#v", obj)
			}
			if got := obj.IsExpired(now); got != test.expired {
				t.Fatalf("IsExpired() = %v, want %v", got, test.expired)
			}
		})
	}
}

func TestObjectStringFormatsNonStringValue(t *testing.T) {
	if got := New("key", 42, 0).String(); got != "42" {
		t.Fatalf("String() = %q, want 42", got)
	}
}
