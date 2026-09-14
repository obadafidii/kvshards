package commands

import (
	"errors"
	"io"
	"kvshard/internal/kverrors"
	"kvshard/internal/store"
	"log/slog"
	"strconv"
	"testing"
	"time"
)

func commandStore(t *testing.T) *store.Store {
	t.Helper()
	s, err := store.NewStoreAt(t.Context(), 0, t.TempDir(), slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestPutGetDeleteAndExists(t *testing.T) {
	s := commandStore(t)
	if result, err := put(s, []string{"key", "value"}); err != nil || result != nil {
		t.Fatalf("put() = (%v, %v)", result, err)
	}
	if result, err := get(s, []string{"key"}); err != nil || result.Data.(interface{ String() string }).String() != "value" {
		t.Fatalf("get() = (%v, %v)", result, err)
	}
	if result, err := exists(s, []string{"key"}); err != nil || result.Data != true {
		t.Fatalf("exists(present) = (%v, %v)", result, err)
	}
	if _, err := del(s, []string{"key"}); err != nil {
		t.Fatalf("del() error = %v", err)
	}
	if result, err := exists(s, []string{"key"}); err != nil || result.Data != false {
		t.Fatalf("exists(missing) = (%v, %v)", result, err)
	}
	if _, err := get(s, []string{"key"}); !errors.Is(err, kverrors.ErrKeyNotFound) {
		t.Fatalf("get(missing) error = %v", err)
	}
	if _, err := del(s, []string{"key"}); !errors.Is(err, kverrors.ErrKeyNotFound) {
		t.Fatalf("del(missing) error = %v", err)
	}
}

func TestPutTTL(t *testing.T) {
	s := commandStore(t)
	future := time.Now().Add(time.Hour).Unix()
	if _, err := put(s, []string{"key", "value", time.Unix(future, 0).Format("not-a-unix-time")}); !errors.Is(err, kverrors.ErrSystemError) {
		t.Fatalf("put(invalid TTL) error = %v", err)
	}
	if _, err := put(s, []string{"key", "value", stringInt(future)}); err != nil {
		t.Fatalf("put(valid TTL) error = %v", err)
	}
	value, _, _ := s.Get("key")
	if value.TTL() != future {
		t.Fatalf("TTL() = %d, want %d", value.TTL(), future)
	}
}

func TestCommandsRejectWrongArgumentCounts(t *testing.T) {
	s := commandStore(t)
	tests := map[string]func() error{
		"put none":     func() error { _, err := put(s, nil); return err },
		"put one":      func() error { _, err := put(s, []string{"key"}); return err },
		"put too many": func() error { _, err := put(s, []string{"key", "value", "1", "extra"}); return err },
		"get none":     func() error { _, err := get(s, nil); return err },
		"get too many": func() error { _, err := get(s, []string{"a", "b"}); return err },
		"del none":     func() error { _, err := del(s, nil); return err },
		"exists none":  func() error { _, err := exists(s, nil); return err },
	}
	for name, run := range tests {
		t.Run(name, func(t *testing.T) {
			if err := run(); !errors.Is(err, kverrors.ErrInvalidArguments) {
				t.Fatalf("error = %v, want ErrInvalidArguments", err)
			}
		})
	}
}

func TestRegistryContainsSupportedCommands(t *testing.T) {
	for _, name := range []string{"put", "get", "del", "exists"} {
		if Registry[name] == nil {
			t.Errorf("Registry[%q] is missing", name)
		}
	}
}

func stringInt(value int64) string {
	return strconv.FormatInt(value, 10)
}
