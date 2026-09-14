package store

import (
	"errors"
	"fmt"
	"io"
	"kvshard/internal/kverrors"
	"kvshard/internal/store/objects"
	"kvshard/internal/store/wal"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"
)

func newTestStore(t *testing.T, shardID int) *Store {
	t.Helper()
	store, err := NewStoreAt(t.Context(), shardID, t.TempDir(), slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("NewStoreAt() error = %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func TestStorePutGetAndOverwrite(t *testing.T) {
	store := newTestStore(t, 10)
	if err := store.Put("key", objects.New("key", "one", 0)); err != nil {
		t.Fatalf("Put() error = %v", err)
	}
	if err := store.Put("key", objects.New("key", "two", 0)); err != nil {
		t.Fatalf("overwrite Put() error = %v", err)
	}
	got, ok, err := store.Get("key")
	if err != nil || !ok || got.String() != "two" {
		t.Fatalf("Get() = (%v, %v, %v), want value two", got, ok, err)
	}
	if store.Size() != 1 || !store.Exists("key") {
		t.Fatalf("stored key missing or Size() = %d, want 1", store.Size())
	}
}

func TestStoreRejectsInvalidPutWithoutMutation(t *testing.T) {
	store := newTestStore(t, 0)
	for name, input := range map[string]struct {
		key   string
		value *objects.Object
	}{
		"empty key": {value: objects.New("", "value", 0)},
		"nil value": {key: "key"},
	} {
		t.Run(name, func(t *testing.T) {
			if err := store.Put(input.key, input.value); !errors.Is(err, kverrors.ErrInvalidArguments) {
				t.Fatalf("Put() error = %v, want ErrInvalidArguments", err)
			}
		})
	}
	if store.Size() != 0 {
		t.Fatalf("Size() = %d, want 0", store.Size())
	}
}

func TestStoreDelete(t *testing.T) {
	store := newTestStore(t, 0)
	if err := store.Put("key", objects.New("key", "value", 0)); err != nil {
		t.Fatal(err)
	}
	if err := store.Delete("key"); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	if _, ok, err := store.Get("key"); ok || !errors.Is(err, kverrors.ErrKeyNotFound) {
		t.Fatalf("Get(deleted) = ok %v, err %v", ok, err)
	}
	if err := store.Delete("key"); !errors.Is(err, kverrors.ErrKeyNotFound) {
		t.Fatalf("second Delete() error = %v, want ErrKeyNotFound", err)
	}
	if store.Size() != 0 {
		t.Fatalf("Size() = %d, want 0", store.Size())
	}
}

func TestStoreExpirationIsEagerAndSweepable(t *testing.T) {
	store := newTestStore(t, 0)
	now := time.Now()
	_ = store.Put("expired-on-read", objects.New("expired-on-read", "value", now.Add(-time.Second).Unix()))
	_ = store.Put("expired-on-sweep", objects.New("expired-on-sweep", "value", now.Add(-time.Second).Unix()))
	_ = store.Put("live", objects.New("live", "value", now.Add(time.Hour).Unix()))
	_ = store.Put("persistent", objects.New("persistent", "value", 0))

	if _, ok, err := store.Get("expired-on-read"); ok || !errors.Is(err, kverrors.ErrKeyNotFound) {
		t.Fatalf("Get(expired) = ok %v, err %v", ok, err)
	}
	if deleted := store.DeleteExpired(now); deleted != 1 {
		t.Fatalf("DeleteExpired() = %d, want 1", deleted)
	}
	if store.Size() != 2 || !store.Exists("live") || !store.Exists("persistent") {
		t.Fatalf("unexpected live set after expiry cleanup; size = %d", store.Size())
	}
}

func TestStoreRestoresWAL(t *testing.T) {
	dir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	first, err := NewStoreAt(t.Context(), 3, dir, logger)
	if err != nil {
		t.Fatal(err)
	}
	_ = first.Put("updated", objects.New("updated", "old", 0))
	_ = first.Put("updated", objects.New("updated", "new", 0))
	_ = first.Put("deleted", objects.New("deleted", "gone", 0))
	_ = first.Delete("deleted")
	_ = first.Put("expired", objects.New("expired", "stale", time.Now().Add(-time.Second).Unix()))
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	restored, err := NewStoreAt(t.Context(), 3, dir, logger)
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	got, ok, err := restored.Get("updated")
	if err != nil || !ok || got.String() != "new" {
		t.Fatalf("restored Get() = (%v, %v, %v)", got, ok, err)
	}
	if restored.Exists("deleted") || restored.Exists("expired") || restored.Size() != 1 {
		t.Fatalf("restored store contains deleted/expired data; size = %d", restored.Size())
	}
}

func TestStoreRestoresBinaryWAL(t *testing.T) {
	dir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	first, err := NewStoreAtWithEncoding(t.Context(), 8, dir, wal.BinaryEncoding, logger)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Put("key", objects.New("key", "binary-value", 0)); err != nil {
		t.Fatal(err)
	}
	_ = first.Close()
	restored, err := NewStoreAtWithEncoding(t.Context(), 8, dir, wal.BinaryEncoding, logger)
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	got, ok, err := restored.Get("key")
	if err != nil || !ok || got.String() != "binary-value" {
		t.Fatalf("binary restored Get() = (%v, %v, %v)", got, ok, err)
	}
}

func TestStoreDoesNotMutateWhenWALIsClosed(t *testing.T) {
	store := newTestStore(t, 0)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := store.Put("key", objects.New("key", "value", 0)); err == nil {
		t.Fatal("Put() error = nil after WAL close")
	}
	if store.Size() != 0 {
		t.Fatalf("Size() = %d, want 0", store.Size())
	}
}

func TestStoreConcurrentWritesKeepAccurateSize(t *testing.T) {
	store := newTestStore(t, 0)
	const keys = 100
	var wg sync.WaitGroup
	for i := range keys {
		wg.Add(2)
		go func() {
			defer wg.Done()
			key := fmt.Sprintf("key-%d", i)
			_ = store.Put(key, objects.New(key, "value", 0))
		}()
		go func() {
			defer wg.Done()
			_ = store.Put("shared", objects.New("shared", fmt.Sprint(i), 0))
		}()
	}
	wg.Wait()
	if store.Size() != keys+1 {
		t.Fatalf("Size() = %d, want %d", store.Size(), keys+1)
	}
}

func TestStoreCompactionPersistsOnlyCurrentLiveState(t *testing.T) {
	dir := t.TempDir()
	config := wal.DefaultConfig()
	config.Compression = wal.GZIPCompression
	config.CompactionThreshold = 3
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	s, err := NewStoreAtWithWALConfig(t.Context(), 0, dir, config, logger)
	if err != nil {
		t.Fatal(err)
	}
	for i := range 8 {
		_ = s.Put("current", objects.New("current", fmt.Sprintf("value-%d", i), 0))
	}
	_ = s.Put("deleted", objects.New("deleted", "value", 0))
	_ = s.Delete("deleted")
	_ = s.Put("expired", objects.New("expired", "value", time.Now().Add(-time.Second).Unix()))
	before, _ := os.Stat(s.wal.Path())
	if !s.NeedsCompaction() {
		t.Fatal("NeedsCompaction() = false")
	}
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	after, _ := os.Stat(s.wal.Path())
	if after.Size() >= before.Size() {
		t.Fatalf("compacted WAL size %d is not smaller than %d", after.Size(), before.Size())
	}
	if s.Size() != 1 || s.Exists("deleted") || s.Exists("expired") {
		t.Fatalf("unexpected state after compaction; size = %d", s.Size())
	}
	_ = s.Close()

	restored, err := NewStoreAtWithWALConfig(t.Context(), 0, dir, config, logger)
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	value, ok, err := restored.Get("current")
	if err != nil || !ok || value.String() != "value-7" || restored.Size() != 1 {
		t.Fatalf("restored compacted state = (%v, %v, %v), size %d", value, ok, err, restored.Size())
	}
}
