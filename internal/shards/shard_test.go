package shards

import (
	"context"
	"errors"
	"io"
	"kvshard/internal/commands"
	"kvshard/internal/store/objects"
	"kvshard/internal/store/wal"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func TestShardExecuteAndExpiryCleanup(t *testing.T) {
	shard, err := newShard(t.Context(), 2, t.TempDir(), quietLogger())
	if err != nil {
		t.Fatal(err)
	}
	defer shard.store.Close()
	if shard.ID() != 2 {
		t.Fatalf("ID() = %d, want 2", shard.ID())
	}
	put := &commands.Command{Args: []string{"key", "value"}, Execute: commands.Registry["put"].Execute}
	if _, err := shard.Execute(put); err != nil {
		t.Fatalf("Execute(put) error = %v", err)
	}
	if !shard.store.Exists("key") {
		t.Fatal("put command did not reach shard store")
	}
	_ = shard.store.Put("expired", objects.New("expired", "value", time.Now().Add(-time.Second).Unix()))
	if deleted := shard.deleteStaleKeys(time.Now()); deleted != 1 {
		t.Fatalf("deleteStaleKeys() = %d, want 1", deleted)
	}
	if _, err := shard.Execute(nil); !errors.Is(err, commands.ErrInvalidCommand) {
		t.Fatalf("Execute(nil) error = %v", err)
	}
}

func TestManagerRoutesDeterministicallyAndShutsDown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	manager, err := newManager(ctx, 4, t.TempDir(), quietLogger())
	if err != nil {
		t.Fatal(err)
	}
	if manager.ShardCount() != 4 {
		t.Fatalf("ShardCount() = %d, want 4", manager.ShardCount())
	}
	first := manager.GetShard("stable-key")
	for range 20 {
		if manager.GetShard("stable-key") != first {
			t.Fatal("same key routed to different shards")
		}
	}
	for i := range 100 {
		id := manager.GetShard(string(rune(i))).ID()
		if id < 0 || id >= manager.ShardCount() {
			t.Fatalf("GetShard() ID = %d, outside valid range", id)
		}
	}

	done := make(chan struct{})
	go func() { manager.Run(); close(done) }()
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Manager.Run() did not stop after cancellation")
	}
	if err := first.store.Put("after-close", objects.New("after-close", "value", 0)); err == nil {
		t.Fatal("shard store accepted a write after manager shutdown")
	}
}

func TestShardAndManagerRejectInvalidConfiguration(t *testing.T) {
	if _, err := newShard(t.Context(), -1, t.TempDir(), quietLogger()); err == nil {
		t.Fatal("newShard(-1) error = nil")
	}
	for _, count := range []int{0, -1} {
		if _, err := newManager(t.Context(), count, t.TempDir(), quietLogger()); err == nil {
			t.Fatalf("newManager(%d) error = nil", count)
		}
	}
	if _, err := newShard(nil, 0, t.TempDir(), quietLogger()); err == nil {
		t.Fatal("newShard(nil context) error = nil")
	}
	if _, err := newManager(nil, 1, t.TempDir(), quietLogger()); err == nil {
		t.Fatal("newManager(nil context) error = nil")
	}
}

func TestManagerUsesConfiguredBinaryWALAndRestoresIt(t *testing.T) {
	dir := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	walConfig := wal.DefaultConfig()
	walConfig.Encoding = wal.BinaryEncoding
	config := Config{ShardCount: 2, DataDir: dir, WAL: walConfig}
	manager, err := NewManagerWithConfig(ctx, config, quietLogger())
	if err != nil {
		t.Fatal(err)
	}
	shard := manager.GetShard("key")
	if err := shard.store.Put("key", objects.New("key", "value", 0)); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { manager.Run(); close(done) }()
	cancel()
	<-done

	ctx, cancel = context.WithCancel(context.Background())
	restored, err := NewManagerWithConfig(ctx, config, quietLogger())
	if err != nil {
		t.Fatal(err)
	}
	got, ok, err := restored.GetShard("key").store.Get("key")
	if err != nil || !ok || got.String() != "value" {
		t.Fatalf("restored binary shard value = (%v, %v, %v)", got, ok, err)
	}
	done = make(chan struct{})
	go func() { restored.Run(); close(done) }()
	cancel()
	<-done
}

func TestShardAutomaticallyCompactsStaleHeavyWAL(t *testing.T) {
	dir := t.TempDir()
	config := wal.DefaultConfig()
	config.CompactionThreshold = 3
	shard, err := newShardWithWALConfig(t.Context(), 2, dir, config, quietLogger())
	if err != nil {
		t.Fatal(err)
	}
	defer shard.store.Close()
	for i := range 8 {
		value := strings.Repeat("x", i+20)
		if err := shard.store.Put("key", objects.New("key", value, 0)); err != nil {
			t.Fatal(err)
		}
	}
	path := filepath.Join(dir, "wal_2.log")
	before, _ := os.Stat(path)
	shard.deleteStaleKeys(time.Now())
	after, _ := os.Stat(path)
	if after.Size() >= before.Size() {
		t.Fatalf("automatic compaction size = %d, want less than %d", after.Size(), before.Size())
	}
}
