package shards

import (
	"context"
	"kvshard/internal/store/policy"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/cespare/xxhash/v2"
)

type Manager struct {
	shards     []*Shard
	signalChan chan os.Signal

	logger *slog.Logger
}

func NewManager(shardCount int, logger *slog.Logger) *Manager {
	// create all shards
	var shards []*Shard
	for i := range shardCount {
		shard := NewShard(i, policy.TimeBasedPolicy, logger)
		shards = append(shards, shard)
	}

	// create manager
	m := &Manager{
		shards:     shards,
		signalChan: make(chan os.Signal, 1),
		logger:     logger.WithGroup("manager"),
	}

	return m
}

func (m *Manager) Run(ctx context.Context) {
	signal.Notify(m.signalChan, syscall.SIGINT, syscall.SIGTERM)

	ctx, cancelShard := context.WithCancel(ctx)
	wg := sync.WaitGroup{}

	// start all shards
	go m.start(ctx, &wg)

	<-ctx.Done()
	m.logger.Info("parent ctx cancelled, shutting down", "err", ctx.Err())
	cancelShard()

	// wait for all shards to close before exiting
	wg.Wait()
}

func (m *Manager) GetShard(key string) *Shard {
	// This gets the shard for the particular key, using deterministic hashing
	// to ensure that the same key always maps to the same shard.
	return m.shards[xxhash.Sum64String(key)%uint64(len(m.shards))]
}

func (m *Manager) start(ctx context.Context, wg *sync.WaitGroup) {
	for _, shard := range m.shards {
		wg.Go(func() {
			shard.start(ctx)
		})
	}
}
