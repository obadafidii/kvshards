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
	shards []*Shard

	wg           *sync.WaitGroup
	signalChan   chan os.Signal
	ctx          context.Context
	cancelManger context.CancelFunc

	logger *slog.Logger
}

func NewManager(ctx context.Context, shardCount int, logger *slog.Logger) *Manager {

	ctx, cancelShard := context.WithCancel(ctx)
	wg := sync.WaitGroup{}

	// create all shards
	var shards []*Shard
	for i := range shardCount {
		shard := NewShard(ctx, i, policy.TimeBasedPolicy, logger)
		shards = append(shards, shard)
	}

	// create manager
	m := &Manager{
		shards:       shards,
		ctx:          ctx,
		wg:           &wg,
		cancelManger: cancelShard,
		signalChan:   make(chan os.Signal, 1),
		logger:       logger.WithGroup("manager"),
	}

	// register for shutdown signals to gracefully shutdown the manager and its shards
	signal.Notify(m.signalChan, syscall.SIGINT, syscall.SIGTERM)

	return m
}

func (m *Manager) Run() {

	// start all shards
	go m.start(m.ctx, m.wg)

	<-m.ctx.Done()
	m.logger.Info("parent ctx cancelled, shutting down", "err", m.ctx.Err())
	m.cancelManger()

	// wait for all shards to close before exiting
	m.wg.Wait()
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
