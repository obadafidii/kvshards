package shards

import (
	"context"
	"fmt"
	"kvshard/internal/store/wal"
	"log/slog"
	"sync"

	"github.com/cespare/xxhash/v2"
)

type Manager struct {
	shards []*Shard

	wg            sync.WaitGroup
	ctx           context.Context
	cancelManager context.CancelFunc

	logger *slog.Logger
}

type Config struct {
	ShardCount int
	DataDir    string
	WAL        wal.Config
}

func NewManager(ctx context.Context, shardCount int, logger *slog.Logger) (*Manager, error) {
	return NewManagerWithConfig(ctx, Config{ShardCount: shardCount, DataDir: "data", WAL: wal.DefaultConfig()}, logger)
}

// NewManagerAt is equivalent to NewManager but stores shard WALs in dataDir.
// It is useful for isolated instances and tests.
func NewManagerAt(ctx context.Context, shardCount int, dataDir string, logger *slog.Logger) (*Manager, error) {
	return NewManagerWithConfig(ctx, Config{ShardCount: shardCount, DataDir: dataDir, WAL: wal.DefaultConfig()}, logger)
}

func NewManagerWithConfig(ctx context.Context, config Config, logger *slog.Logger) (*Manager, error) {
	return newManagerWithWALConfig(ctx, config.ShardCount, config.DataDir, config.WAL, logger)
}

func newManager(ctx context.Context, shardCount int, dataDir string, logger *slog.Logger) (*Manager, error) {
	return newManagerWithEncoding(ctx, shardCount, dataDir, wal.JSONEncoding, logger)
}

func newManagerWithEncoding(ctx context.Context, shardCount int, dataDir string, encoding wal.Encoding, logger *slog.Logger) (*Manager, error) {
	config := wal.DefaultConfig()
	config.Encoding = encoding
	return newManagerWithWALConfig(ctx, shardCount, dataDir, config, logger)
}

func newManagerWithWALConfig(ctx context.Context, shardCount int, dataDir string, config wal.Config, logger *slog.Logger) (*Manager, error) {
	if ctx == nil {
		return nil, fmt.Errorf("context cannot be nil")
	}
	if shardCount <= 0 {
		return nil, fmt.Errorf("shard count must be greater than zero")
	}
	if logger == nil {
		logger = slog.Default()
	}

	ctx, cancelShard := context.WithCancel(ctx)
	// create all shards
	var shards []*Shard
	for i := range shardCount {
		shard, err := newShardWithWALConfig(ctx, i, dataDir, config, logger)
		if err != nil {
			cancelShard()
			for _, opened := range shards {
				_ = opened.store.Close()
			}
			return nil, fmt.Errorf("create shard %d: %w", i, err)
		}
		shards = append(shards, shard)
	}

	// create manager
	m := &Manager{
		shards:        shards,
		ctx:           ctx,
		cancelManager: cancelShard,
		logger:        logger.WithGroup("manager"),
	}
	return m, nil
}

func (m *Manager) Run() {

	// start all shards
	m.start(m.ctx)

	<-m.ctx.Done()
	m.logger.Info("parent ctx cancelled, shutting down", "err", m.ctx.Err())
	m.cancelManager()

	// wait for all shards to close before exiting
	m.wg.Wait()
}

func (m *Manager) GetShard(key string) *Shard {
	// This gets the shard for the particular key, using deterministic hashing
	// to ensure that the same key always maps to the same shard.
	return m.shards[xxhash.Sum64String(key)%uint64(len(m.shards))]
}

func (m *Manager) ShardCount() int { return len(m.shards) }

func (m *Manager) start(ctx context.Context) {
	for _, shard := range m.shards {
		m.wg.Go(func() {
			shard.start(ctx)
		})
	}
}
