package shards

import (
	"context"
	"fmt"
	"kvshard/internal/commands"
	"kvshard/internal/store"
	"kvshard/internal/store/wal"
	"log/slog"
	"time"
)

const cleanupInterval = 5 * time.Second

type Shard struct {
	id    int
	store *store.Store

	logger *slog.Logger
}

func NewShard(ctx context.Context, id int, logger *slog.Logger) (*Shard, error) {
	return newShard(ctx, id, "data", logger)
}

func newShard(ctx context.Context, id int, dataDir string, logger *slog.Logger) (*Shard, error) {
	return newShardWithEncoding(ctx, id, dataDir, wal.JSONEncoding, logger)
}

func newShardWithEncoding(ctx context.Context, id int, dataDir string, encoding wal.Encoding, logger *slog.Logger) (*Shard, error) {
	config := wal.DefaultConfig()
	config.Encoding = encoding
	return newShardWithWALConfig(ctx, id, dataDir, config, logger)
}

func newShardWithWALConfig(ctx context.Context, id int, dataDir string, config wal.Config, logger *slog.Logger) (*Shard, error) {
	if ctx == nil {
		return nil, fmt.Errorf("context cannot be nil")
	}
	if id < 0 {
		return nil, fmt.Errorf("shard ID must be non-negative")
	}
	if logger == nil {
		logger = slog.Default()
	}
	shardStore, err := store.NewStoreAtWithWALConfig(ctx, id, dataDir, config, logger)
	if err != nil {
		return nil, err
	}
	return &Shard{
		id:     id,
		store:  shardStore,
		logger: logger.WithGroup("shard").With("id", id),
	}, nil
}

func (s *Shard) start(ctx context.Context) {
	// each shard will have its own event-loop managing its own state no shared state between shards.
	// following the shared-nothing architecture.

	ticker := time.NewTicker(cleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			// stop shard if parent ctx is cancelled do cleanup here foreach shard
			s.logger.Info("shard received a shutdown request", "shard", s.id, "err", ctx.Err())
			s.cleanup()

			return
		case now := <-ticker.C:
			// cleanup expired keys every T durations
			s.deleteStaleKeys(now)
		}
	}
}

func (s *Shard) deleteStaleKeys(now time.Time) int {
	s.logger.Info("deleting stale keys", "shard", s.id, "size", s.store.Size())
	deleted := s.store.DeleteExpired(now)
	if s.store.NeedsCompaction() {
		if err := s.store.Compact(); err != nil {
			s.logger.Error("failed to compact shard WAL", "shard", s.id, "err", err)
		}
	}
	s.logger.Info("shard cleaned up", "shard", s.id, "deleted", deleted, "size", s.store.Size())
	return deleted
}

func (s *Shard) cleanup() {
	// during cleanup, we can flush the WAL buffer to the disk and close the WAL file to ensure that all data is persisted before shutting down the shard.
	if err := s.store.Close(); err != nil {
		s.logger.Error("failed to flush WAL buffer during shard cleanup", "shard", s.id, "err", err)
	}

	s.logger.Info("shard cleanup done", "shard", s.id)
}

func (s *Shard) ID() int {
	return s.id
}

func (s *Shard) Execute(cmd *commands.Command) (*commands.Result, error) {
	if cmd == nil || cmd.Execute == nil {
		return nil, commands.ErrInvalidCommand
	}
	return cmd.Execute(s.store, cmd.Args)
}
