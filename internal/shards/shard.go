package shards

import (
	"context"
	"kvshard/internal/commands"
	"kvshard/internal/store"
	"kvshard/internal/store/policy"
	"log/slog"
	"time"
)

// TODO: use this to clean up shard once shard is full
var (
	MaxKeyPerShard int = 256
)

type Shard struct {
	id     int
	store  *store.Store
	policy policy.EvictionPolicy

	logger *slog.Logger
}

func NewShard(ctx context.Context, id int, policy policy.EvictionPolicy, logger *slog.Logger) *Shard {
	return &Shard{
		id:     id,
		policy: policy,
		store:  store.NewStore(ctx, id, logger),
		logger: logger.WithGroup("shard").With("id", id),
	}
}

func (s *Shard) start(ctx context.Context) {
	// each shard will have its own event-loop managing its own state no shared state between shards.
	// following the shared-nothing architecture.

	ticker := time.NewTicker(5 * time.Second) //TODO: add a configuration blayer.
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			// stop shard if parent ctx is cancelled do cleanup here foreach shard
			s.logger.Info("shard recieved a shutdown request", "shard", s.id, "err", ctx.Err())
			s.cleanup(ctx)

			return
		case <-ticker.C:
			// cleanup expired keys every T durations
			go s.deleteStaleKeys()
		}
	}
}

func (s *Shard) deleteStaleKeys() {
	s.logger.Info("deleting stale keys", "shard", s.id, "size", s.store.Size())

	s.logger.Info("shard cleaned up", "shard", s.id, "size", s.store.Size())
}

func (s *Shard) cleanup(ctx context.Context) {

	// during clean up, we can flush the WAL buffer to the disk and close the WAL file to ensure that all data is persisted before shutting down the shard.
	if err := s.store.Flush(); err != nil {
		s.logger.Error("failed to flush WAL buffer during shard cleanup", "shard", s.id, "err", err)
	}

	s.logger.Info("shard cleanup done", "shard", s.id)
}

func (s *Shard) ID() int {
	return s.id
}

func (s *Shard) Execute(cmd *commands.Command) (*commands.Result, error) {
	return cmd.Execute(s.store, cmd.Args)
}
