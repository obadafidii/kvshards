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

func NewShard(id int, policy policy.EvictionPolicy, logger *slog.Logger) *Shard {
	return &Shard{
		id:     id,
		policy: policy,
		store:  store.NewStore(id),
		logger: logger.WithGroup("shard").With("id", id),
	}
}

func (s *Shard) start(ctx context.Context) {
	// each shard will have its own event-loop managing its own state no shared state between shards.
	// following the shared-nothing architecture.

	ticker := time.NewTicker(5 * time.Minute) //TODO: add a configuration blayer.
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			// stop shard if parent ctx is cancelled do cleanup here foreach shard
			s.logger.Info("shard recieved a shutdown request", "shard", s.id, "err", ctx.Err())
			s.cleanup()
			s.logger.Info("shard cleanup done", "shard", s.id)

			return
		case <-ticker.C:
			// cleanup expired keys every T durations
			s.logger.Info("starting periodic cleaning", "shard", s.id, "size", s.store.Size())
			s.cleanup()
			s.logger.Info("shard cleaned up", "shard", s.id, "size", s.store.Size())
		}
	}
}

func (s *Shard) cleanup() {
}

func (s *Shard) ID() int {
	return s.id
}

func (s *Shard) Execute(cmd *commands.Command) (*commands.Result, error) {
	return cmd.Execute(s.store, cmd.Args)
}
