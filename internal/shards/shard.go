package shards

import (
	"context"
	"kvshard/internal/commands"
	"kvshard/internal/store"
	"log/slog"
)

type Shard struct {
	ID    int
	Store *store.Store

	logger *slog.Logger
}

func NewShard(id int, logger *slog.Logger) *Shard {
	return &Shard{
		ID:    id,
		Store: store.NewStore(id),
		logger: logger.WithGroup("shard").With("id", id),
	}
}

func (s *Shard) start(ctx context.Context) {
	// each shard will have its own event-loop managing its own state
	// no shared state between shards.

	//TODO: implement keys cleanup when I implement TTL
	//TODO: clean-up time should be configurable
	// ticker := time.NewTicker(1 * time.Second)
	// defer ticker.Stop()


	for {
		select {
		case <-ctx.Done():
			// stop shard if parent ctx is cancelled
			// do cleanup here foreach shard
			s.cleanup()
			s.logger.Info("shard stopped", "shard", s.ID, "err", ctx.Err())
			return
		// case <-ticker.C:
		// 	// cleanup expired keys every T durations
		// 	s.cleanup()
		default:
			continue
		}
	}
}

func (s *Shard) cleanup() {
	s.logger.Info("not implemented")
}

func (s *Shard) Execute(cmd *commands.Command) (*commands.Result, error) {
	s.logger.Info("executing command", "command", cmd.Name, "args", cmd.Args)
	return cmd.Execute(s.Store, cmd.Args)
}
