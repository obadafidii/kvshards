package shards

import (
	"context"
	"kvshard/internal/commands"
	"kvshard/internal/store"
)

type Shard struct {
	ID    int
	Store *store.Store
}

func NewShard(id int) *Shard {
	return &Shard{
		ID:    id,
		Store: store.NewStore(id),
	}
}

func (s *Shard) start(ctx context.Context) {
	// each shard will have its own event-loop managing its own state
	// no shared state between shards.
	for {
		select {
		case <-ctx.Done():
			// stop shard if parent ctx is cancelled
			// do cleanup here foreach shard
		default:
			return
		}
	}
}

func (s *Shard) Execute(cmd *commands.Command) (*commands.Result, error) {
	return cmd.Execute(s.Store, cmd.Args)
}
