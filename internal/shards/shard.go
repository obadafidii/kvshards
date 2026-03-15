package shards

import (
	"context"
	"fmt"
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
	fmt.Println("not implemented")
}

func (s *Shard) Execute(cmd *commands.Command) (*commands.Result, error) {
	return cmd.Execute(s.Store, cmd.Args)
}
