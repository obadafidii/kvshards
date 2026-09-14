package commands

import (
	"kvshard/internal/kverrors"
	"kvshard/internal/store"
)

// exists is a multi-shard command
// I will work on this later.
func exists(store store.Storage, args []string) (res *Result, err error) {
	if len(args) != 1 {
		return res, kverrors.ErrInvalidArguments
	}
	return &Result{Data: store.Exists(args[0])}, nil
}
