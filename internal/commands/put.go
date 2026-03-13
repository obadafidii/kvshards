package commands

import (
	"kvshard/internal/kverrors"
	"kvshard/internal/store"
)

func put(store *store.Store, args []string) (res *Result, err error) {
	if len(args) != 2 {
		return res, kverrors.ErrInvalidArguments
	}
	return res, nil
}
