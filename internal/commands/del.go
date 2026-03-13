package commands

import (
	"kvshard/internal/kverrors"
	"kvshard/internal/store"
)

func del(store *store.Store, args []string) (res *Result, err error) {
	if len(args) != 1 {
		return res, kverrors.ErrInvalidArguments
	}
	return res, nil
}
