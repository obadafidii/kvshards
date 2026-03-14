package commands

import (
	"kvshard/internal/kverrors"
	"kvshard/internal/store"
)

func get(store store.Storage, args []string) (res *Result, err error) {
	if len(args) != 1 {
		return res, kverrors.ErrInvalidArguments
	}
	value, ok, err := store.Get(args[0])
	if !ok || err != nil {
		return nil, err
	}
	res = &Result{Data: value}

	return res, nil
}
