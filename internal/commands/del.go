package commands

import (
	"kvshard/internal/kverrors"
	"kvshard/internal/store"
)

func del(store store.Storage, args []string) (res *Result, err error) {
	if len(args) != 1 {
		return res, kverrors.ErrInvalidArguments
	}
	err = store.Delete(args[0])
	if err != nil {
		return res, err
	}
	return res, nil
}
