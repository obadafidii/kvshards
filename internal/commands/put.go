package commands

import (
	"fmt"
	"kvshard/internal/kverrors"
	"kvshard/internal/store"
)

func put(store store.Storage, args []string) (res *Result, err error) {
	if len(args) != 2 {
		fmt.Printf("Put expects 2 arguments, got=%d, args=%v\n", len(args), args)
		return res, kverrors.ErrInvalidArguments
	}
	err = store.Put(args[0], args[1])
	if err != nil {
		return nil, err
	}
	return res, nil
}
