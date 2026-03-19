package commands

import (
	"fmt"
	"kvshard/internal/kverrors"
	"kvshard/internal/store"
	"kvshard/internal/store/objects"
	"kvshard/toolkit"
	"time"
)

func put(store store.Storage, args []string) (res *Result, err error) {
	if len(args) < 2 {
		fmt.Printf("Put expects 2 arguments, got=%d, args=%v\n", len(args), args)
		return res, kverrors.ErrInvalidArguments
	}

	var ttl time.Time
	key, val := args[0], args[1]
	if len(args) == 3 {
		ttl, err = toolkit.StringToTime(args[2])
		if err != nil {
			return nil, kverrors.ErrSystemError
		}
	}

	obj := objects.New(key, val, ttl.Unix())

	err = store.Put(key, obj)
	if err != nil {
		return nil, err
	}
	return res, nil
}
