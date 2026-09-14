package commands

import (
	"kvshard/internal/kverrors"
	"kvshard/internal/store"
	"kvshard/internal/store/objects"
	"kvshard/toolkit"
	"time"
)

func put(store store.Storage, args []string) (res *Result, err error) {
	if len(args) < 2 || len(args) > 3 {
		return res, kverrors.ErrInvalidArguments
	}

	var ttlUnix int64
	key, val := args[0], args[1]
	if key == "" {
		return nil, kverrors.ErrInvalidArguments
	}
	if len(args) == 3 {
		var ttl time.Time
		ttl, err = toolkit.StringToTime(args[2])
		if err != nil {
			return nil, kverrors.ErrSystemError
		}
		ttlUnix = ttl.Unix()
	}

	obj := objects.New(key, val, ttlUnix)

	err = store.Put(key, obj)
	if err != nil {
		return nil, err
	}
	return res, nil
}
