package kverrors

import "errors"

var (
	// shard errors
	ErrKeyNotFound = errors.New("key not found")

	// command errors
	ErrInvalidArguments = errors.New("invalid arguments")
	ErrUnknownCommand   = errors.New("unknown command")
)
