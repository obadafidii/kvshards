package commands

import (
	"errors"
	"kvshard/internal/store"
)

var (
	ErrInvalidArguments = errors.New("invalid arguments")
)

type Result struct{}

type Command struct {
	Name        string
	Args        []string
	Description string
	Execute     func(store *store.Store, args []string) (*Result, error)
}
