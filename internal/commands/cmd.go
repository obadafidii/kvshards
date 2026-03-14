package commands

import (
	"kvshard/internal/store"
)

type Result struct {
	Data interface{}
}

type Command struct {
	Name        string
	Args        []string
	Description string
	Execute     func(store store.Storage, args []string) (*Result, error)
}
