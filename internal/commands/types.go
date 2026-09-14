package commands

import (
	"errors"
)

var ErrInvalidCommand = errors.New("invalid command")

type Result struct {
	Data any
}
