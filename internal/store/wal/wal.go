package wal

import (
	"context"
	"log/slog"
)

type WAL struct{}

func New(ctx context.Context, shardID int, logger *slog.Logger) (*WAL, error) {
	return &WAL{}, nil
}
