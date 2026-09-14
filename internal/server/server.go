package server

import (
	"bufio"
	"context"
	"fmt"
	"kvshard/internal/commands"
	"kvshard/internal/kverrors"
	"kvshard/internal/shards"
	"kvshard/internal/store/wal"
	"log/slog"
	"net"
	"strings"
)

type server struct {
	manager *shards.Manager
	ctx     context.Context

	logger *slog.Logger
}

func Start(ctx context.Context, numOfShards int, logger *slog.Logger) error {
	return StartWithWALEncoding(ctx, numOfShards, wal.JSONEncoding, logger)
}

func StartWithWALEncoding(ctx context.Context, numOfShards int, encoding wal.Encoding, logger *slog.Logger) error {
	config := wal.DefaultConfig()
	config.Encoding = encoding
	return StartWithWALConfig(ctx, numOfShards, config, logger)
}

func StartWithWALConfig(ctx context.Context, numOfShards int, walConfig wal.Config, logger *slog.Logger) error {
	if logger == nil {
		logger = slog.Default()
	}
	ln, err := net.Listen("tcp", ":9500")
	if err != nil {
		return fmt.Errorf("listen on port 9500: %w", err)
	}
	manager, err := shards.NewManagerWithConfig(ctx, shards.Config{
		ShardCount: numOfShards,
		DataDir:    "data",
		WAL:        walConfig,
	}, logger)
	if err != nil {
		_ = ln.Close()
		return fmt.Errorf("create shard manager: %w", err)
	}

	svr := &server{
		ctx:     ctx,
		logger:  logger.WithGroup("server"),
		manager: manager,
	}

	go svr.manager.Run()

	go func() {
		<-ctx.Done()
		svr.logger.Info("context cancelled shutting down server")
		_ = ln.Close()
	}()

	for {
		conn, aerr := ln.Accept()
		if aerr != nil {
			// if the listener closed then stop close the server also (graceful degradation)
			if ctx.Err() != nil {
				svr.logger.Info("listener closed sever exiting", "err", ctx.Err())
				break
			}
			continue
		}

		go svr.handle(conn)
	}
	return nil
}

func (s *server) handle(conn net.Conn) {
	defer conn.Close()

	scanner := bufio.NewScanner(conn)

	for scanner.Scan() {
		line := scanner.Text()

		// Fields accepts repeated spaces and CRLF, and makes whitespace-only
		// input safe to ignore.
		input := strings.Fields(line)
		if len(input) == 0 {
			continue
		}

		// command is at the first index
		// ["put|get|del|exists", ...args]
		cmd := input[0]
		registered, ok := commands.Registry[strings.ToLower(cmd)]
		if !ok {
			s.logger.Warn("unknown command", "command", cmd)
			conn.Write([]byte(kverrors.ErrUnknownCommand.Error() + "\n"))
			continue
		}

		// Commands in the registry are immutable templates. Copying prevents
		// concurrent clients from overwriting each other's arguments.
		command := *registered
		command.Args = append([]string(nil), input[1:]...)
		if len(command.Args) < command.MinArgs || len(command.Args) > command.MaxArgs {
			s.logger.Info("invalid args provided", "min-args-expected", command.MinArgs, "max-args-expected", command.MaxArgs, "args-provided", len(command.Args))
			conn.Write([]byte(kverrors.ErrInvalidArguments.Error() + "\n"))
			continue
		}

		// get the shard responsible for this key
		key := input[1] // key
		shard := s.manager.GetShard(key)

		// perform the operation on the shard
		result, err := shard.Execute(&command)

		if err != nil {
			s.logger.Info("error executing command", "error", err)
			conn.Write([]byte(err.Error() + "\n"))
			continue
		}

		// send response
		if result == nil {
			conn.Write([]byte("ok\n"))
			continue
		}

		conn.Write([]byte(fmt.Sprint(result.Data) + "\n"))
	}
}
