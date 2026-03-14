package server

import (
	"bufio"
	"context"
	"kvshard/internal/commands"
	"kvshard/internal/kverrors"
	"kvshard/internal/shards"
	"log/slog"
	"net"
	"os"
	"strings"
)

type server struct {
	manager *shards.Manager
	ctx     context.Context
	logger  *slog.Logger

	signalChan chan os.Signal
}

func Start(ctx context.Context, logger *slog.Logger, numOfShards int) {
	ln, err := net.Listen("tcp", ":9500")
	if err != nil {
		panic(err)
	}

	svr := &server{
		ctx:        ctx,
		logger:     logger,
		manager:    shards.NewManager(numOfShards),
		signalChan: make(chan os.Signal, 1),
	}

	for {
		select {
		case <-svr.ctx.Done():
			return
		case <-svr.signalChan:
			return
		default:
			conn, aerr := ln.Accept()
			if aerr != nil {
				conn.Write([]byte(err.Error()))
			}

			go svr.handleConnection(conn)
		}

	}
}

func (s *server) handleConnection(conn net.Conn) {
	defer conn.Close()

	scanner := bufio.NewScanner(conn)

	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}

		// get the commands and keys
		msg := strings.Split(line, " ")
		cmd, key := msg[0], msg[1]
		command, ok := commands.Registry[strings.ToLower(cmd)]
		if !ok {
			s.logger.Warn("unknown command", "command", cmd, "key", key)
			conn.Write([]byte(kverrors.ErrCommandNotFound.Error()))
			continue
		}

		// get the shard responsible for this data
		shard := s.manager.GetShardForKey(key)
		s.logger.Info("fetched shard", "shard", shard)
		command.Args = append(command.Args, msg[1:]...)

		// perform the operation on the shard
		result, err := shard.Execute(command)
		s.logger.Info("command executed", "result", result, "err", err)

		command.Args = []string{}
		if err != nil {
			conn.Write([]byte(err.Error() + "\n"))
			continue
		}

		// send response
		if result == nil {
			conn.Write([]byte("ok\n"))
			continue
		}

		conn.Write([]byte(result.Data.(string) + "\n"))
	}
}
