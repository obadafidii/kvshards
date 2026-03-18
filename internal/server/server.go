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
	manager    *shards.Manager
	ctx        context.Context
	signalChan chan os.Signal

	logger *slog.Logger
}

func Start(ctx context.Context, logger *slog.Logger, numOfShards int) {
	ln, err := net.Listen("tcp", ":9500")
	if err != nil {
		panic(err)
	}

	svr := &server{
		ctx:        ctx,
		logger:     logger.WithGroup("server"),
		manager:    shards.NewManager(numOfShards, logger),
		signalChan: make(chan os.Signal, 1),
	}

	for {
		select {
		case <-svr.ctx.Done():
			svr.logger.Info("context cancelled: shutting down server")
			return
		case <-svr.signalChan:
			svr.logger.Info("os signal recieved: shutting down server")
			return
		default:
			conn, aerr := ln.Accept()
			if aerr != nil {
				conn.Write([]byte(err.Error()))
			}

			go svr.handle(conn)
		}

	}
}

func (s *server) handle(conn net.Conn) {
	defer conn.Close()

	scanner := bufio.NewScanner(conn)

	for scanner.Scan() {
		line := scanner.Text()

		// continue if empty line is sent from client
		if line == "" {
			continue
		}

		// extract the command and arguements
		input := strings.Split(line, " ")

		// command is at the first index
		// ["put|get|del|exists", ...args]
		cmd := input[0]
		command, ok := commands.Registry[strings.ToLower(cmd)]
		if !ok {
			s.logger.Warn("unknown command", "command", cmd)
			conn.Write([]byte(kverrors.ErrUnknownCommand.Error() + "\n"))
			continue
		}

		// extract and validate the command arguements
		command.Args = append(command.Args, input[1:]...)
		if len(command.Args) != command.MinArgs || len(command.Args) != command.MaxArgs {
			s.logger.Info("invalid args provided", "min-args-expected", command.MinArgs, "max-args-expected", command.MaxArgs)
			conn.Write([]byte(kverrors.ErrInvalidArguments.Error() + "\n"))
			continue
		}

		// get the shard responsible for this key
		key := command.Args[1] // key
		shard := s.manager.GetShard(key)

		// perform the operation on the shard
		result, err := shard.Execute(command)

		clear(command.Args)

		if err != nil {
			s.logger.Info("error executing command", "error", err)
			conn.Write([]byte(err.Error() + "\n"))
			continue
		}

		// send response
		if result == nil {
			s.logger.Info("command does not return a result", "command", command.Name)
			conn.Write([]byte("ok\n"))
			continue
		}

		conn.Write([]byte(result.Data.(string) + "\n"))
	}
}
