package server

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"kvshard/internal/shards"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

func testServer(t *testing.T) (*server, func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	manager, err := shards.NewManagerAt(ctx, 4, t.TempDir(), logger)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { manager.Run(); close(done) }()
	cleanup := func() {
		cancel()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("manager did not shut down")
		}
	}
	return &server{manager: manager, ctx: ctx, logger: logger}, cleanup
}

func openTestConnection(t *testing.T, s *server) (net.Conn, *bufio.Reader) {
	t.Helper()
	client, peer := net.Pipe()
	go s.handle(peer)
	t.Cleanup(func() { _ = client.Close() })
	return client, bufio.NewReader(client)
}

func exchange(t *testing.T, conn net.Conn, reader *bufio.Reader, command string) string {
	t.Helper()
	if _, err := io.WriteString(conn, command+"\n"); err != nil {
		t.Fatal(err)
	}
	response, err := reader.ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	return strings.TrimSpace(response)
}

func TestHandleCommandScenariosAndEdges(t *testing.T) {
	s, cleanup := testServer(t)
	defer cleanup()
	conn, reader := openTestConnection(t, s)

	tests := []struct {
		command string
		want    string
	}{
		{command: "unknown key", want: "unknown command"},
		{command: "get", want: "invalid arguments"},
		{command: "put key", want: "invalid arguments"},
		{command: "put key value 1 extra", want: "invalid arguments"},
		{command: "  PuT   key   value  ", want: "ok"},
		{command: "GET key", want: "value"},
		{command: "exists key", want: "true"},
		{command: "del key", want: "ok"},
		{command: "exists key", want: "false"},
		{command: "get key", want: "key not found"},
	}
	for _, test := range tests {
		t.Run(test.command, func(t *testing.T) {
			if got := exchange(t, conn, reader, test.command); got != test.want {
				t.Fatalf("response = %q, want %q", got, test.want)
			}
		})
	}
}

func TestHandleConcurrentClientsKeepArgumentsIsolated(t *testing.T) {
	s, cleanup := testServer(t)
	defer cleanup()
	const clients = 24
	var wg sync.WaitGroup
	errs := make(chan error, clients)
	for i := range clients {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, peer := net.Pipe()
			defer conn.Close()
			go s.handle(peer)
			reader := bufio.NewReader(conn)
			key := "key-" + string(rune('a'+i))
			value := "value-" + string(rune('a'+i))
			if _, err := io.WriteString(conn, "put "+key+" "+value+"\n"); err != nil {
				errs <- err
				return
			}
			got, err := reader.ReadString('\n')
			if err != nil || strings.TrimSpace(got) != "ok" {
				errs <- fmt.Errorf("put response = %q, error = %v", got, err)
				return
			}
			if _, err := io.WriteString(conn, "get "+key+"\n"); err != nil {
				errs <- err
				return
			}
			got, err = reader.ReadString('\n')
			if err != nil || strings.TrimSpace(got) != value {
				errs <- fmt.Errorf("get response = %q, want %q, error = %v", got, value, err)
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}
