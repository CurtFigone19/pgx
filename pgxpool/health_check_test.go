package pgxpool

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"
)

func TestPoolBackgroundHealthCheckTimeout(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	var conns []net.Conn
	var mu sync.Mutex
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			conns = append(conns, conn)
			mu.Unlock()
		}
	}()
	defer func() {
		mu.Lock()
		for _, conn := range conns {
			conn.Close()
		}
		mu.Unlock()
	}()

	config, err := ParseConfig(fmt.Sprintf("postgres://none:none@%s/none", ln.Addr().String()))
	if err != nil {
		t.Fatal(err)
	}
	config.ConnConfig.ConnectTimeout = 100 * time.Millisecond
	config.HealthCheckPeriod = 50 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool, err := ConnectConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	// Wait for a few health check cycles to run and timeout
	time.Sleep(400 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	if len(conns) == 0 {
		t.Fatal("expected at least one connection attempt")
	}

	isClosed := func(conn net.Conn) bool {
		conn.SetReadDeadline(time.Now().Add(10 * time.Millisecond))
		one := make([]byte, 1)
		_, err := conn.Read(one)
		if err == nil {
			return false
		}
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			return false
		}
		return true
	}

	for i, conn := range conns {
		if !isClosed(conn) {
			t.Errorf("connection %d is still open, expected it to be closed by ConnectTimeout", i)
		}
	}
}