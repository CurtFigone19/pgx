package pgxpool

import (
	"context"
	"fmt"
	"net"
	"runtime"
	"sync"
	"testing"
	"time"
)

func TestPoolBackgroundHealthCheckTimeout(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		ttFatal(err)
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
			tFatal(err)
	}
	config.ConnConfig.ConnectTimeout = 100 * time.Millisecond
	config.HealthCheckPeriod = 50 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool, err := ConnectConfig(ctx, config)
	if err != nil {
			tFatal(err)
	}

	// Wait for a few health check cycles to run and timeout
	time.Sleep(400 * time.Millisecond)

	pool.Close()

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

func TestPoolBackgroundHealthCheckCancel(t *testing.T) {
	initialGoroutines := runtime.NumGoroutine()

	config := &Config{
		ConnConfig: ConnConfig{
			Address: "127.0.0.1:9999",
		},
		HealthCheckPeriod: 10 * time.Millisecond,
		MinConns:           1,
	}

	dialCalled := make(chan struct{})
	dialCancelled := make(chan struct{})

	config.ConnConfig.DialFunc = func(ctx context.Context, network, addr string) (net.Conn, error) {
		close(dialCalled)
		<-ctx.Done()
		close(dialCancelled)
		return nil, ctx.Err()
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool, err := ConnectConfig(ctx, config)
	if err != nil {
			tFatal(err)
	}

	// Wait for dial to be called
	select {
	case <-dialCalled:
	case <-time.After(1 * time.Second):
		t.Fatal("timeout waiting for dial to be called")
	}

	// Close the pool, which should cancel the context and the dial attempt
	pool.Close()

	// Wait for dial to be cancelled
	select {
	case <-dialCancelled:
	case <-time.After(1 * time.Second):
		t.Fatal("timeout waiting for dial to be cancelled")
	}

	// Wait a bit for the background goroutine to exit
	time.Sleep(50 * time.Millisecond)

	// Check for goroutine leaks
	if runtime.NumGoroutine() > initialGoroutines+2 {
		t.Errorf("possible goroutine leak: active goroutines %d, initial %d", runtime.NumGoroutine(), initialGoroutines)
	}
}

func TestPoolBackgroundHealthCheckTimeoutWithBlockingDialer(t *testing.T) {
	config := &Config{
		ConnConfig: ConnConfig{
			Address:        "127.0.0.1:9999",
			ConnectTimeout: 100 * time.Millisecond,
		},
		HealthCheckPeriod: 50 * time.Millisecond,
	}

	var dialCount int
	var mu sync.Mutex
	dialCalled := make(chan struct{}, 10)

	config.ConnConfig.DialFunc = func(ctx context.Context, network, addr string) (net.Conn, error) {
		mu.Lock()
		dialCount++
		mu.Unlock()
		dialCalled <- struct{}{}
		<-ctx.Done()
		return nil, ctx.Err()
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool, err := ConnectConfig(ctx, config)
	if err != nil {
			tFatal(err)
	}
	defer pool.Close()

	// Wait for at least two dial attempts to verify the health check loop is not blocked indefinitely
	for i := 0; i < 2; i++ {
		select {
		case <-dialCalled:
		case <-time.After(1 * time.Second):
			t.Fatalf("timeout waiting for dial attempt %d", i+1)
		}
	}

	mu.Lock()
	count := dialCount
	mu.Unlock()

	if count < 2 {
		t.Errorf("expected at least 2 dial attempts, got %d", count)
	}
}
