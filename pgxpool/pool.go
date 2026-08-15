package pgxpool

import (
	"context"
	"net"
	"net/url"
	"time"
)

type ConnConfig struct {
	ConnectTimeout time.Duration
	Address        string
}

type Config struct {
	ConnConfig         ConnConfig
	HealthCheckPeriod  time.Duration
	MinConns           int
}

type conn struct {
	netConn net.Conn
}

func (c *conn) Close(ctx context.Context) error {
	if c.netConn != nil {
		return c.netConn.Close()
	}
	return nil
}

type Pool struct {
	config *Config
	cancel context.CancelFunc
}

func ParseConfig(connString string) (*Config, error) {
	u, err := url.Parse(connString)
	if err != nil {
		return nil, err
	}
	return &Config{
		ConnConfig: ConnConfig{
			Address: u.Host,
		},
	}, nil
}

func ConnectConfig(ctx context.Context, config *Config) (*Pool, error) {
	ctx, cancel := context.WithCancel(ctx)
	p := &Pool{
		config: config,
		cancel: cancel,
	}
	go p.backgroundHealthCheck(ctx)
	return p, nil
}

func (p *Pool) Close() {
	if p.cancel != nil {
		p.cancel()
	}
}

func (p *Pool) connect(ctx context.Context) (*conn, error) {
	var dialer net.Dialer
	netConn, err := dialer.DialContext(ctx, "tcp", p.config.ConnConfig.Address)
	if err != nil {
		return nil, err
	}
	return &conn{netConn: netConn}, nil
}

func (p *Pool) backgroundHealthCheck(ctx context.Context) {
	if p.config.HealthCheckPeriod <= 0 {
		return
	}
	ticker := time.NewTicker(p.config.HealthCheckPeriod)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			timeout := p.config.ConnConfig.ConnectTimeout
			if timeout <= 0 {
				timeout = 5 * time.Second
			}
			connectCtx, cancel := context.WithTimeout(ctx, timeout)
			conn, err := p.connect(connectCtx)
			cancel()
			if err == nil {
				// Close with the pool's lifecycle context, not context.Background,
				// so a health-check connection never blocks past pool shutdown.
				// See issue: respect context deadline during health-check close.
				conn.Close(ctx)
			}
		}
	}
}