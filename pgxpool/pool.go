package pgxpool

import (
	"context"
	"time"
)

type ConnConfig struct {
	ConnectTimeout time.Duration
}

type Config struct {
	ConnConfig         ConnConfig
	HealthCheckPeriod  time.Duration
	MinConns           int
}

type conn struct {
}

func (c *conn) Close(ctx context.Context) error {
	return nil
}

type Pool struct {
	config *Config
}

func (p *Pool) connect(ctx context.Context) (*conn, error) {
	return &conn{}, nil
}

func (p *Pool) backgroundHealthCheck(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(p.config.HealthCheckPeriod):
			func() {
				connectCtx := ctx
				if p.config.ConnConfig.ConnectTimeout > 0 {
					var cancel context.CancelFunc
					connectCtx, cancel = context.WithTimeout(ctx, p.config.ConnConfig.ConnectTimeout)
					defer cancel()
				}

				conn, err := p.connect(connectCtx)
				if err != nil {
					return
				}
				conn.Close(ctx)
			}()
		}
	}
}