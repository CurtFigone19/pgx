package pgxpool

import (
	"context"
	"time"
)

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
				conn.Close(context.Background())
			}()
		}
	}
}