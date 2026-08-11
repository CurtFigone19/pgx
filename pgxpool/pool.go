func (p *Pool) backgroundHealthCheck(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(p.config.HealthCheckPeriod):
			// Create a context that respects both the pool context and the connect timeout
			connectCtx := ctx
			if p.config.ConnConfig.ConnectTimeout > 0 {
				var cancel context.CancelFunc
				connectCtx, cancel = context.WithTimeout(ctx, p.config.ConnConfig.ConnectTimeout)
				defer cancel()
			}

			conn, err := p.connect(connectCtx)
			if err != nil {
				continue
			}
			conn.Close(context.Background())
		}
	}
}