// ... existing code ...

func (p *Pool) backgroundHealthCheck(ctx context.Context) {
	// ... existing code ...
	// Ensure connection establishment respects the pool's context
	conn, err := p.connect(ctx)
	// ... existing code ...
}

func (p *Pool) connect(ctx context.Context) (*pgx.Conn, error) {
	// Use the provided context (which is derived from the pool's context) for dialing
	return pgx.ConnectConfig(ctx, p.config.ConnConfig)
}

// ... existing code ...