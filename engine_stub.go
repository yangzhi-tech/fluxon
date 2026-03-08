package fluxon

import "context"

// runEngine is implemented in internal/engine and wired here.
// This stub is replaced once the engine package is implemented.
func runEngine(ctx context.Context, cfg Config, h Handler) error {
	<-ctx.Done()
	return ctx.Err()
}
