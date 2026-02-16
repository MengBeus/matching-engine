package engine

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"matching-engine/internal/matching"
)

// EventStore defines the minimal interface needed for event persistence
type EventStore interface {
	Append(ctx context.Context, symbol string, event matching.Event) error
}

// SnapshotStore defines the minimal interface needed for snapshot persistence
type SnapshotStore interface {
	Save(ctx context.Context, snapshot any) error
}

// Engine manages multiple shards and routes commands to them
type Engine struct {
	router    *Router
	shards    []*Shard
	closed    atomic.Bool
	closeOnce sync.Once
}

// EngineConfig holds configuration for the engine
type EngineConfig struct {
	ShardCount     int           // Number of shards (default: 8)
	QueueSize      int           // Command queue size per shard (default: 1000)
	IdempotencyTTL time.Duration // Idempotency record TTL (default: 24h)
}

// DefaultEngineConfig returns default engine configuration
func DefaultEngineConfig() *EngineConfig {
	return &EngineConfig{
		ShardCount:     8,
		QueueSize:      1000,
		IdempotencyTTL: 24 * time.Hour,
	}
}

// NewEngine creates a new engine with the given configuration
func NewEngine(config *EngineConfig) *Engine {
	cfg := normalizeEngineConfig(config)

	// Create router
	router := NewRouter(cfg.ShardCount)

	// Create shards
	shards := make([]*Shard, cfg.ShardCount)
	for i := 0; i < cfg.ShardCount; i++ {
		shards[i] = NewShard(i, cfg.QueueSize, cfg.IdempotencyTTL)
		shards[i].Start()
	}

	return &Engine{
		router: router,
		shards: shards,
	}
}

// Submit submits a command to the appropriate shard and returns the result
func (e *Engine) Submit(envelope *CommandEnvelope) *CommandExecResult {
	if envelope == nil {
		return &CommandExecResult{
			ErrorCode: ErrorCodeInvalidArgument,
			Err:       fmt.Errorf("command envelope is nil"),
		}
	}
	if e.closed.Load() {
		return &CommandExecResult{
			ErrorCode: ErrorCodeInvalidArgument,
			Err:       fmt.Errorf("engine is closed"),
		}
	}

	// Route to shard
	shardID := e.router.Route(envelope.Symbol)
	if shardID < 0 || shardID >= len(e.shards) {
		return &CommandExecResult{
			ErrorCode: ErrorCodeInvalidArgument,
			Err:       fmt.Errorf("invalid shard id: %d", shardID),
		}
	}
	shard := e.shards[shardID]

	// Submit to shard
	return shard.Submit(envelope)
}

// GetShardID returns the shard ID for a given symbol (for testing)
func (e *Engine) GetShardID(symbol string) int {
	return e.router.Route(symbol)
}

// Close gracefully stops all shards.
func (e *Engine) Close() {
	e.closeOnce.Do(func() {
		e.closed.Store(true)
		for _, shard := range e.shards {
			shard.Stop()
		}
	})
}

// RecoverSymbol recovers a single symbol by replaying events
// This should be called before the engine starts processing new commands
func (e *Engine) RecoverSymbol(symbol string, events []matching.Event) error {
	if e.closed.Load() {
		return fmt.Errorf("engine is closed")
	}

	// Route to shard
	shardID := e.router.Route(symbol)
	if shardID < 0 || shardID >= len(e.shards) {
		return fmt.Errorf("invalid shard id: %d", shardID)
	}
	shard := e.shards[shardID]

	// Replay events in the shard
	return shard.ReplayEvents(symbol, events)
}

// SetEventStore sets the event store for all shards
// This should be called before the engine starts processing commands
func (e *Engine) SetEventStore(eventStore EventStore) {
	for _, shard := range e.shards {
		shard.SetEventStore(eventStore)
	}
}

// SetSnapshotStore sets the snapshot store for all shards
// This should be called before the engine starts processing commands
func (e *Engine) SetSnapshotStore(snapshotStore SnapshotStore) {
	for _, shard := range e.shards {
		shard.SetSnapshotStore(snapshotStore)
	}
}

func normalizeEngineConfig(config *EngineConfig) EngineConfig {
	defaults := DefaultEngineConfig()
	if config == nil {
		return *defaults
	}

	normalized := *config
	if normalized.ShardCount <= 0 {
		normalized.ShardCount = defaults.ShardCount
	}
	if normalized.QueueSize < 0 {
		normalized.QueueSize = defaults.QueueSize
	}
	if normalized.IdempotencyTTL <= 0 {
		normalized.IdempotencyTTL = defaults.IdempotencyTTL
	}

	return normalized
}
