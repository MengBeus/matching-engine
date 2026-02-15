package engine

import (
	"time"
)

// Engine manages multiple shards and routes commands to them
type Engine struct {
	router *Router
	shards []*Shard
}

// EngineConfig holds configuration for the engine
type EngineConfig struct {
	ShardCount    int           // Number of shards (default: 8)
	QueueSize     int           // Command queue size per shard (default: 1000)
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
	if config == nil {
		config = DefaultEngineConfig()
	}

	// Create router
	router := NewRouter(config.ShardCount)

	// Create shards
	shards := make([]*Shard, config.ShardCount)
	for i := 0; i < config.ShardCount; i++ {
		shards[i] = NewShard(i, config.QueueSize, config.IdempotencyTTL)
		shards[i].Start()
	}

	return &Engine{
		router: router,
		shards: shards,
	}
}

// Submit submits a command to the appropriate shard and returns the result
func (e *Engine) Submit(envelope *CommandEnvelope) *CommandExecResult {
	// Route to shard
	shardID := e.router.Route(envelope.Symbol)
	shard := e.shards[shardID]

	// Submit to shard
	return shard.Submit(envelope)
}

// GetShardID returns the shard ID for a given symbol (for testing)
func (e *Engine) GetShardID(symbol string) int {
	return e.router.Route(symbol)
}
