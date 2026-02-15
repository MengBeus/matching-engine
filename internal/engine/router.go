package engine

import (
	"hash/fnv"
)

// Router routes commands to shards based on symbol
type Router struct {
	shardCount int
}

// NewRouter creates a new router with the specified shard count
func NewRouter(shardCount int) *Router {
	if shardCount <= 0 {
		shardCount = 1
	}
	return &Router{
		shardCount: shardCount,
	}
}

// Route calculates the shard ID for a given symbol
// Uses FNV-1a hash for stable, deterministic routing
func (r *Router) Route(symbol string) int {
	if r.shardCount <= 0 {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(symbol))
	return int(h.Sum32()) % r.shardCount
}
