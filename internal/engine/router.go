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
	return &Router{
		shardCount: shardCount,
	}
}

// Route calculates the shard ID for a given symbol
// Uses FNV-1a hash for stable, deterministic routing
func (r *Router) Route(symbol string) int {
	h := fnv.New32a()
	h.Write([]byte(symbol))
	return int(h.Sum32()) % r.shardCount
}
