package engine

import (
	"fmt"
	"sync"
	"time"

	"matching-engine/internal/matching"
)

// IdempotencyKey represents the composite key for idempotency checking
type IdempotencyKey struct {
	AccountID      string
	Symbol         string
	CommandType    CommandType
	IdempotencyKey string
}

// String returns a string representation of the idempotency key
func (k IdempotencyKey) String() string {
	return fmt.Sprintf("%s:%s:%s:%s", k.AccountID, k.Symbol, k.CommandType, k.IdempotencyKey)
}

// IdempotencyRecord stores the cached result of a command execution
type IdempotencyRecord struct {
	PayloadHash string                  // Hash of the original payload
	Result      *matching.CommandResult // Cached execution result
	ErrorCode   ErrorCode               // Cached error code
	Err         error                   // Cached error
	ExpiresAt   time.Time               // Expiration time
}

// IdempotencyStore manages idempotency records
type IdempotencyStore struct {
	mu      sync.RWMutex
	records map[string]*IdempotencyRecord
	ttl     time.Duration
}

// NewIdempotencyStore creates a new idempotency store
func NewIdempotencyStore(ttl time.Duration) *IdempotencyStore {
	return &IdempotencyStore{
		records: make(map[string]*IdempotencyRecord),
		ttl:     ttl,
	}
}

// Check checks if a command is duplicate or conflict
// Returns:
// - (nil, nil) if not seen before (should execute)
// - (result, nil) if duplicate with same payload (return cached result)
// - (nil, error) if conflict with different payload
func (s *IdempotencyStore) Check(key IdempotencyKey, payloadHash string) (*CommandExecResult, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	keyStr := key.String()
	record, exists := s.records[keyStr]
	if !exists {
		// Not seen before, should execute
		return nil, nil
	}

	// Check if expired
	if time.Now().After(record.ExpiresAt) {
		// Expired, treat as not seen
		return nil, nil
	}

	// Check payload hash
	if record.PayloadHash != payloadHash {
		// Conflict: same idempotency key but different payload
		return nil, fmt.Errorf("idempotency key conflict: same key with different payload")
	}

	// Duplicate: return cached result
	return &CommandExecResult{
		Result:    record.Result,
		ErrorCode: record.ErrorCode,
		Err:       record.Err,
	}, nil
}

// Store stores the execution result for future idempotency checks
func (s *IdempotencyStore) Store(key IdempotencyKey, payloadHash string, result *CommandExecResult) {
	s.mu.Lock()
	defer s.mu.Unlock()

	keyStr := key.String()
	s.records[keyStr] = &IdempotencyRecord{
		PayloadHash: payloadHash,
		Result:      result.Result,
		ErrorCode:   result.ErrorCode,
		Err:         result.Err,
		ExpiresAt:   time.Now().Add(s.ttl),
	}
}

// Cleanup removes expired records
func (s *IdempotencyStore) Cleanup() {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	for key, record := range s.records {
		if now.After(record.ExpiresAt) {
			delete(s.records, key)
		}
	}
}

// Size returns the number of records in the store (for testing)
func (s *IdempotencyStore) Size() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.records)
}
