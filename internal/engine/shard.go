package engine

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"time"

	"matching-engine/internal/matching"
)

// Shard represents a single shard that processes commands for specific symbols
type Shard struct {
	id        int
	cmdQueue  chan *commandRequest
	books     map[string]*matching.OrderBook
	idemStore *IdempotencyStore
}

// commandRequest wraps a command with a response channel
type commandRequest struct {
	envelope *CommandEnvelope
	respChan chan *CommandExecResult
}

// NewShard creates a new shard
func NewShard(id int, queueSize int, idemTTL time.Duration) *Shard {
	return &Shard{
		id:        id,
		cmdQueue:  make(chan *commandRequest, queueSize),
		books:     make(map[string]*matching.OrderBook),
		idemStore: NewIdempotencyStore(idemTTL),
	}
}

// Start starts the shard's event loop in a goroutine
func (s *Shard) Start() {
	go s.eventLoop()
}

// Submit submits a command to the shard and waits for the result
func (s *Shard) Submit(envelope *CommandEnvelope) *CommandExecResult {
	respChan := make(chan *CommandExecResult, 1)
	req := &commandRequest{
		envelope: envelope,
		respChan: respChan,
	}

	s.cmdQueue <- req
	return <-respChan
}

// eventLoop is the main event loop that processes commands serially
func (s *Shard) eventLoop() {
	for req := range s.cmdQueue {
		result := s.processCommand(req.envelope)
		req.respChan <- result
	}
}

// processCommand processes a single command
func (s *Shard) processCommand(envelope *CommandEnvelope) *CommandExecResult {
	// Build idempotency key
	idemKey := IdempotencyKey{
		AccountID:      envelope.AccountID,
		Symbol:         envelope.Symbol,
		CommandType:    envelope.CommandType,
		IdempotencyKey: envelope.IdempotencyKey,
	}

	// Check idempotency
	cachedResult, err := s.idemStore.Check(idemKey, envelope.PayloadHash)
	if err != nil {
		// Conflict: same idempotency key with different payload
		return &CommandExecResult{
			Result:    nil,
			ErrorCode: ErrorCodeDuplicateRequest,
			Err:       err,
		}
	}
	if cachedResult != nil {
		// Duplicate: return cached result
		return cachedResult
	}

	// Not seen before, execute command
	var result *CommandExecResult

	switch envelope.CommandType {
	case CommandTypePlace:
		result = s.executePlace(envelope)
	case CommandTypeCancel:
		result = s.executeCancel(envelope)
	default:
		result = &CommandExecResult{
			Result:    nil,
			ErrorCode: ErrorCodeInvalidArgument,
			Err:       fmt.Errorf("unknown command type: %s", envelope.CommandType),
		}
	}

	// Store result in idempotency cache
	s.idemStore.Store(idemKey, envelope.PayloadHash, result)

	return result
}

// executePlace executes a place order command
func (s *Shard) executePlace(envelope *CommandEnvelope) *CommandExecResult {
	// Extract payload
	req, ok := envelope.Payload.(*matching.PlaceOrderRequest)
	if !ok {
		return &CommandExecResult{
			Result:    nil,
			ErrorCode: ErrorCodeInvalidArgument,
			Err:       fmt.Errorf("invalid payload type for PLACE command"),
		}
	}

	// Get or create order book for symbol
	book, exists := s.books[envelope.Symbol]
	if !exists {
		book = matching.NewOrderBook(envelope.Symbol)
		s.books[envelope.Symbol] = book
	}

	// Execute place order
	matchResult, err := book.PlaceLimit(req)
	if err != nil {
		return &CommandExecResult{
			Result:    nil,
			ErrorCode: s.mapErrorCode(err),
			Err:       err,
		}
	}

	return &CommandExecResult{
		Result:    matchResult,
		ErrorCode: ErrorCodeNone,
		Err:       nil,
	}
}

// executeCancel executes a cancel order command
func (s *Shard) executeCancel(envelope *CommandEnvelope) *CommandExecResult {
	// Extract payload
	req, ok := envelope.Payload.(*matching.CancelOrderRequest)
	if !ok {
		return &CommandExecResult{
			Result:    nil,
			ErrorCode: ErrorCodeInvalidArgument,
			Err:       fmt.Errorf("invalid payload type for CANCEL command"),
		}
	}

	// Get order book for symbol
	book, exists := s.books[envelope.Symbol]
	if !exists {
		return &CommandExecResult{
			Result:    nil,
			ErrorCode: ErrorCodeOrderNotFound,
			Err:       fmt.Errorf("order book not found for symbol: %s", envelope.Symbol),
		}
	}

	// Execute cancel order
	matchResult, err := book.Cancel(req)
	if err != nil {
		return &CommandExecResult{
			Result:    nil,
			ErrorCode: s.mapErrorCode(err),
			Err:       err,
		}
	}

	return &CommandExecResult{
		Result:    matchResult,
		ErrorCode: ErrorCodeNone,
		Err:       nil,
	}
}

// mapErrorCode maps matching engine errors to error codes
func (s *Shard) mapErrorCode(err error) ErrorCode {
	errMsg := err.Error()

	// Check for specific error patterns
	if contains(errMsg, "not found") {
		return ErrorCodeOrderNotFound
	}
	if contains(errMsg, "unauthorized") || contains(errMsg, "different account") {
		return ErrorCodeUnauthorized
	}
	if contains(errMsg, "already filled") || contains(errMsg, "filled order") {
		return ErrorCodeOrderAlreadyFilled
	}
	if contains(errMsg, "already canceled") {
		return ErrorCodeOrderAlreadyCanceled
	}

	// Default to invalid argument
	return ErrorCodeInvalidArgument
}

// contains checks if a string contains a substring (case-insensitive helper)
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > len(substr) &&
		(s[:len(substr)] == substr || s[len(s)-len(substr):] == substr ||
		findSubstring(s, substr)))
}

func findSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// ComputePayloadHash computes SHA256 hash of the payload
func ComputePayloadHash(payload any) (string, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	hash := sha256.Sum256(data)
	return fmt.Sprintf("%x", hash), nil
}
