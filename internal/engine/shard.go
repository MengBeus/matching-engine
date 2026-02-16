package engine

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"matching-engine/internal/matching"
)

const defaultIdempotencyCleanupInterval = time.Minute
const defaultSnapshotInterval = 100 // Create snapshot every N events

// Shard represents a single shard that processes commands for specific symbols
type Shard struct {
	id            int
	cmdQueue      chan *commandRequest
	books         map[string]*matching.OrderBook
	idemStore     *IdempotencyStore
	eventStore    EventStore    // Optional: if nil, events are not persisted
	snapshotStore SnapshotStore // Optional: if nil, snapshots are not created

	// Snapshot tracking per symbol
	eventCounters    map[string]int64 // symbol -> event count since last snapshot
	snapshotInterval int64            // Number of events between snapshots

	submitMu sync.RWMutex
	stopped  bool
	wg       sync.WaitGroup
}

// commandRequest wraps a command with a response channel
type commandRequest struct {
	envelope *CommandEnvelope
	respChan chan *CommandExecResult
}

// NewShard creates a new shard
func NewShard(id int, queueSize int, idemTTL time.Duration) *Shard {
	return &Shard{
		id:               id,
		cmdQueue:         make(chan *commandRequest, queueSize),
		books:            make(map[string]*matching.OrderBook),
		idemStore:        NewIdempotencyStore(idemTTL),
		eventCounters:    make(map[string]int64),
		snapshotInterval: defaultSnapshotInterval,
	}
}

// SetEventStore sets the event store for persistence (optional)
func (s *Shard) SetEventStore(eventStore EventStore) {
	s.eventStore = eventStore
}

// SetSnapshotStore sets the snapshot store for persistence (optional)
func (s *Shard) SetSnapshotStore(snapshotStore SnapshotStore) {
	s.snapshotStore = snapshotStore
}

// SetSnapshotInterval sets the number of events between snapshots
func (s *Shard) SetSnapshotInterval(interval int64) {
	if interval > 0 {
		s.snapshotInterval = interval
	}
}

// Start starts the shard's event loop in a goroutine
func (s *Shard) Start() {
	s.wg.Add(1)
	go s.eventLoop()
}

// Stop gracefully stops the shard event loop.
func (s *Shard) Stop() {
	s.submitMu.Lock()
	if s.stopped {
		s.submitMu.Unlock()
		return
	}
	s.stopped = true
	close(s.cmdQueue)
	s.submitMu.Unlock()

	s.wg.Wait()
}

// Submit submits a command to the shard and waits for the result
func (s *Shard) Submit(envelope *CommandEnvelope) *CommandExecResult {
	if envelope == nil {
		return &CommandExecResult{
			Result:    nil,
			ErrorCode: ErrorCodeInvalidArgument,
			Err:       fmt.Errorf("command envelope is nil"),
		}
	}

	respChan := make(chan *CommandExecResult, 1)
	req := &commandRequest{
		envelope: envelope,
		respChan: respChan,
	}

	s.submitMu.RLock()
	if s.stopped {
		s.submitMu.RUnlock()
		return &CommandExecResult{
			Result:    nil,
			ErrorCode: ErrorCodeInvalidArgument,
			Err:       fmt.Errorf("shard is stopped"),
		}
	}
	s.cmdQueue <- req
	s.submitMu.RUnlock()
	return <-respChan
}

// eventLoop is the main event loop that processes commands serially
func (s *Shard) eventLoop() {
	defer s.wg.Done()

	ticker := time.NewTicker(defaultIdempotencyCleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case req, ok := <-s.cmdQueue:
			if !ok {
				return
			}
			if req == nil {
				continue
			}
			result := s.processCommand(req.envelope)
			req.respChan <- result
		case <-ticker.C:
			s.idemStore.Cleanup()
		}
	}
}

// processCommand processes a single command
func (s *Shard) processCommand(envelope *CommandEnvelope) *CommandExecResult {
	if envelope == nil {
		return &CommandExecResult{
			Result:    nil,
			ErrorCode: ErrorCodeInvalidArgument,
			Err:       fmt.Errorf("command envelope is nil"),
		}
	}

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
	case CommandTypeQuery:
		result = s.executeQuery(envelope)
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

	// Persist events if event store is configured
	if s.eventStore != nil && len(matchResult.Events) > 0 {
		ctx := context.Background()
		for _, event := range matchResult.Events {
			if err := s.eventStore.Append(ctx, envelope.Symbol, event); err != nil {
				// Event persistence failed - this is a critical error
				// In production, we should have proper error handling and rollback
				// For MVP, we log the error but continue
				// TODO: Implement proper transaction handling
				return &CommandExecResult{
					Result:    nil,
					ErrorCode: ErrorCodeInternalError,
					Err:       fmt.Errorf("failed to persist event: %w", err),
				}
			}
		}
		lastSeq := matchResult.Events[len(matchResult.Events)-1].Sequence()
		s.checkAndCreateSnapshot(envelope.Symbol, len(matchResult.Events), lastSeq)
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

	// Persist events if event store is configured
	if s.eventStore != nil && len(matchResult.Events) > 0 {
		ctx := context.Background()
		for _, event := range matchResult.Events {
			if err := s.eventStore.Append(ctx, envelope.Symbol, event); err != nil {
				return &CommandExecResult{
					Result:    nil,
					ErrorCode: ErrorCodeInternalError,
					Err:       fmt.Errorf("failed to persist event: %w", err),
				}
			}
		}
		lastSeq := matchResult.Events[len(matchResult.Events)-1].Sequence()
		s.checkAndCreateSnapshot(envelope.Symbol, len(matchResult.Events), lastSeq)
	}

	return &CommandExecResult{
		Result:    matchResult,
		ErrorCode: ErrorCodeNone,
		Err:       nil,
	}
}

// executeQuery executes a query order command
func (s *Shard) executeQuery(envelope *CommandEnvelope) *CommandExecResult {
	// Extract payload
	req, ok := envelope.Payload.(*matching.QueryOrderRequest)
	if !ok {
		return &CommandExecResult{
			Result:    nil,
			ErrorCode: ErrorCodeInvalidArgument,
			Err:       fmt.Errorf("invalid payload type for QUERY command"),
		}
	}

	// Validate request
	if err := req.Validate(); err != nil {
		return &CommandExecResult{
			Result:    nil,
			ErrorCode: ErrorCodeInvalidArgument,
			Err:       err,
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

	// Query order snapshot
	snapshot, err := book.GetOrderSnapshot(req.OrderID)
	if err != nil {
		return &CommandExecResult{
			Result:    nil,
			ErrorCode: s.mapErrorCode(err),
			Err:       err,
		}
	}

	// Verify account authorization
	if snapshot.AccountID != req.AccountID {
		return &CommandExecResult{
			Result:    nil,
			ErrorCode: ErrorCodeUnauthorized,
			Err:       fmt.Errorf("unauthorized: order belongs to different account"),
		}
	}

	// Return snapshot as result
	// Note: We wrap the snapshot in a CommandResult-like structure
	// The API layer will extract the snapshot from the result
	return &CommandExecResult{
		Result:    snapshot,
		ErrorCode: ErrorCodeNone,
		Err:       nil,
	}
}

// mapErrorCode maps matching engine errors to error codes
func (s *Shard) mapErrorCode(err error) ErrorCode {
	errMsg := strings.ToLower(err.Error())

	// Check for specific error patterns
	if strings.Contains(errMsg, "not found") {
		return ErrorCodeOrderNotFound
	}
	if strings.Contains(errMsg, "unauthorized") || strings.Contains(errMsg, "different account") {
		return ErrorCodeUnauthorized
	}
	if strings.Contains(errMsg, "already filled") || strings.Contains(errMsg, "filled order") {
		return ErrorCodeOrderAlreadyFilled
	}
	if strings.Contains(errMsg, "already canceled") {
		return ErrorCodeOrderAlreadyCanceled
	}

	// Default to invalid argument
	return ErrorCodeInvalidArgument
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

// LoadSnapshot restores a symbol's orderbook from snapshot state.
func (s *Shard) LoadSnapshot(symbol string, state *matching.OrderBookState, lastSequence int64) error {
	book, exists := s.books[symbol]
	if !exists {
		book = matching.NewOrderBook(symbol)
		s.books[symbol] = book
	}

	if state != nil {
		if err := book.ImportState(state); err != nil {
			return fmt.Errorf("failed to import orderbook state: %w", err)
		}
	}

	// Keep sequence monotonic even when loading older/legacy snapshot payload.
	if book.GetEventSequence() < lastSequence {
		book.SetEventSequence(lastSequence)
	}

	return nil
}

// ReplayEvents replays a batch of events to rebuild orderbook state
// This is used during recovery to restore state from event log
func (s *Shard) ReplayEvents(symbol string, events []matching.Event) error {
	if len(events) == 0 {
		return nil
	}

	// Get or create order book
	book, exists := s.books[symbol]
	if !exists {
		book = matching.NewOrderBook(symbol)
		s.books[symbol] = book
	}

	// Track the maximum sequence number
	var maxSeq int64

	// Replay each event
	for _, event := range events {
		if event.Sequence() > maxSeq {
			maxSeq = event.Sequence()
		}

		// Apply event based on type
		switch e := event.(type) {
		case *matching.OrderAcceptedEvent:
			if err := s.replayOrderAccepted(book, e); err != nil {
				return fmt.Errorf("failed to replay OrderAccepted(seq=%d): %w", e.Sequence(), err)
			}
		case *matching.OrderMatchedEvent:
			// OrderMatched is derived from OrderAccepted replay via deterministic matching.
			// We still advance maxSeq to keep sequence monotonic.
			continue
		case *matching.OrderCanceledEvent:
			if err := s.replayOrderCanceled(book, e); err != nil {
				return fmt.Errorf("failed to replay OrderCanceled(seq=%d): %w", e.Sequence(), err)
			}
		default:
			return fmt.Errorf("unknown event type: %T", event)
		}
	}

	// Set the orderbook's event sequence to the maximum sequence from replayed events
	// This ensures the next event will have the correct sequence number
	book.SetEventSequence(maxSeq)

	return nil
}

// replayOrderAccepted replays an OrderAccepted event
func (s *Shard) replayOrderAccepted(book *matching.OrderBook, event *matching.OrderAcceptedEvent) error {
	// Reconstruct the place order request
	req := &matching.PlaceOrderRequest{
		OrderID:       event.OrderID,
		ClientOrderID: event.ClientOrderID,
		AccountID:     event.AccountID,
		Symbol:        event.Symbol(),
		Side:          event.Side,
		PriceInt:      event.Price,
		QuantityInt:   event.Quantity,
	}

	// Execute place order (this will generate new events, but we ignore them during replay)
	_, err := book.PlaceLimit(req)
	return err
}

// replayOrderCanceled replays an OrderCanceled event
func (s *Shard) replayOrderCanceled(book *matching.OrderBook, event *matching.OrderCanceledEvent) error {
	// Reconstruct the cancel order request
	req := &matching.CancelOrderRequest{
		OrderID:   event.OrderID,
		AccountID: event.AccountID,
		Symbol:    event.Symbol(),
	}

	// Execute cancel order
	_, err := book.Cancel(req)
	if err != nil {
		// During replay, order might already be filled/canceled
		// This is expected, so we can ignore certain errors
		errMsg := strings.ToLower(err.Error())
		if strings.Contains(errMsg, "not found") ||
			strings.Contains(errMsg, "already") {
			return nil
		}
		return err
	}
	return nil
}

// checkAndCreateSnapshot checks if snapshot should be created and creates it.
func (s *Shard) checkAndCreateSnapshot(symbol string, persistedEvents int, lastPersistedSeq int64) {
	if s.snapshotStore == nil {
		return
	}
	if persistedEvents <= 0 {
		return
	}

	// Increment event counter.
	s.eventCounters[symbol] += int64(persistedEvents)

	// Check if we should create a snapshot
	if s.eventCounters[symbol] >= s.snapshotInterval {
		s.createSnapshot(symbol, lastPersistedSeq)
		s.eventCounters[symbol] = s.eventCounters[symbol] % s.snapshotInterval
	}
}

// createSnapshot creates a snapshot for a symbol
func (s *Shard) createSnapshot(symbol string, lastPersistedSeq int64) {
	book, exists := s.books[symbol]
	if !exists {
		return
	}

	state := book.ExportState()
	if state.EventSeq < lastPersistedSeq {
		state.EventSeq = lastPersistedSeq
	}

	snapshot := &Snapshot{
		Version:      1,
		Symbol:       symbol,
		LastSequence: state.EventSeq,
		CapturedAt:   time.Now(),
		Orderbook:    state,
	}

	ctx := context.Background()
	if err := s.snapshotStore.Save(ctx, snapshot); err != nil {
		// Log error but don't fail the command
		// In production, should have proper error handling and monitoring
		fmt.Printf("Warning: failed to create snapshot for %s: %v\n", symbol, err)
	}
}

// Snapshot represents a simplified snapshot structure
type Snapshot struct {
	Version      int                      `json:"version"`
	Symbol       string                   `json:"symbol"`
	LastSequence int64                    `json:"last_sequence"`
	CapturedAt   time.Time                `json:"captured_at"`
	Orderbook    *matching.OrderBookState `json:"orderbook,omitempty"`
}
