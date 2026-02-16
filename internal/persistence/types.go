package persistence

import (
	"context"
	"time"

	"matching-engine/internal/matching"
)

// EventRecord represents a persisted event record
type EventRecord struct {
	Version     int       `json:"version"`
	Symbol      string    `json:"symbol"`
	Sequence    int64     `json:"sequence"`
	Type        string    `json:"type"`
	OccurredAt  time.Time `json:"occurred_at"`
	Payload     any       `json:"payload"`
}

// Snapshot represents a point-in-time snapshot of engine state
type Snapshot struct {
	Version          int                    `json:"version"`
	Symbol           string                 `json:"symbol"`
	LastSequence     int64                  `json:"last_sequence"`
	CapturedAt       time.Time              `json:"captured_at"`
	Orderbook        any                    `json:"orderbook"`
	ClosedOrders     map[string]any         `json:"closed_orders"`
	AccountBalances  map[string]any         `json:"account_balances"`
	IdempotencyState map[string]any         `json:"idempotency_state,omitempty"`
}

// EventStore defines the interface for event log persistence
type EventStore interface {
	// Append appends an event to the log for a specific symbol
	Append(ctx context.Context, symbol string, event matching.Event) error

	// ReadFrom reads events from a specific sequence number (inclusive)
	ReadFrom(ctx context.Context, symbol string, fromSeq int64) ([]matching.Event, error)

	// GetLastSequence returns the last sequence number for a symbol
	GetLastSequence(ctx context.Context, symbol string) (int64, error)

	// ListSymbols lists all symbols that have event logs
	ListSymbols(ctx context.Context) ([]string, error)

	// Close closes the event store
	Close() error
}

// SnapshotStore defines the interface for snapshot persistence
type SnapshotStore interface {
	// Save saves a snapshot for a specific symbol
	// Accepts any snapshot type that can be marshaled to JSON
	Save(ctx context.Context, snapshot any) error

	// Load loads the latest snapshot for a specific symbol
	Load(ctx context.Context, symbol string) (*Snapshot, error)

	// ListSnapshots lists all available snapshots for a symbol (sorted by sequence desc)
	ListSnapshots(ctx context.Context, symbol string) ([]SnapshotMetadata, error)

	// Close closes the snapshot store
	Close() error
}

// SnapshotMetadata represents snapshot metadata
type SnapshotMetadata struct {
	Symbol       string    `json:"symbol"`
	LastSequence int64     `json:"last_sequence"`
	CapturedAt   time.Time `json:"captured_at"`
	FilePath     string    `json:"file_path"`
}

// RecoveryService defines the interface for recovery operations
type RecoveryService interface {
	// Recover recovers engine state for a specific symbol
	// Returns the recovered snapshot and events to replay
	Recover(ctx context.Context, symbol string) (*Snapshot, []matching.Event, error)

	// ValidateSequence validates that event sequences are continuous
	ValidateSequence(events []matching.Event) error
}
