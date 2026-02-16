package persistence

import (
	"context"
	"fmt"

	"matching-engine/internal/matching"
)

// FileRecoveryService implements RecoveryService
type FileRecoveryService struct {
	eventStore    EventStore
	snapshotStore SnapshotStore
}

// NewFileRecoveryService creates a new recovery service
func NewFileRecoveryService(eventStore EventStore, snapshotStore SnapshotStore) *FileRecoveryService {
	return &FileRecoveryService{
		eventStore:    eventStore,
		snapshotStore: snapshotStore,
	}
}

// Recover recovers engine state for a specific symbol
// Returns the recovered snapshot and events to replay
func (s *FileRecoveryService) Recover(ctx context.Context, symbol string) (*Snapshot, []matching.Event, error) {
	// Step 1: Load the latest snapshot
	snapshot, err := s.snapshotStore.Load(ctx, symbol)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to load snapshot: %w", err)
	}

	var fromSeq int64 = 1
	if snapshot != nil {
		// Start from the next sequence after snapshot
		fromSeq = snapshot.LastSequence + 1
	}

	// Step 2: Read events from last_sequence + 1
	events, err := s.eventStore.ReadFrom(ctx, symbol, fromSeq)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read events: %w", err)
	}

	// Step 3: Validate sequence continuity
	if err := s.ValidateSequence(events); err != nil {
		return nil, nil, fmt.Errorf("sequence validation failed: %w", err)
	}

	return snapshot, events, nil
}

// ValidateSequence validates that event sequences are continuous
func (s *FileRecoveryService) ValidateSequence(events []matching.Event) error {
	if len(events) == 0 {
		return nil // Empty is valid
	}

	// Check for continuity
	for i := 1; i < len(events); i++ {
		prevSeq := events[i-1].Sequence()
		currSeq := events[i].Sequence()

		if currSeq != prevSeq+1 {
			return fmt.Errorf("sequence gap detected: expected %d, got %d", prevSeq+1, currSeq)
		}
	}

	return nil
}
