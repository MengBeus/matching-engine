package persistence

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"matching-engine/internal/matching"
)

func TestFileRecoveryService_RecoverFromSnapshot(t *testing.T) {
	tempDir := t.TempDir()

	// Setup stores
	eventStore, err := NewFileEventStore(filepath.Join(tempDir, "events"))
	if err != nil {
		t.Fatalf("failed to create event store: %v", err)
	}
	defer eventStore.Close()

	snapshotStore, err := NewFileSnapshotStore(filepath.Join(tempDir, "snapshots"))
	if err != nil {
		t.Fatalf("failed to create snapshot store: %v", err)
	}
	defer snapshotStore.Close()

	recoveryService := NewFileRecoveryService(eventStore, snapshotStore)

	ctx := context.Background()
	symbol := "BTC-USDT"

	// Append events 1-5
	for i := int64(1); i <= 5; i++ {
		event := &matching.OrderAcceptedEvent{
			EventIDValue:    "evt-" + string(rune(i)),
			SequenceValue:   i,
			SymbolValue:     symbol,
			OccurredAtValue: time.Now(),
			OrderID:         "order-" + string(rune(i)),
			ClientOrderID:   "client-" + string(rune(i)),
			AccountID:       "acc-1",
			Side:            matching.SideBuy,
			Price:           100000,
			Quantity:        10000,
			Status:          matching.OrderStatusNew,
		}
		if err := eventStore.Append(ctx, symbol, event); err != nil {
			t.Fatalf("failed to append event: %v", err)
		}
	}

	// Save snapshot at sequence 3
	snapshot := &Snapshot{
		Version:      1,
		Symbol:       symbol,
		LastSequence: 3,
		CapturedAt:   time.Now(),
		Orderbook:    map[string]any{"bids": []any{}, "asks": []any{}},
	}
	if err := snapshotStore.Save(ctx, snapshot); err != nil {
		t.Fatalf("failed to save snapshot: %v", err)
	}

	// Append events 6-8 after snapshot
	for i := int64(6); i <= 8; i++ {
		event := &matching.OrderAcceptedEvent{
			EventIDValue:    "evt-" + string(rune(i)),
			SequenceValue:   i,
			SymbolValue:     symbol,
			OccurredAtValue: time.Now(),
			OrderID:         "order-" + string(rune(i)),
			ClientOrderID:   "client-" + string(rune(i)),
			AccountID:       "acc-1",
			Side:            matching.SideBuy,
			Price:           100000,
			Quantity:        10000,
			Status:          matching.OrderStatusNew,
		}
		if err := eventStore.Append(ctx, symbol, event); err != nil {
			t.Fatalf("failed to append event: %v", err)
		}
	}

	// Test: Recover should return snapshot and events 4-8
	recoveredSnapshot, events, err := recoveryService.Recover(ctx, symbol)
	if err != nil {
		t.Fatalf("failed to recover: %v", err)
	}

	if recoveredSnapshot == nil {
		t.Fatal("expected snapshot, got nil")
	}
	if recoveredSnapshot.LastSequence != 3 {
		t.Errorf("expected snapshot last sequence 3, got %d", recoveredSnapshot.LastSequence)
	}

	// Should have events 4-8 (5 events)
	if len(events) != 5 {
		t.Fatalf("expected 5 events to replay, got %d", len(events))
	}

	if events[0].Sequence() != 4 {
		t.Errorf("expected first event sequence 4, got %d", events[0].Sequence())
	}
	if events[4].Sequence() != 8 {
		t.Errorf("expected last event sequence 8, got %d", events[4].Sequence())
	}
}

func TestFileRecoveryService_RecoverFromEmpty(t *testing.T) {
	tempDir := t.TempDir()

	eventStore, err := NewFileEventStore(filepath.Join(tempDir, "events"))
	if err != nil {
		t.Fatalf("failed to create event store: %v", err)
	}
	defer eventStore.Close()

	snapshotStore, err := NewFileSnapshotStore(filepath.Join(tempDir, "snapshots"))
	if err != nil {
		t.Fatalf("failed to create snapshot store: %v", err)
	}
	defer snapshotStore.Close()

	recoveryService := NewFileRecoveryService(eventStore, snapshotStore)

	ctx := context.Background()
	symbol := "BTC-USDT"

	// Append some events
	for i := int64(1); i <= 3; i++ {
		event := &matching.OrderAcceptedEvent{
			EventIDValue:    "evt-" + string(rune(i)),
			SequenceValue:   i,
			SymbolValue:     symbol,
			OccurredAtValue: time.Now(),
			OrderID:         "order-" + string(rune(i)),
			ClientOrderID:   "client-" + string(rune(i)),
			AccountID:       "acc-1",
			Side:            matching.SideBuy,
			Price:           100000,
			Quantity:        10000,
			Status:          matching.OrderStatusNew,
		}
		if err := eventStore.Append(ctx, symbol, event); err != nil {
			t.Fatalf("failed to append event: %v", err)
		}
	}

	// Test: Recover without snapshot should replay all events
	recoveredSnapshot, events, err := recoveryService.Recover(ctx, symbol)
	if err != nil {
		t.Fatalf("failed to recover: %v", err)
	}

	if recoveredSnapshot != nil {
		t.Errorf("expected nil snapshot, got %v", recoveredSnapshot)
	}

	if len(events) != 3 {
		t.Fatalf("expected 3 events to replay, got %d", len(events))
	}

	if events[0].Sequence() != 1 {
		t.Errorf("expected first event sequence 1, got %d", events[0].Sequence())
	}
}

func TestFileRecoveryService_ValidateSequence(t *testing.T) {
	recoveryService := NewFileRecoveryService(nil, nil)

	tests := []struct {
		name      string
		events    []matching.Event
		expectErr bool
	}{
		{
			name:      "empty events",
			events:    []matching.Event{},
			expectErr: false,
		},
		{
			name: "continuous sequence",
			events: []matching.Event{
				&matching.OrderAcceptedEvent{SequenceValue: 1},
				&matching.OrderAcceptedEvent{SequenceValue: 2},
				&matching.OrderAcceptedEvent{SequenceValue: 3},
			},
			expectErr: false,
		},
		{
			name: "gap in sequence",
			events: []matching.Event{
				&matching.OrderAcceptedEvent{SequenceValue: 1},
				&matching.OrderAcceptedEvent{SequenceValue: 2},
				&matching.OrderAcceptedEvent{SequenceValue: 4}, // Gap: missing 3
			},
			expectErr: true,
		},
		{
			name: "duplicate sequence",
			events: []matching.Event{
				&matching.OrderAcceptedEvent{SequenceValue: 1},
				&matching.OrderAcceptedEvent{SequenceValue: 2},
				&matching.OrderAcceptedEvent{SequenceValue: 2}, // Duplicate
			},
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := recoveryService.ValidateSequence(tt.events)
			if tt.expectErr && err == nil {
				t.Error("expected error, got nil")
			}
			if !tt.expectErr && err != nil {
				t.Errorf("expected no error, got %v", err)
			}
		})
	}
}

func TestFileRecoveryService_RecoverNewSymbol(t *testing.T) {
	tempDir := t.TempDir()

	eventStore, err := NewFileEventStore(filepath.Join(tempDir, "events"))
	if err != nil {
		t.Fatalf("failed to create event store: %v", err)
	}
	defer eventStore.Close()

	snapshotStore, err := NewFileSnapshotStore(filepath.Join(tempDir, "snapshots"))
	if err != nil {
		t.Fatalf("failed to create snapshot store: %v", err)
	}
	defer snapshotStore.Close()

	recoveryService := NewFileRecoveryService(eventStore, snapshotStore)

	ctx := context.Background()

	// Test: Recover non-existent symbol should return empty state
	recoveredSnapshot, events, err := recoveryService.Recover(ctx, "NEW-SYMBOL")
	if err != nil {
		t.Fatalf("failed to recover: %v", err)
	}

	if recoveredSnapshot != nil {
		t.Errorf("expected nil snapshot, got %v", recoveredSnapshot)
	}

	if len(events) != 0 {
		t.Errorf("expected 0 events, got %d", len(events))
	}
}
