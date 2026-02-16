package persistence

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"matching-engine/internal/matching"
)

func TestFileEventStore_AppendAndRead(t *testing.T) {
	// Setup: create temp directory
	tempDir := t.TempDir()
	store, err := NewFileEventStore(filepath.Join(tempDir, "events"))
	if err != nil {
		t.Fatalf("failed to create event store: %v", err)
	}
	defer store.Close()

	ctx := context.Background()
	symbol := "BTC-USDT"

	// Create test events
	events := []matching.Event{
		&matching.OrderAcceptedEvent{
			EventIDValue:    "evt-1",
			SequenceValue:   1,
			SymbolValue:     symbol,
			OccurredAtValue: time.Now(),
			OrderID:         "order-1",
			ClientOrderID:   "client-1",
			AccountID:       "acc-1",
			Side:            matching.SideBuy,
			Price:           100000,
			Quantity:        10000,
			Status:          matching.OrderStatusNew,
		},
		&matching.OrderMatchedEvent{
			EventIDValue:    "evt-2",
			SequenceValue:   2,
			SymbolValue:     symbol,
			OccurredAtValue: time.Now(),
			TradeID:         "trade-1",
			MakerOrderID:    "order-1",
			TakerOrderID:    "order-2",
			Price:           100000,
			Quantity:        5000,
			MakerSide:       matching.SideBuy,
			TakerSide:       matching.SideSell,
		},
	}

	// Test: Append events
	for _, event := range events {
		if err := store.Append(ctx, symbol, event); err != nil {
			t.Fatalf("failed to append event: %v", err)
		}
	}

	// Test: Read events from sequence 1
	readEvents, err := store.ReadFrom(ctx, symbol, 1)
	if err != nil {
		t.Fatalf("failed to read events: %v", err)
	}

	if len(readEvents) != 2 {
		t.Fatalf("expected 2 events, got %d", len(readEvents))
	}

	// Verify first event
	if readEvents[0].EventType() != "OrderAccepted" {
		t.Errorf("expected OrderAccepted, got %s", readEvents[0].EventType())
	}
	if readEvents[0].Sequence() != 1 {
		t.Errorf("expected sequence 1, got %d", readEvents[0].Sequence())
	}

	// Verify second event
	if readEvents[1].EventType() != "OrderMatched" {
		t.Errorf("expected OrderMatched, got %s", readEvents[1].EventType())
	}
	if readEvents[1].Sequence() != 2 {
		t.Errorf("expected sequence 2, got %d", readEvents[1].Sequence())
	}
}

func TestFileEventStore_GetLastSequence(t *testing.T) {
	tempDir := t.TempDir()
	store, err := NewFileEventStore(filepath.Join(tempDir, "events"))
	if err != nil {
		t.Fatalf("failed to create event store: %v", err)
	}
	defer store.Close()

	ctx := context.Background()
	symbol := "BTC-USDT"

	// Test: Empty store should return 0
	lastSeq, err := store.GetLastSequence(ctx, symbol)
	if err != nil {
		t.Fatalf("failed to get last sequence: %v", err)
	}
	if lastSeq != 0 {
		t.Errorf("expected last sequence 0, got %d", lastSeq)
	}

	// Append some events
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
		if err := store.Append(ctx, symbol, event); err != nil {
			t.Fatalf("failed to append event: %v", err)
		}
	}

	// Test: Should return 5
	lastSeq, err = store.GetLastSequence(ctx, symbol)
	if err != nil {
		t.Fatalf("failed to get last sequence: %v", err)
	}
	if lastSeq != 5 {
		t.Errorf("expected last sequence 5, got %d", lastSeq)
	}
}

func TestFileEventStore_ReadFromMiddle(t *testing.T) {
	tempDir := t.TempDir()
	store, err := NewFileEventStore(filepath.Join(tempDir, "events"))
	if err != nil {
		t.Fatalf("failed to create event store: %v", err)
	}
	defer store.Close()

	ctx := context.Background()
	symbol := "BTC-USDT"

	// Append 5 events
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
		if err := store.Append(ctx, symbol, event); err != nil {
			t.Fatalf("failed to append event: %v", err)
		}
	}

	// Test: Read from sequence 3
	events, err := store.ReadFrom(ctx, symbol, 3)
	if err != nil {
		t.Fatalf("failed to read events: %v", err)
	}

	if len(events) != 3 {
		t.Fatalf("expected 3 events (seq 3,4,5), got %d", len(events))
	}

	if events[0].Sequence() != 3 {
		t.Errorf("expected first event sequence 3, got %d", events[0].Sequence())
	}
	if events[2].Sequence() != 5 {
		t.Errorf("expected last event sequence 5, got %d", events[2].Sequence())
	}
}

func TestFileEventStore_MultipleSymbols(t *testing.T) {
	tempDir := t.TempDir()
	store, err := NewFileEventStore(filepath.Join(tempDir, "events"))
	if err != nil {
		t.Fatalf("failed to create event store: %v", err)
	}
	defer store.Close()

	ctx := context.Background()

	// Append events for BTC-USDT
	btcEvent := &matching.OrderAcceptedEvent{
		EventIDValue:    "evt-btc-1",
		SequenceValue:   1,
		SymbolValue:     "BTC-USDT",
		OccurredAtValue: time.Now(),
		OrderID:         "order-btc-1",
		ClientOrderID:   "client-btc-1",
		AccountID:       "acc-1",
		Side:            matching.SideBuy,
		Price:           100000,
		Quantity:        10000,
		Status:          matching.OrderStatusNew,
	}
	if err := store.Append(ctx, "BTC-USDT", btcEvent); err != nil {
		t.Fatalf("failed to append BTC event: %v", err)
	}

	// Append events for ETH-USDT
	ethEvent := &matching.OrderAcceptedEvent{
		EventIDValue:    "evt-eth-1",
		SequenceValue:   1,
		SymbolValue:     "ETH-USDT",
		OccurredAtValue: time.Now(),
		OrderID:         "order-eth-1",
		ClientOrderID:   "client-eth-1",
		AccountID:       "acc-1",
		Side:            matching.SideSell,
		Price:           5000,
		Quantity:        20000,
		Status:          matching.OrderStatusNew,
	}
	if err := store.Append(ctx, "ETH-USDT", ethEvent); err != nil {
		t.Fatalf("failed to append ETH event: %v", err)
	}

	// Test: Read BTC events
	btcEvents, err := store.ReadFrom(ctx, "BTC-USDT", 1)
	if err != nil {
		t.Fatalf("failed to read BTC events: %v", err)
	}
	if len(btcEvents) != 1 {
		t.Fatalf("expected 1 BTC event, got %d", len(btcEvents))
	}
	if btcEvents[0].Symbol() != "BTC-USDT" {
		t.Errorf("expected BTC-USDT, got %s", btcEvents[0].Symbol())
	}

	// Test: Read ETH events
	ethEvents, err := store.ReadFrom(ctx, "ETH-USDT", 1)
	if err != nil {
		t.Fatalf("failed to read ETH events: %v", err)
	}
	if len(ethEvents) != 1 {
		t.Fatalf("expected 1 ETH event, got %d", len(ethEvents))
	}
	if ethEvents[0].Symbol() != "ETH-USDT" {
		t.Errorf("expected ETH-USDT, got %s", ethEvents[0].Symbol())
	}
}
