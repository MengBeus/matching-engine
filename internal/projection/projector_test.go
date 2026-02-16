package projection

import (
	"context"
	"errors"
	"testing"
	"time"

	"matching-engine/internal/matching"
)

func TestProjector_SequenceValidation(t *testing.T) {
	ctx := context.Background()

	t.Run("continuous sequence passes", func(t *testing.T) {
		orderRepo := NewMemoryOrderRepository()
		tradeRepo := NewMemoryTradeRepository()
		projector := NewProjector(orderRepo, tradeRepo)

		// Project events with continuous sequence
		events := []*matching.OrderAcceptedEvent{
			{
				EventIDValue:    "event-1",
				SequenceValue:   1,
				SymbolValue:     "BTC-USDT",
				OccurredAtValue: time.Now(),
				OrderID:         "order-1",
				ClientOrderID:   "client-1",
				AccountID:       "acc-1",
				Side:            matching.SideBuy,
				Price:           50000,
				Quantity:        100,
			},
			{
				EventIDValue:    "event-2",
				SequenceValue:   2,
				SymbolValue:     "BTC-USDT",
				OccurredAtValue: time.Now(),
				OrderID:         "order-2",
				ClientOrderID:   "client-2",
				AccountID:       "acc-2",
				Side:            matching.SideSell,
				Price:           50000,
				Quantity:        100,
			},
			{
				EventIDValue:    "event-3",
				SequenceValue:   3,
				SymbolValue:     "BTC-USDT",
				OccurredAtValue: time.Now(),
				OrderID:         "order-3",
				ClientOrderID:   "client-3",
				AccountID:       "acc-3",
				Side:            matching.SideBuy,
				Price:           50000,
				Quantity:        100,
			},
		}

		for _, event := range events {
			if err := projector.Project(ctx, event); err != nil {
				t.Fatalf("expected continuous sequence to pass, got error: %v", err)
			}
		}

		// Verify last sequence
		lastSeq, err := orderRepo.GetLastSequence(ctx, "BTC-USDT")
		if err != nil {
			t.Fatalf("failed to get last sequence: %v", err)
		}
		if lastSeq != 3 {
			t.Errorf("expected last sequence 3, got %d", lastSeq)
		}
	})

	t.Run("sequence gap fails", func(t *testing.T) {
		orderRepo := NewMemoryOrderRepository()
		tradeRepo := NewMemoryTradeRepository()
		projector := NewProjector(orderRepo, tradeRepo)

		// Project first event
		event1 := &matching.OrderAcceptedEvent{
			EventIDValue:    "event-1",
			SequenceValue:   1,
			SymbolValue:     "BTC-USDT",
			OccurredAtValue: time.Now(),
			OrderID:         "order-1",
			ClientOrderID:   "client-1",
			AccountID:       "acc-1",
			Side:            matching.SideBuy,
			Price:           50000,
			Quantity:        100,
		}

		if err := projector.Project(ctx, event1); err != nil {
			t.Fatalf("first event should succeed: %v", err)
		}

		// Try to project event with gap (sequence 3 instead of 2)
		event3 := &matching.OrderAcceptedEvent{
			EventIDValue:    "event-3",
			SequenceValue:   3,
			SymbolValue:     "BTC-USDT",
			OccurredAtValue: time.Now(),
			OrderID:         "order-3",
			ClientOrderID:   "client-3",
			AccountID:       "acc-3",
			Side:            matching.SideBuy,
			Price:           50000,
			Quantity:        100,
		}

		err := projector.Project(ctx, event3)
		if err == nil {
			t.Fatal("expected sequence gap to fail, but it succeeded")
		}

		// Verify error message contains "gap"
		if !contains(err.Error(), "gap") {
			t.Errorf("expected error to mention 'gap', got: %v", err)
		}

		// Verify last sequence is still 1
		lastSeq, err := orderRepo.GetLastSequence(ctx, "BTC-USDT")
		if err != nil {
			t.Fatalf("failed to get last sequence: %v", err)
		}
		if lastSeq != 1 {
			t.Errorf("expected last sequence to remain 1, got %d", lastSeq)
		}
	})

	t.Run("sequence regression fails", func(t *testing.T) {
		orderRepo := NewMemoryOrderRepository()
		tradeRepo := NewMemoryTradeRepository()
		projector := NewProjector(orderRepo, tradeRepo)

		// Project events 1 and 2
		events := []*matching.OrderAcceptedEvent{
			{
				EventIDValue:    "event-1",
				SequenceValue:   1,
				SymbolValue:     "BTC-USDT",
				OccurredAtValue: time.Now(),
				OrderID:         "order-1",
				ClientOrderID:   "client-1",
				AccountID:       "acc-1",
				Side:            matching.SideBuy,
				Price:           50000,
				Quantity:        100,
			},
			{
				EventIDValue:    "event-2",
				SequenceValue:   2,
				SymbolValue:     "BTC-USDT",
				OccurredAtValue: time.Now(),
				OrderID:         "order-2",
				ClientOrderID:   "client-2",
				AccountID:       "acc-2",
				Side:            matching.SideSell,
				Price:           50000,
				Quantity:        100,
			},
		}

		for _, event := range events {
			if err := projector.Project(ctx, event); err != nil {
				t.Fatalf("setup events should succeed: %v", err)
			}
		}

		// Try to project event with regression (sequence 1 again)
		event1Again := &matching.OrderAcceptedEvent{
			EventIDValue:    "event-1-again",
			SequenceValue:   1,
			SymbolValue:     "BTC-USDT",
			OccurredAtValue: time.Now(),
			OrderID:         "order-1-again",
			ClientOrderID:   "client-1-again",
			AccountID:       "acc-1",
			Side:            matching.SideBuy,
			Price:           50000,
			Quantity:        100,
		}

		err := projector.Project(ctx, event1Again)
		if err == nil {
			t.Fatal("expected sequence regression to fail, but it succeeded")
		}

		// Verify error message contains "regression"
		if !contains(err.Error(), "regression") {
			t.Errorf("expected error to mention 'regression', got: %v", err)
		}

		// Verify last sequence is still 2
		lastSeq, err := orderRepo.GetLastSequence(ctx, "BTC-USDT")
		if err != nil {
			t.Fatalf("failed to get last sequence: %v", err)
		}
		if lastSeq != 2 {
			t.Errorf("expected last sequence to remain 2, got %d", lastSeq)
		}
	})

	t.Run("first event must be sequence 1", func(t *testing.T) {
		orderRepo := NewMemoryOrderRepository()
		tradeRepo := NewMemoryTradeRepository()
		projector := NewProjector(orderRepo, tradeRepo)

		// Try to project first event with sequence 2
		event := &matching.OrderAcceptedEvent{
			EventIDValue:    "event-2",
			SequenceValue:   2,
			SymbolValue:     "BTC-USDT",
			OccurredAtValue: time.Now(),
			OrderID:         "order-1",
			ClientOrderID:   "client-1",
			AccountID:       "acc-1",
			Side:            matching.SideBuy,
			Price:           50000,
			Quantity:        100,
		}

		err := projector.Project(ctx, event)
		if err == nil {
			t.Fatal("expected first event with sequence != 1 to fail")
		}

		if !contains(err.Error(), "first event must have sequence 1") {
			t.Errorf("expected error about first event, got: %v", err)
		}
	})
}

func TestProjector_OrderAccepted(t *testing.T) {
	ctx := context.Background()
	orderRepo := NewMemoryOrderRepository()
	tradeRepo := NewMemoryTradeRepository()
	projector := NewProjector(orderRepo, tradeRepo)

	now := time.Now()
	event := &matching.OrderAcceptedEvent{
		EventIDValue:    "event-1",
		SequenceValue:   1,
		SymbolValue:     "BTC-USDT",
		OccurredAtValue: now,
		OrderID:         "order-1",
		ClientOrderID:   "client-1",
		AccountID:       "acc-1",
		Side:            matching.SideBuy,
		Price:           50000,
		Quantity:        100,
	}

	if err := projector.Project(ctx, event); err != nil {
		t.Fatalf("failed to project OrderAccepted: %v", err)
	}

	// Verify order view was created
	order, err := orderRepo.GetByID(ctx, "order-1")
	if err != nil {
		t.Fatalf("failed to get order: %v", err)
	}

	if order.OrderID != "order-1" {
		t.Errorf("expected order_id 'order-1', got '%s'", order.OrderID)
	}
	if order.Status != OrderStatusNew {
		t.Errorf("expected status NEW, got %s", order.Status)
	}
	if order.RemainingQty != 100 {
		t.Errorf("expected remaining_qty 100, got %d", order.RemainingQty)
	}
	if order.FilledQty != 0 {
		t.Errorf("expected filled_qty 0, got %d", order.FilledQty)
	}
}

func TestProjector_OrderMatched(t *testing.T) {
	ctx := context.Background()
	orderRepo := NewMemoryOrderRepository()
	tradeRepo := NewMemoryTradeRepository()
	projector := NewProjector(orderRepo, tradeRepo)

	now := time.Now()

	// Create maker order
	makerEvent := &matching.OrderAcceptedEvent{
		EventIDValue:    "event-1",
		SequenceValue:   1,
		SymbolValue:     "BTC-USDT",
		OccurredAtValue: now,
		OrderID:         "maker-1",
		ClientOrderID:   "client-maker",
		AccountID:       "acc-maker",
		Side:            matching.SideBuy,
		Price:           50000,
		Quantity:        100,
	}

	if err := projector.Project(ctx, makerEvent); err != nil {
		t.Fatalf("failed to project maker order: %v", err)
	}

	// Create taker order
	takerEvent := &matching.OrderAcceptedEvent{
		EventIDValue:    "event-2",
		SequenceValue:   2,
		SymbolValue:     "BTC-USDT",
		OccurredAtValue: now,
		OrderID:         "taker-1",
		ClientOrderID:   "client-taker",
		AccountID:       "acc-taker",
		Side:            matching.SideSell,
		Price:           50000,
		Quantity:        100,
	}

	if err := projector.Project(ctx, takerEvent); err != nil {
		t.Fatalf("failed to project taker order: %v", err)
	}

	// Project match event (partial fill)
	matchEvent := &matching.OrderMatchedEvent{
		EventIDValue:    "event-3",
		SequenceValue:   3,
		SymbolValue:     "BTC-USDT",
		OccurredAtValue: now,
		TradeID:         "trade-1",
		MakerOrderID:    "maker-1",
		TakerOrderID:    "taker-1",
		Price:           50000,
		Quantity:        60, // Partial fill
		MakerSide:       matching.SideBuy,
	}

	if err := projector.Project(ctx, matchEvent); err != nil {
		t.Fatalf("failed to project OrderMatched: %v", err)
	}

	// Verify maker order updated
	makerOrder, err := orderRepo.GetByID(ctx, "maker-1")
	if err != nil {
		t.Fatalf("failed to get maker order: %v", err)
	}
	if makerOrder.FilledQty != 60 {
		t.Errorf("expected maker filled_qty 60, got %d", makerOrder.FilledQty)
	}
	if makerOrder.RemainingQty != 40 {
		t.Errorf("expected maker remaining_qty 40, got %d", makerOrder.RemainingQty)
	}
	if makerOrder.Status != OrderStatusPartiallyFilled {
		t.Errorf("expected maker status PARTIALLY_FILLED, got %s", makerOrder.Status)
	}

	// Verify taker order updated
	takerOrder, err := orderRepo.GetByID(ctx, "taker-1")
	if err != nil {
		t.Fatalf("failed to get taker order: %v", err)
	}
	if takerOrder.FilledQty != 60 {
		t.Errorf("expected taker filled_qty 60, got %d", takerOrder.FilledQty)
	}
	if takerOrder.RemainingQty != 40 {
		t.Errorf("expected taker remaining_qty 40, got %d", takerOrder.RemainingQty)
	}

	// Verify trade view created
	trade, err := tradeRepo.GetByID(ctx, "trade-1")
	if err != nil {
		t.Fatalf("failed to get trade: %v", err)
	}
	if trade.MakerOrderID != "maker-1" {
		t.Errorf("expected maker_order_id 'maker-1', got '%s'", trade.MakerOrderID)
	}
	if trade.Quantity != 60 {
		t.Errorf("expected trade quantity 60, got %d", trade.Quantity)
	}
}

func TestProjector_OrderCanceled(t *testing.T) {
	ctx := context.Background()
	orderRepo := NewMemoryOrderRepository()
	tradeRepo := NewMemoryTradeRepository()
	projector := NewProjector(orderRepo, tradeRepo)

	now := time.Now()

	// Create order
	acceptEvent := &matching.OrderAcceptedEvent{
		EventIDValue:    "event-1",
		SequenceValue:   1,
		SymbolValue:     "BTC-USDT",
		OccurredAtValue: now,
		OrderID:         "order-1",
		ClientOrderID:   "client-1",
		AccountID:       "acc-1",
		Side:            matching.SideBuy,
		Price:           50000,
		Quantity:        100,
	}

	if err := projector.Project(ctx, acceptEvent); err != nil {
		t.Fatalf("failed to project order: %v", err)
	}

	// Cancel order
	cancelEvent := &matching.OrderCanceledEvent{
		EventIDValue:    "event-2",
		SequenceValue:   2,
		SymbolValue:     "BTC-USDT",
		OccurredAtValue: now,
		OrderID:         "order-1",
		AccountID:       "acc-1",
		RemainingQty:    100,
		CanceledBy:      matching.CancelReasonUser,
	}

	if err := projector.Project(ctx, cancelEvent); err != nil {
		t.Fatalf("failed to project OrderCanceled: %v", err)
	}

	// Verify order status updated
	order, err := orderRepo.GetByID(ctx, "order-1")
	if err != nil {
		t.Fatalf("failed to get order: %v", err)
	}
	if order.Status != OrderStatusCanceled {
		t.Errorf("expected status CANCELED, got %s", order.Status)
	}
}

func TestProjector_SequenceMismatchFails(t *testing.T) {
	ctx := context.Background()
	orderRepo := NewMemoryOrderRepository()
	tradeRepo := NewMemoryTradeRepository()
	projector := NewProjector(orderRepo, tradeRepo)

	if err := orderRepo.SetLastSequence(ctx, "BTC-USDT", 1); err != nil {
		t.Fatalf("failed to set order sequence: %v", err)
	}

	event := &matching.OrderAcceptedEvent{
		EventIDValue:    "event-2",
		SequenceValue:   2,
		SymbolValue:     "BTC-USDT",
		OccurredAtValue: time.Now(),
		OrderID:         "order-2",
		ClientOrderID:   "client-2",
		AccountID:       "acc-2",
		Side:            matching.SideBuy,
		Price:           50000,
		Quantity:        100,
	}

	err := projector.Project(ctx, event)
	if err == nil {
		t.Fatal("expected projection sequence mismatch error")
	}
	if !contains(err.Error(), "projection sequence mismatch") {
		t.Fatalf("expected sequence mismatch error, got %v", err)
	}
}

func TestProjector_AdvanceTradeBeforeOrder(t *testing.T) {
	ctx := context.Background()
	orderRepo := NewMemoryOrderRepository()
	tradeRepo := &failOnceTradeSequenceRepo{
		MemoryTradeRepository: NewMemoryTradeRepository(),
	}
	projector := NewProjector(orderRepo, tradeRepo)

	event := &matching.OrderAcceptedEvent{
		EventIDValue:    "event-1",
		SequenceValue:   1,
		SymbolValue:     "BTC-USDT",
		OccurredAtValue: time.Now(),
		OrderID:         "order-1",
		ClientOrderID:   "client-1",
		AccountID:       "acc-1",
		Side:            matching.SideBuy,
		Price:           50000,
		Quantity:        100,
	}

	err := projector.Project(ctx, event)
	if err == nil {
		t.Fatal("expected first project to fail on trade sequence set")
	}

	orderSeq, _ := orderRepo.GetLastSequence(ctx, "BTC-USDT")
	if orderSeq != 0 {
		t.Fatalf("order sequence should not advance when trade sequence set fails, got %d", orderSeq)
	}

	if err := projector.Project(ctx, event); err != nil {
		t.Fatalf("retry should succeed: %v", err)
	}
	orderSeq, _ = orderRepo.GetLastSequence(ctx, "BTC-USDT")
	tradeSeq, _ := tradeRepo.GetLastSequence(ctx, "BTC-USDT")
	if orderSeq != 1 || tradeSeq != 1 {
		t.Fatalf("expected both sequences to advance to 1, got order=%d trade=%d", orderSeq, tradeSeq)
	}
}

func TestProjector_OrderMatchedRetryDoesNotDoubleApply(t *testing.T) {
	ctx := context.Background()
	orderRepo := &failOnceOrderSaveRepo{
		MemoryOrderRepository: NewMemoryOrderRepository(),
		failOrderID:           "taker-1",
		failSequence:          3,
	}
	tradeRepo := NewMemoryTradeRepository()
	projector := NewProjector(orderRepo, tradeRepo)

	now := time.Now()
	events := []matching.Event{
		&matching.OrderAcceptedEvent{
			EventIDValue:    "event-1",
			SequenceValue:   1,
			SymbolValue:     "BTC-USDT",
			OccurredAtValue: now,
			OrderID:         "maker-1",
			ClientOrderID:   "client-maker",
			AccountID:       "acc-maker",
			Side:            matching.SideBuy,
			Price:           50000,
			Quantity:        100,
		},
		&matching.OrderAcceptedEvent{
			EventIDValue:    "event-2",
			SequenceValue:   2,
			SymbolValue:     "BTC-USDT",
			OccurredAtValue: now,
			OrderID:         "taker-1",
			ClientOrderID:   "client-taker",
			AccountID:       "acc-taker",
			Side:            matching.SideSell,
			Price:           50000,
			Quantity:        100,
		},
	}
	for _, evt := range events {
		if err := projector.Project(ctx, evt); err != nil {
			t.Fatalf("setup event failed: %v", err)
		}
	}

	matchEvent := &matching.OrderMatchedEvent{
		EventIDValue:    "event-3",
		SequenceValue:   3,
		SymbolValue:     "BTC-USDT",
		OccurredAtValue: now,
		TradeID:         "trade-1",
		MakerOrderID:    "maker-1",
		TakerOrderID:    "taker-1",
		Price:           50000,
		Quantity:        60,
		MakerSide:       matching.SideBuy,
	}

	err := projector.Project(ctx, matchEvent)
	if err == nil {
		t.Fatal("expected first matched projection to fail")
	}

	if err := projector.Project(ctx, matchEvent); err != nil {
		t.Fatalf("retry projection should succeed: %v", err)
	}

	maker, err := orderRepo.GetByID(ctx, "maker-1")
	if err != nil {
		t.Fatalf("failed to load maker: %v", err)
	}
	if maker.FilledQty != 60 || maker.RemainingQty != 40 {
		t.Fatalf("maker should be applied exactly once, got filled=%d remaining=%d", maker.FilledQty, maker.RemainingQty)
	}
}

// Helper function to check if a string contains a substring
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > len(substr) && containsHelper(s, substr))
}

func containsHelper(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

type failOnceTradeSequenceRepo struct {
	*MemoryTradeRepository
	failed bool
}

func (r *failOnceTradeSequenceRepo) SetLastSequence(ctx context.Context, symbol string, sequence int64) error {
	if !r.failed {
		r.failed = true
		return errors.New("simulated trade sequence persist failure")
	}
	return r.MemoryTradeRepository.SetLastSequence(ctx, symbol, sequence)
}

type failOnceOrderSaveRepo struct {
	*MemoryOrderRepository
	failOrderID  string
	failSequence int64
	failed       bool
}

func (r *failOnceOrderSaveRepo) Save(ctx context.Context, order *OrderView) error {
	if !r.failed && order != nil && order.OrderID == r.failOrderID && order.LastSequence == r.failSequence {
		r.failed = true
		return errors.New("simulated order save failure")
	}
	return r.MemoryOrderRepository.Save(ctx, order)
}
