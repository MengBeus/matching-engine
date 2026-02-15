package matching

import (
	"fmt"
	"testing"
)

// TestDeterministicReplay tests that the same sequence of commands produces identical results
func TestDeterministicReplay(t *testing.T) {
	// Define a sequence of commands
	commands := []struct {
		name string
		req  *PlaceOrderRequest
	}{
		{
			name: "buy1",
			req: &PlaceOrderRequest{
				OrderID:       "buy1",
				ClientOrderID: "cli_buy1",
				AccountID:     "acc1",
				Symbol:        "BTC-USDT",
				Side:          SideBuy,
				PriceInt:      43000,
				QuantityInt:   100,
			},
		},
		{
			name: "buy2",
			req: &PlaceOrderRequest{
				OrderID:       "buy2",
				ClientOrderID: "cli_buy2",
				AccountID:     "acc2",
				Symbol:        "BTC-USDT",
				Side:          SideBuy,
				PriceInt:      43100,
				QuantityInt:   200,
			},
		},
		{
			name: "sell1",
			req: &PlaceOrderRequest{
				OrderID:       "sell1",
				ClientOrderID: "cli_sell1",
				AccountID:     "acc3",
				Symbol:        "BTC-USDT",
				Side:          SideSell,
				PriceInt:      43000,
				QuantityInt:   150,
			},
		},
		{
			name: "sell2",
			req: &PlaceOrderRequest{
				OrderID:       "sell2",
				ClientOrderID: "cli_sell2",
				AccountID:     "acc4",
				Symbol:        "BTC-USDT",
				Side:          SideSell,
				PriceInt:      42900,
				QuantityInt:   50,
			},
		},
	}

	// Run first execution
	ob1 := NewOrderBook("BTC-USDT")
	var results1 []*CommandResult
	for _, cmd := range commands {
		result := mustPlaceLimit(t, ob1, cmd.req)
		results1 = append(results1, result)
	}

	// Run second execution
	ob2 := NewOrderBook("BTC-USDT")
	var results2 []*CommandResult
	for _, cmd := range commands {
		result := mustPlaceLimit(t, ob2, cmd.req)
		results2 = append(results2, result)
	}

	// Compare results (ignoring timestamps and event IDs)
	if len(results1) != len(results2) {
		t.Fatalf("Different number of results: %d vs %d", len(results1), len(results2))
	}

	for i := range results1 {
		r1, r2 := results1[i], results2[i]

		// Compare trades
		if len(r1.Trades) != len(r2.Trades) {
			t.Errorf("Command %d: different number of trades: %d vs %d", i, len(r1.Trades), len(r2.Trades))
			continue
		}

		for j := range r1.Trades {
			t1, t2 := r1.Trades[j], r2.Trades[j]
			if t1.MakerOrderID != t2.MakerOrderID {
				t.Errorf("Command %d, trade %d: MakerOrderID mismatch: %s vs %s", i, j, t1.MakerOrderID, t2.MakerOrderID)
			}
			if t1.TakerOrderID != t2.TakerOrderID {
				t.Errorf("Command %d, trade %d: TakerOrderID mismatch: %s vs %s", i, j, t1.TakerOrderID, t2.TakerOrderID)
			}
			if t1.Price != t2.Price {
				t.Errorf("Command %d, trade %d: Price mismatch: %d vs %d", i, j, t1.Price, t2.Price)
			}
			if t1.Quantity != t2.Quantity {
				t.Errorf("Command %d, trade %d: Quantity mismatch: %d vs %d", i, j, t1.Quantity, t2.Quantity)
			}
			if t1.MakerSide != t2.MakerSide {
				t.Errorf("Command %d, trade %d: MakerSide mismatch: %s vs %s", i, j, t1.MakerSide, t2.MakerSide)
			}
			if t1.TakerSide != t2.TakerSide {
				t.Errorf("Command %d, trade %d: TakerSide mismatch: %s vs %s", i, j, t1.TakerSide, t2.TakerSide)
			}
		}

		// Compare order status changes
		if len(r1.OrderStatusChanges) != len(r2.OrderStatusChanges) {
			t.Errorf("Command %d: different number of status changes: %d vs %d", i, len(r1.OrderStatusChanges), len(r2.OrderStatusChanges))
			continue
		}

		for j := range r1.OrderStatusChanges {
			s1, s2 := r1.OrderStatusChanges[j], r2.OrderStatusChanges[j]
			if s1.OrderID != s2.OrderID {
				t.Errorf("Command %d, status %d: OrderID mismatch: %s vs %s", i, j, s1.OrderID, s2.OrderID)
			}
			if s1.OldStatus != s2.OldStatus {
				t.Errorf("Command %d, status %d: OldStatus mismatch: %s vs %s", i, j, s1.OldStatus, s2.OldStatus)
			}
			if s1.NewStatus != s2.NewStatus {
				t.Errorf("Command %d, status %d: NewStatus mismatch: %s vs %s", i, j, s1.NewStatus, s2.NewStatus)
			}
			if s1.RemainingQty != s2.RemainingQty {
				t.Errorf("Command %d, status %d: RemainingQty mismatch: %d vs %d", i, j, s1.RemainingQty, s2.RemainingQty)
			}
			if s1.FilledQty != s2.FilledQty {
				t.Errorf("Command %d, status %d: FilledQty mismatch: %d vs %d", i, j, s1.FilledQty, s2.FilledQty)
			}
		}

		// Compare events (ignore event_id and occurred_at, keep business fields and sequence)
		if len(r1.Events) != len(r2.Events) {
			t.Errorf("Command %d: different number of events: %d vs %d", i, len(r1.Events), len(r2.Events))
			continue
		}
		for j := range r1.Events {
			e1 := compactEvent(r1.Events[j])
			e2 := compactEvent(r2.Events[j])
			if e1 != e2 {
				t.Errorf("Command %d, event %d mismatch:\nrun1: %s\nrun2: %s", i, j, e1, e2)
			}
		}
	}

	// Compare final order book state
	if len(ob1.Orders) != len(ob2.Orders) {
		t.Errorf("Final order book: different number of orders: %d vs %d", len(ob1.Orders), len(ob2.Orders))
	}

	for orderID, order1 := range ob1.Orders {
		order2, exists := ob2.Orders[orderID]
		if !exists {
			t.Errorf("Order %s exists in ob1 but not in ob2", orderID)
			continue
		}
		if order1.RemainingQty != order2.RemainingQty {
			t.Errorf("Order %s: RemainingQty mismatch: %d vs %d", orderID, order1.RemainingQty, order2.RemainingQty)
		}
		if order1.Status != order2.Status {
			t.Errorf("Order %s: Status mismatch: %s vs %s", orderID, order1.Status, order2.Status)
		}
	}
}

// TestDeterministicReplayWithCancel tests determinism with cancel operations
func TestDeterministicReplayWithCancel(t *testing.T) {
	// Run first execution
	ob1 := NewOrderBook("BTC-USDT")
	mustPlaceLimit(t, ob1, &PlaceOrderRequest{
		OrderID:       "buy1",
		ClientOrderID: "cli_buy1",
		AccountID:     "acc1",
		Symbol:        "BTC-USDT",
		Side:          SideBuy,
		PriceInt:      43000,
		QuantityInt:   100,
	})
	mustPlaceLimit(t, ob1, &PlaceOrderRequest{
		OrderID:       "buy2",
		ClientOrderID: "cli_buy2",
		AccountID:     "acc2",
		Symbol:        "BTC-USDT",
		Side:          SideBuy,
		PriceInt:      43000,
		QuantityInt:   200,
	})
	result1, err1 := ob1.Cancel(&CancelOrderRequest{
		OrderID:   "buy1",
		AccountID: "acc1",
		Symbol:    "BTC-USDT",
	})
	if err1 != nil {
		t.Fatalf("Cancel failed in ob1: %v", err1)
	}

	// Run second execution
	ob2 := NewOrderBook("BTC-USDT")
	mustPlaceLimit(t, ob2, &PlaceOrderRequest{
		OrderID:       "buy1",
		ClientOrderID: "cli_buy1",
		AccountID:     "acc1",
		Symbol:        "BTC-USDT",
		Side:          SideBuy,
		PriceInt:      43000,
		QuantityInt:   100,
	})
	mustPlaceLimit(t, ob2, &PlaceOrderRequest{
		OrderID:       "buy2",
		ClientOrderID: "cli_buy2",
		AccountID:     "acc2",
		Symbol:        "BTC-USDT",
		Side:          SideBuy,
		PriceInt:      43000,
		QuantityInt:   200,
	})
	result2, err2 := ob2.Cancel(&CancelOrderRequest{
		OrderID:   "buy1",
		AccountID: "acc1",
		Symbol:    "BTC-USDT",
	})
	if err2 != nil {
		t.Fatalf("Cancel failed in ob2: %v", err2)
	}

	// Compare cancel results
	if len(result1.OrderStatusChanges) != len(result2.OrderStatusChanges) {
		t.Errorf("Different number of status changes: %d vs %d", len(result1.OrderStatusChanges), len(result2.OrderStatusChanges))
	}

	// Compare final state
	if len(ob1.Orders) != len(ob2.Orders) {
		t.Errorf("Different number of orders: %d vs %d", len(ob1.Orders), len(ob2.Orders))
	}

	// Verify buy1 is canceled in both
	if _, exists := ob1.Orders["buy1"]; exists {
		t.Errorf("buy1 should be canceled in ob1")
	}
	if _, exists := ob2.Orders["buy1"]; exists {
		t.Errorf("buy1 should be canceled in ob2")
	}

	// Verify buy2 still exists in both
	if _, exists := ob1.Orders["buy2"]; !exists {
		t.Errorf("buy2 should exist in ob1")
	}
	if _, exists := ob2.Orders["buy2"]; !exists {
		t.Errorf("buy2 should exist in ob2")
	}
}

// TestSequenceMonotonicity tests that event sequence numbers are strictly continuous (+1)
func TestSequenceMonotonicity(t *testing.T) {
	ob := NewOrderBook("BTC-USDT")

	var lastSequence int64 = 0

	// Place orders and verify sequence increases
	result1 := mustPlaceLimit(t, ob, &PlaceOrderRequest{
		OrderID:       "buy1",
		ClientOrderID: "cli_buy1",
		AccountID:     "acc1",
		Symbol:        "BTC-USDT",
		Side:          SideBuy,
		PriceInt:      43000,
		QuantityInt:   100,
	})

	for _, event := range result1.Events {
		seq := event.Sequence()
		if seq != lastSequence+1 {
			t.Errorf("Sequence not continuous: got %d, want %d", seq, lastSequence+1)
		}
		lastSequence = seq
	}

	result2 := mustPlaceLimit(t, ob, &PlaceOrderRequest{
		OrderID:       "sell1",
		ClientOrderID: "cli_sell1",
		AccountID:     "acc2",
		Symbol:        "BTC-USDT",
		Side:          SideSell,
		PriceInt:      43000,
		QuantityInt:   50,
	})

	for _, event := range result2.Events {
		seq := event.Sequence()
		if seq != lastSequence+1 {
			t.Errorf("Sequence not continuous: got %d, want %d", seq, lastSequence+1)
		}
		lastSequence = seq
	}
}

// TestEventOrdering tests that events are generated in correct order
func TestEventOrdering(t *testing.T) {
	ob := NewOrderBook("BTC-USDT")

	// Place a buy order
	result1 := mustPlaceLimit(t, ob, &PlaceOrderRequest{
		OrderID:       "buy1",
		ClientOrderID: "cli_buy1",
		AccountID:     "acc1",
		Symbol:        "BTC-USDT",
		Side:          SideBuy,
		PriceInt:      43000,
		QuantityInt:   100,
	})

	// Should have OrderAccepted event
	if len(result1.Events) != 1 {
		t.Fatalf("Expected 1 event, got %d", len(result1.Events))
	}
	if result1.Events[0].EventType() != "OrderAccepted" {
		t.Errorf("Expected OrderAccepted, got %s", result1.Events[0].EventType())
	}

	// Place a matching sell order
	result2 := mustPlaceLimit(t, ob, &PlaceOrderRequest{
		OrderID:       "sell1",
		ClientOrderID: "cli_sell1",
		AccountID:     "acc2",
		Symbol:        "BTC-USDT",
		Side:          SideSell,
		PriceInt:      43000,
		QuantityInt:   100,
	})

	// Should have OrderAccepted followed by OrderMatched
	if len(result2.Events) < 2 {
		t.Fatalf("Expected at least 2 events, got %d", len(result2.Events))
	}
	if result2.Events[0].EventType() != "OrderAccepted" {
		t.Errorf("First event should be OrderAccepted, got %s", result2.Events[0].EventType())
	}
	if result2.Events[1].EventType() != "OrderMatched" {
		t.Errorf("Second event should be OrderMatched, got %s", result2.Events[1].EventType())
	}

	// Verify sequence order
	for i := 1; i < len(result2.Events); i++ {
		if result2.Events[i].Sequence() != result2.Events[i-1].Sequence()+1 {
			t.Errorf("Event %d sequence (%d) should equal previous + 1 (%d)",
				i, result2.Events[i].Sequence(), result2.Events[i-1].Sequence()+1)
		}
	}
}

func compactEvent(event Event) string {
	switch e := event.(type) {
	case *OrderAcceptedEvent:
		return fmt.Sprintf("OrderAccepted|%d|%s|%s|%s|%s|%s|%d|%d|%s",
			e.Sequence(), e.Symbol(), e.OrderID, e.ClientOrderID, e.AccountID, e.Side, e.Price, e.Quantity, e.Status)
	case *OrderMatchedEvent:
		return fmt.Sprintf("OrderMatched|%d|%s|%s|%s|%d|%d|%s|%s",
			e.Sequence(), e.Symbol(), e.MakerOrderID, e.TakerOrderID, e.Price, e.Quantity, e.MakerSide, e.TakerSide)
	case *OrderCanceledEvent:
		return fmt.Sprintf("OrderCanceled|%d|%s|%s|%s|%d|%s",
			e.Sequence(), e.Symbol(), e.OrderID, e.AccountID, e.RemainingQty, e.CanceledBy)
	default:
		return fmt.Sprintf("%s|%d|%s", event.EventType(), event.Sequence(), event.Symbol())
	}
}
