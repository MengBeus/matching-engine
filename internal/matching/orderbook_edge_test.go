package matching

import (
	"strings"
	"testing"
)

// TestCancelUnauthorized tests canceling order with wrong account
func TestCancelUnauthorized(t *testing.T) {
	ob := NewOrderBook("BTC-USDT")

	// Place order with acc1
	mustPlaceLimit(t, ob, &PlaceOrderRequest{
		OrderID:       "buy1",
		ClientOrderID: "cli_buy1",
		AccountID:     "acc1",
		Symbol:        "BTC-USDT",
		Side:          SideBuy,
		PriceInt:      43000,
		QuantityInt:   100,
	})

	// Try to cancel with different account
	_, err := ob.Cancel(&CancelOrderRequest{
		OrderID:   "buy1",
		AccountID: "acc2", // Wrong account
		Symbol:    "BTC-USDT",
	})

	if err == nil {
		t.Fatalf("Expected error when canceling with wrong account")
	}
	if !strings.Contains(err.Error(), "unauthorized") && !strings.Contains(err.Error(), "different account") {
		t.Errorf("Expected unauthorized error, got: %v", err)
	}

	// Verify order still exists
	if _, exists := ob.Orders["buy1"]; !exists {
		t.Errorf("Order should still exist after failed cancel")
	}
}

// TestCancelAlreadyCanceled tests canceling an already canceled order
func TestCancelAlreadyCanceled(t *testing.T) {
	ob := NewOrderBook("BTC-USDT")

	// Place order
	mustPlaceLimit(t, ob, &PlaceOrderRequest{
		OrderID:       "buy1",
		ClientOrderID: "cli_buy1",
		AccountID:     "acc1",
		Symbol:        "BTC-USDT",
		Side:          SideBuy,
		PriceInt:      43000,
		QuantityInt:   100,
	})

	// Cancel once
	_, err := ob.Cancel(&CancelOrderRequest{
		OrderID:   "buy1",
		AccountID: "acc1",
		Symbol:    "BTC-USDT",
	})
	if err != nil {
		t.Fatalf("First cancel failed: %v", err)
	}

	// Try to cancel again
	_, err = ob.Cancel(&CancelOrderRequest{
		OrderID:   "buy1",
		AccountID: "acc1",
		Symbol:    "BTC-USDT",
	})

	if err == nil {
		t.Fatalf("Expected error when canceling already canceled order")
	}
	if !strings.Contains(err.Error(), "already canceled") {
		t.Errorf("Expected 'already canceled' error, got: %v", err)
	}
}

// TestNoMatchPriceNotCrossing tests that orders don't match when prices don't cross
func TestNoMatchPriceNotCrossing(t *testing.T) {
	ob := NewOrderBook("BTC-USDT")

	// Place buy order at 43000
	result1 := mustPlaceLimit(t, ob, &PlaceOrderRequest{
		OrderID:       "buy1",
		ClientOrderID: "cli_buy1",
		AccountID:     "acc1",
		Symbol:        "BTC-USDT",
		Side:          SideBuy,
		PriceInt:      43000,
		QuantityInt:   100,
	})

	if len(result1.Trades) != 0 {
		t.Errorf("Expected no trades, got %d", len(result1.Trades))
	}

	// Place sell order at 43100 (higher than buy)
	result2 := mustPlaceLimit(t, ob, &PlaceOrderRequest{
		OrderID:       "sell1",
		ClientOrderID: "cli_sell1",
		AccountID:     "acc2",
		Symbol:        "BTC-USDT",
		Side:          SideSell,
		PriceInt:      43100,
		QuantityInt:   100,
	})

	if len(result2.Trades) != 0 {
		t.Errorf("Expected no trades, got %d", len(result2.Trades))
	}

	// Verify both orders are in order book
	if _, exists := ob.Orders["buy1"]; !exists {
		t.Errorf("buy1 should be in order book")
	}
	if _, exists := ob.Orders["sell1"]; !exists {
		t.Errorf("sell1 should be in order book")
	}

	// Verify bid and ask levels
	if len(ob.BidLevels) != 1 {
		t.Errorf("Expected 1 bid level, got %d", len(ob.BidLevels))
	}
	if len(ob.AskLevels) != 1 {
		t.Errorf("Expected 1 ask level, got %d", len(ob.AskLevels))
	}
}

// TestMatchAcrossMultiplePriceLevels tests matching across multiple price levels
func TestMatchAcrossMultiplePriceLevels(t *testing.T) {
	ob := NewOrderBook("BTC-USDT")

	// Place sell orders at different prices
	mustPlaceLimit(t, ob, &PlaceOrderRequest{
		OrderID:       "sell1",
		ClientOrderID: "cli_sell1",
		AccountID:     "acc1",
		Symbol:        "BTC-USDT",
		Side:          SideSell,
		PriceInt:      43000,
		QuantityInt:   100,
	})

	mustPlaceLimit(t, ob, &PlaceOrderRequest{
		OrderID:       "sell2",
		ClientOrderID: "cli_sell2",
		AccountID:     "acc2",
		Symbol:        "BTC-USDT",
		Side:          SideSell,
		PriceInt:      43100,
		QuantityInt:   100,
	})

	mustPlaceLimit(t, ob, &PlaceOrderRequest{
		OrderID:       "sell3",
		ClientOrderID: "cli_sell3",
		AccountID:     "acc3",
		Symbol:        "BTC-USDT",
		Side:          SideSell,
		PriceInt:      43200,
		QuantityInt:   100,
	})

	// Place a large buy order that crosses multiple levels
	result := mustPlaceLimit(t, ob, &PlaceOrderRequest{
		OrderID:       "buy1",
		ClientOrderID: "cli_buy1",
		AccountID:     "acc4",
		Symbol:        "BTC-USDT",
		Side:          SideBuy,
		PriceInt:      43150, // Crosses 43000 and 43100
		QuantityInt:   250,
	})

	// Should have 2 trades (43000 and 43100 levels)
	if len(result.Trades) != 2 {
		t.Fatalf("Expected 2 trades, got %d", len(result.Trades))
	}

	// First trade at 43000
	if result.Trades[0].Price != 43000 {
		t.Errorf("First trade price should be 43000, got %d", result.Trades[0].Price)
	}
	if result.Trades[0].Quantity != 100 {
		t.Errorf("First trade quantity should be 100, got %d", result.Trades[0].Quantity)
	}

	// Second trade at 43100
	if result.Trades[1].Price != 43100 {
		t.Errorf("Second trade price should be 43100, got %d", result.Trades[1].Price)
	}
	if result.Trades[1].Quantity != 100 {
		t.Errorf("Second trade quantity should be 100, got %d", result.Trades[1].Quantity)
	}

	// Verify sell1 and sell2 are fully filled
	if _, exists := ob.Orders["sell1"]; exists {
		t.Errorf("sell1 should be removed")
	}
	if _, exists := ob.Orders["sell2"]; exists {
		t.Errorf("sell2 should be removed")
	}

	// Verify sell3 still exists
	if _, exists := ob.Orders["sell3"]; !exists {
		t.Errorf("sell3 should still exist")
	}

	// Verify buy1 is partially filled with 50 remaining
	buy1 := ob.Orders["buy1"]
	if buy1 == nil {
		t.Fatalf("buy1 should exist")
	}
	if buy1.RemainingQty != 50 {
		t.Errorf("buy1 remaining should be 50, got %d", buy1.RemainingQty)
	}
	if buy1.Status != OrderStatusPartiallyFilled {
		t.Errorf("buy1 status should be PARTIALLY_FILLED, got %s", buy1.Status)
	}
}

// TestVolumeConsistencyAfterPartialFill tests volume consistency after partial fill
func TestVolumeConsistencyAfterPartialFill(t *testing.T) {
	ob := NewOrderBook("BTC-USDT")

	// Place buy order
	mustPlaceLimit(t, ob, &PlaceOrderRequest{
		OrderID:       "buy1",
		ClientOrderID: "cli_buy1",
		AccountID:     "acc1",
		Symbol:        "BTC-USDT",
		Side:          SideBuy,
		PriceInt:      43000,
		QuantityInt:   1000,
	})

	// Verify initial volume
	if ob.BidLevels[43000].Volume != 1000 {
		t.Errorf("Initial volume should be 1000, got %d", ob.BidLevels[43000].Volume)
	}

	// Partially fill with sell order
	mustPlaceLimit(t, ob, &PlaceOrderRequest{
		OrderID:       "sell1",
		ClientOrderID: "cli_sell1",
		AccountID:     "acc2",
		Symbol:        "BTC-USDT",
		Side:          SideSell,
		PriceInt:      43000,
		QuantityInt:   300,
	})

	// Verify volume after partial fill
	if ob.BidLevels[43000].Volume != 700 {
		t.Errorf("Volume after partial fill should be 700, got %d", ob.BidLevels[43000].Volume)
	}

	// Verify order remaining quantity matches volume
	buy1 := ob.Orders["buy1"]
	if buy1.RemainingQty != ob.BidLevels[43000].Volume {
		t.Errorf("Order remaining (%d) should match level volume (%d)", buy1.RemainingQty, ob.BidLevels[43000].Volume)
	}
}

// TestVolumeConsistencyAfterFullFill tests that price level is removed after full fill
func TestVolumeConsistencyAfterFullFill(t *testing.T) {
	ob := NewOrderBook("BTC-USDT")

	// Place buy order
	mustPlaceLimit(t, ob, &PlaceOrderRequest{
		OrderID:       "buy1",
		ClientOrderID: "cli_buy1",
		AccountID:     "acc1",
		Symbol:        "BTC-USDT",
		Side:          SideBuy,
		PriceInt:      43000,
		QuantityInt:   500,
	})

	// Fully fill with sell order
	mustPlaceLimit(t, ob, &PlaceOrderRequest{
		OrderID:       "sell1",
		ClientOrderID: "cli_sell1",
		AccountID:     "acc2",
		Symbol:        "BTC-USDT",
		Side:          SideSell,
		PriceInt:      43000,
		QuantityInt:   500,
	})

	// Verify price level is removed
	if _, exists := ob.BidLevels[43000]; exists {
		t.Errorf("Price level 43000 should be removed after full fill")
	}

	// Verify order is removed
	if _, exists := ob.Orders["buy1"]; exists {
		t.Errorf("buy1 should be removed after full fill")
	}
}

// TestVolumeConsistencyAfterCancel tests volume consistency after cancel
func TestVolumeConsistencyAfterCancel(t *testing.T) {
	ob := NewOrderBook("BTC-USDT")

	// Place two buy orders at same price
	mustPlaceLimit(t, ob, &PlaceOrderRequest{
		OrderID:       "buy1",
		ClientOrderID: "cli_buy1",
		AccountID:     "acc1",
		Symbol:        "BTC-USDT",
		Side:          SideBuy,
		PriceInt:      43000,
		QuantityInt:   300,
	})

	mustPlaceLimit(t, ob, &PlaceOrderRequest{
		OrderID:       "buy2",
		ClientOrderID: "cli_buy2",
		AccountID:     "acc2",
		Symbol:        "BTC-USDT",
		Side:          SideBuy,
		PriceInt:      43000,
		QuantityInt:   200,
	})

	// Verify total volume
	if ob.BidLevels[43000].Volume != 500 {
		t.Errorf("Total volume should be 500, got %d", ob.BidLevels[43000].Volume)
	}

	// Cancel buy1
	_, err := ob.Cancel(&CancelOrderRequest{
		OrderID:   "buy1",
		AccountID: "acc1",
		Symbol:    "BTC-USDT",
	})
	if err != nil {
		t.Fatalf("Cancel failed: %v", err)
	}

	// Verify volume after cancel
	if ob.BidLevels[43000].Volume != 200 {
		t.Errorf("Volume after cancel should be 200, got %d", ob.BidLevels[43000].Volume)
	}

	// Verify remaining order quantity matches volume
	buy2 := ob.Orders["buy2"]
	if buy2.RemainingQty != ob.BidLevels[43000].Volume {
		t.Errorf("Order remaining (%d) should match level volume (%d)", buy2.RemainingQty, ob.BidLevels[43000].Volume)
	}
}

// TestCancelPartiallyFilledOrder tests canceling a partially filled order
func TestCancelPartiallyFilledOrder(t *testing.T) {
	ob := NewOrderBook("BTC-USDT")

	// Place buy order
	mustPlaceLimit(t, ob, &PlaceOrderRequest{
		OrderID:       "buy1",
		ClientOrderID: "cli_buy1",
		AccountID:     "acc1",
		Symbol:        "BTC-USDT",
		Side:          SideBuy,
		PriceInt:      43000,
		QuantityInt:   1000,
	})

	// Partially fill
	mustPlaceLimit(t, ob, &PlaceOrderRequest{
		OrderID:       "sell1",
		ClientOrderID: "cli_sell1",
		AccountID:     "acc2",
		Symbol:        "BTC-USDT",
		Side:          SideSell,
		PriceInt:      43000,
		QuantityInt:   400,
	})

	// Verify order is partially filled
	buy1 := ob.Orders["buy1"]
	if buy1.Status != OrderStatusPartiallyFilled {
		t.Errorf("Order should be PARTIALLY_FILLED, got %s", buy1.Status)
	}
	if buy1.RemainingQty != 600 {
		t.Errorf("Remaining should be 600, got %d", buy1.RemainingQty)
	}

	// Cancel the partially filled order
	result, err := ob.Cancel(&CancelOrderRequest{
		OrderID:   "buy1",
		AccountID: "acc1",
		Symbol:    "BTC-USDT",
	})
	if err != nil {
		t.Fatalf("Cancel failed: %v", err)
	}

	// Verify status change
	if len(result.OrderStatusChanges) != 1 {
		t.Fatalf("Expected 1 status change, got %d", len(result.OrderStatusChanges))
	}
	if result.OrderStatusChanges[0].OldStatus != OrderStatusPartiallyFilled {
		t.Errorf("Old status should be PARTIALLY_FILLED, got %s", result.OrderStatusChanges[0].OldStatus)
	}
	if result.OrderStatusChanges[0].NewStatus != OrderStatusCanceled {
		t.Errorf("New status should be CANCELED, got %s", result.OrderStatusChanges[0].NewStatus)
	}
	if result.OrderStatusChanges[0].RemainingQty != 600 {
		t.Errorf("Remaining should be 600, got %d", result.OrderStatusChanges[0].RemainingQty)
	}

	// Verify order is removed
	if _, exists := ob.Orders["buy1"]; exists {
		t.Errorf("Order should be removed after cancel")
	}

	// Verify price level is removed
	if _, exists := ob.BidLevels[43000]; exists {
		t.Errorf("Price level should be removed")
	}
}

// TestCancelFilledOrder tests that filled orders cannot be canceled
func TestCancelFilledOrder(t *testing.T) {
	ob := NewOrderBook("BTC-USDT")

	// Place and immediately fill order
	mustPlaceLimit(t, ob, &PlaceOrderRequest{
		OrderID:       "buy1",
		ClientOrderID: "cli_buy1",
		AccountID:     "acc1",
		Symbol:        "BTC-USDT",
		Side:          SideBuy,
		PriceInt:      43000,
		QuantityInt:   100,
	})

	mustPlaceLimit(t, ob, &PlaceOrderRequest{
		OrderID:       "sell1",
		ClientOrderID: "cli_sell1",
		AccountID:     "acc2",
		Symbol:        "BTC-USDT",
		Side:          SideSell,
		PriceInt:      43000,
		QuantityInt:   100,
	})

	// Try to cancel the filled order (it's already removed from Orders map)
	_, err := ob.Cancel(&CancelOrderRequest{
		OrderID:   "buy1",
		AccountID: "acc1",
		Symbol:    "BTC-USDT",
	})

	if err == nil {
		t.Fatalf("Expected error when canceling filled order")
	}
	if !strings.Contains(err.Error(), "already filled") {
		t.Errorf("Expected 'already filled' error, got: %v", err)
	}
}
