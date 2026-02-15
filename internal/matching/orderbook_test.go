package matching

import (
	"testing"
)

func mustPlaceLimit(t *testing.T, ob *OrderBook, req *PlaceOrderRequest) *CommandResult {
	t.Helper()
	result, err := ob.PlaceLimit(req)
	if err != nil {
		t.Fatalf("PlaceLimit failed: %v", err)
	}
	return result
}

// TestFIFO_SamePrice tests that orders at the same price are matched in FIFO order
func TestFIFO_SamePrice(t *testing.T) {
	ob := NewOrderBook("BTC-USDT")

	// Place three buy orders at the same price
	buyReq1 := &PlaceOrderRequest{
		OrderID:       "buy1",
		ClientOrderID: "cli_buy1",
		AccountID:     "acc1",
		Symbol:        "BTC-USDT",
		Side:          SideBuy,
		PriceInt:      43000,
		QuantityInt:   100,
	}
	result1 := mustPlaceLimit(t, ob, buyReq1)
	if len(result1.Trades) != 0 {
		t.Errorf("Expected no trades, got %d", len(result1.Trades))
	}

	buyReq2 := &PlaceOrderRequest{
		OrderID:       "buy2",
		ClientOrderID: "cli_buy2",
		AccountID:     "acc2",
		Symbol:        "BTC-USDT",
		Side:          SideBuy,
		PriceInt:      43000,
		QuantityInt:   100,
	}
	result2 := mustPlaceLimit(t, ob, buyReq2)
	if len(result2.Trades) != 0 {
		t.Errorf("Expected no trades, got %d", len(result2.Trades))
	}

	buyReq3 := &PlaceOrderRequest{
		OrderID:       "buy3",
		ClientOrderID: "cli_buy3",
		AccountID:     "acc3",
		Symbol:        "BTC-USDT",
		Side:          SideBuy,
		PriceInt:      43000,
		QuantityInt:   100,
	}
	result3 := mustPlaceLimit(t, ob, buyReq3)
	if len(result3.Trades) != 0 {
		t.Errorf("Expected no trades, got %d", len(result3.Trades))
	}

	// Place a sell order that matches all three buy orders
	sellReq := &PlaceOrderRequest{
		OrderID:       "sell1",
		ClientOrderID: "cli_sell1",
		AccountID:     "acc4",
		Symbol:        "BTC-USDT",
		Side:          SideSell,
		PriceInt:      43000,
		QuantityInt:   250,
	}
	result := mustPlaceLimit(t, ob, sellReq)

	// Should have 3 trades
	if len(result.Trades) != 3 {
		t.Fatalf("Expected 3 trades, got %d", len(result.Trades))
	}

	// Verify FIFO order on maker side: buy1 -> buy2 -> buy3
	if result.Trades[0].MakerOrderID != "buy1" {
		t.Errorf("First trade maker should be buy1, got %s", result.Trades[0].MakerOrderID)
	}
	if result.Trades[0].Quantity != 100 {
		t.Errorf("First trade quantity should be 100, got %d", result.Trades[0].Quantity)
	}

	if result.Trades[1].MakerOrderID != "buy2" {
		t.Errorf("Second trade maker should be buy2, got %s", result.Trades[1].MakerOrderID)
	}
	if result.Trades[1].Quantity != 100 {
		t.Errorf("Second trade quantity should be 100, got %d", result.Trades[1].Quantity)
	}

	if result.Trades[2].MakerOrderID != "buy3" {
		t.Errorf("Third trade maker should be buy3, got %s", result.Trades[2].MakerOrderID)
	}
	if result.Trades[2].Quantity != 50 {
		t.Errorf("Third trade quantity should be 50, got %d", result.Trades[2].Quantity)
	}

	// Verify buy1 and buy2 are fully filled
	if _, exists := ob.Orders["buy1"]; exists {
		t.Errorf("buy1 should be removed from order book")
	}
	if _, exists := ob.Orders["buy2"]; exists {
		t.Errorf("buy2 should be removed from order book")
	}

	// Verify buy3 is partially filled with 50 remaining
	buy3 := ob.Orders["buy3"]
	if buy3 == nil {
		t.Fatalf("buy3 should still be in order book")
	}
	if buy3.RemainingQty != 50 {
		t.Errorf("buy3 remaining quantity should be 50, got %d", buy3.RemainingQty)
	}
	if buy3.Status != OrderStatusPartiallyFilled {
		t.Errorf("buy3 status should be PARTIALLY_FILLED, got %s", buy3.Status)
	}
}

// TestPartialFill tests partial order filling
func TestPartialFill(t *testing.T) {
	ob := NewOrderBook("BTC-USDT")

	// Place a large buy order
	buyReq := &PlaceOrderRequest{
		OrderID:       "buy1",
		ClientOrderID: "cli_buy1",
		AccountID:     "acc1",
		Symbol:        "BTC-USDT",
		Side:          SideBuy,
		PriceInt:      43000,
		QuantityInt:   1000,
	}
	mustPlaceLimit(t, ob, buyReq)

	// Place a smaller sell order
	sellReq := &PlaceOrderRequest{
		OrderID:       "sell1",
		ClientOrderID: "cli_sell1",
		AccountID:     "acc2",
		Symbol:        "BTC-USDT",
		Side:          SideSell,
		PriceInt:      43000,
		QuantityInt:   300,
	}
	result := mustPlaceLimit(t, ob, sellReq)

	// Should have 1 trade
	if len(result.Trades) != 1 {
		t.Fatalf("Expected 1 trade, got %d", len(result.Trades))
	}

	// Verify trade quantity
	if result.Trades[0].Quantity != 300 {
		t.Errorf("Trade quantity should be 300, got %d", result.Trades[0].Quantity)
	}

	// Verify sell order is fully filled
	if _, exists := ob.Orders["sell1"]; exists {
		t.Errorf("sell1 should be removed from order book")
	}

	// Verify buy order is partially filled
	buy1 := ob.Orders["buy1"]
	if buy1 == nil {
		t.Fatalf("buy1 should still be in order book")
	}
	if buy1.RemainingQty != 700 {
		t.Errorf("buy1 remaining quantity should be 700, got %d", buy1.RemainingQty)
	}
	if buy1.Status != OrderStatusPartiallyFilled {
		t.Errorf("buy1 status should be PARTIALLY_FILLED, got %s", buy1.Status)
	}

	// Place another sell order to partially fill more
	sellReq2 := &PlaceOrderRequest{
		OrderID:       "sell2",
		ClientOrderID: "cli_sell2",
		AccountID:     "acc3",
		Symbol:        "BTC-USDT",
		Side:          SideSell,
		PriceInt:      43000,
		QuantityInt:   400,
	}
	result2 := mustPlaceLimit(t, ob, sellReq2)

	// Should have 1 trade
	if len(result2.Trades) != 1 {
		t.Fatalf("Expected 1 trade, got %d", len(result2.Trades))
	}

	// Verify buy order remaining quantity
	if buy1.RemainingQty != 300 {
		t.Errorf("buy1 remaining quantity should be 300, got %d", buy1.RemainingQty)
	}
}

// TestFullFill tests complete order filling
func TestFullFill(t *testing.T) {
	ob := NewOrderBook("BTC-USDT")

	// Place a buy order
	buyReq := &PlaceOrderRequest{
		OrderID:       "buy1",
		ClientOrderID: "cli_buy1",
		AccountID:     "acc1",
		Symbol:        "BTC-USDT",
		Side:          SideBuy,
		PriceInt:      43000,
		QuantityInt:   500,
	}
	mustPlaceLimit(t, ob, buyReq)

	// Place a sell order with exact same quantity
	sellReq := &PlaceOrderRequest{
		OrderID:       "sell1",
		ClientOrderID: "cli_sell1",
		AccountID:     "acc2",
		Symbol:        "BTC-USDT",
		Side:          SideSell,
		PriceInt:      43000,
		QuantityInt:   500,
	}
	result := mustPlaceLimit(t, ob, sellReq)

	// Should have 1 trade
	if len(result.Trades) != 1 {
		t.Fatalf("Expected 1 trade, got %d", len(result.Trades))
	}

	// Verify trade quantity
	if result.Trades[0].Quantity != 500 {
		t.Errorf("Trade quantity should be 500, got %d", result.Trades[0].Quantity)
	}

	// Verify both orders are fully filled and removed
	if _, exists := ob.Orders["buy1"]; exists {
		t.Errorf("buy1 should be removed from order book")
	}
	if _, exists := ob.Orders["sell1"]; exists {
		t.Errorf("sell1 should be removed from order book")
	}

	// Verify order book is empty
	if len(ob.Orders) != 0 {
		t.Errorf("Order book should be empty, got %d orders", len(ob.Orders))
	}
	if len(ob.BidLevels) != 0 {
		t.Errorf("Bid levels should be empty, got %d levels", len(ob.BidLevels))
	}
	if len(ob.AskLevels) != 0 {
		t.Errorf("Ask levels should be empty, got %d levels", len(ob.AskLevels))
	}
}

// TestCancel tests order cancellation
func TestCancel(t *testing.T) {
	ob := NewOrderBook("BTC-USDT")

	// Place a buy order
	buyReq := &PlaceOrderRequest{
		OrderID:       "buy1",
		ClientOrderID: "cli_buy1",
		AccountID:     "acc1",
		Symbol:        "BTC-USDT",
		Side:          SideBuy,
		PriceInt:      43000,
		QuantityInt:   500,
	}
	mustPlaceLimit(t, ob, buyReq)

	// Verify order is in order book
	if _, exists := ob.Orders["buy1"]; !exists {
		t.Fatalf("buy1 should be in order book")
	}

	// Cancel the order
	cancelReq := &CancelOrderRequest{
		OrderID:   "buy1",
		AccountID: "acc1",
		Symbol:    "BTC-USDT",
	}
	result, err := ob.Cancel(cancelReq)
	if err != nil {
		t.Fatalf("Cancel failed: %v", err)
	}

	// Verify OrderCanceled event was generated
	if len(result.Events) != 1 {
		t.Fatalf("Expected 1 event, got %d", len(result.Events))
	}
	if result.Events[0].EventType() != "OrderCanceled" {
		t.Errorf("Expected OrderCanceled event, got %s", result.Events[0].EventType())
	}

	// Verify order is removed from order book
	if _, exists := ob.Orders["buy1"]; exists {
		t.Errorf("buy1 should be removed from order book")
	}

	// Verify bid level is removed
	if len(ob.BidLevels) != 0 {
		t.Errorf("Bid levels should be empty, got %d levels", len(ob.BidLevels))
	}

	// Test canceling non-existent order
	cancelReq2 := &CancelOrderRequest{
		OrderID:   "nonexistent",
		AccountID: "acc1",
		Symbol:    "BTC-USDT",
	}
	_, err = ob.Cancel(cancelReq2)
	if err == nil {
		t.Errorf("Expected error when canceling non-existent order")
	}

	// Test canceling partially filled order
	buyReq2 := &PlaceOrderRequest{
		OrderID:       "buy2",
		ClientOrderID: "cli_buy2",
		AccountID:     "acc2",
		Symbol:        "BTC-USDT",
		Side:          SideBuy,
		PriceInt:      43000,
		QuantityInt:   1000,
	}
	mustPlaceLimit(t, ob, buyReq2)

	// Partially fill the order
	sellReq := &PlaceOrderRequest{
		OrderID:       "sell1",
		ClientOrderID: "cli_sell1",
		AccountID:     "acc3",
		Symbol:        "BTC-USDT",
		Side:          SideSell,
		PriceInt:      43000,
		QuantityInt:   300,
	}
	mustPlaceLimit(t, ob, sellReq)

	// Cancel the partially filled order
	cancelReq3 := &CancelOrderRequest{
		OrderID:   "buy2",
		AccountID: "acc2",
		Symbol:    "BTC-USDT",
	}
	result3, err := ob.Cancel(cancelReq3)
	if err != nil {
		t.Fatalf("Cancel failed: %v", err)
	}

	// Verify order is removed
	if _, exists := ob.Orders["buy2"]; exists {
		t.Errorf("buy2 should be removed from order book")
	}

	// Verify status change
	if len(result3.OrderStatusChanges) != 1 {
		t.Fatalf("Expected 1 status change, got %d", len(result3.OrderStatusChanges))
	}
	if result3.OrderStatusChanges[0].NewStatus != OrderStatusCanceled {
		t.Errorf("Expected CANCELED status, got %s", result3.OrderStatusChanges[0].NewStatus)
	}
	if result3.OrderStatusChanges[0].RemainingQty != 700 {
		t.Errorf("Expected remaining quantity 700, got %d", result3.OrderStatusChanges[0].RemainingQty)
	}
}

func TestMakerTakerWhenIncomingSell(t *testing.T) {
	ob := NewOrderBook("BTC-USDT")
	mustPlaceLimit(t, ob, &PlaceOrderRequest{
		OrderID:       "buy1",
		ClientOrderID: "cli_buy1",
		AccountID:     "acc1",
		Symbol:        "BTC-USDT",
		Side:          SideBuy,
		PriceInt:      43000,
		QuantityInt:   100,
	})

	result := mustPlaceLimit(t, ob, &PlaceOrderRequest{
		OrderID:       "sell1",
		ClientOrderID: "cli_sell1",
		AccountID:     "acc2",
		Symbol:        "BTC-USDT",
		Side:          SideSell,
		PriceInt:      43000,
		QuantityInt:   50,
	})

	if len(result.Trades) != 1 {
		t.Fatalf("Expected 1 trade, got %d", len(result.Trades))
	}
	trade := result.Trades[0]
	if trade.MakerOrderID != "buy1" || trade.MakerSide != SideBuy {
		t.Errorf("Expected maker buy1/BUY, got %s/%s", trade.MakerOrderID, trade.MakerSide)
	}
	if trade.TakerOrderID != "sell1" || trade.TakerSide != SideSell {
		t.Errorf("Expected taker sell1/SELL, got %s/%s", trade.TakerOrderID, trade.TakerSide)
	}
}

func TestPriceLevelVolumeConsistency(t *testing.T) {
	ob := NewOrderBook("BTC-USDT")
	mustPlaceLimit(t, ob, &PlaceOrderRequest{
		OrderID:       "buy1",
		ClientOrderID: "cli_buy1",
		AccountID:     "acc1",
		Symbol:        "BTC-USDT",
		Side:          SideBuy,
		PriceInt:      43000,
		QuantityInt:   100,
	})

	if ob.BidLevels[43000].Volume != 100 {
		t.Fatalf("Expected volume 100, got %d", ob.BidLevels[43000].Volume)
	}

	mustPlaceLimit(t, ob, &PlaceOrderRequest{
		OrderID:       "sell1",
		ClientOrderID: "cli_sell1",
		AccountID:     "acc2",
		Symbol:        "BTC-USDT",
		Side:          SideSell,
		PriceInt:      43000,
		QuantityInt:   40,
	})

	if ob.BidLevels[43000].Volume != 60 {
		t.Fatalf("Expected volume 60 after partial fill, got %d", ob.BidLevels[43000].Volume)
	}
}

func TestRejectDuplicateOrderIDAndWrongSymbol(t *testing.T) {
	ob := NewOrderBook("BTC-USDT")
	mustPlaceLimit(t, ob, &PlaceOrderRequest{
		OrderID:       "dup",
		ClientOrderID: "cli_1",
		AccountID:     "acc1",
		Symbol:        "BTC-USDT",
		Side:          SideBuy,
		PriceInt:      43000,
		QuantityInt:   10,
	})

	if _, err := ob.PlaceLimit(&PlaceOrderRequest{
		OrderID:       "dup",
		ClientOrderID: "cli_2",
		AccountID:     "acc1",
		Symbol:        "BTC-USDT",
		Side:          SideBuy,
		PriceInt:      43000,
		QuantityInt:   10,
	}); err == nil {
		t.Fatalf("Expected duplicate order_id error")
	}

	if _, err := ob.PlaceLimit(&PlaceOrderRequest{
		OrderID:       "wrong-symbol",
		ClientOrderID: "cli_3",
		AccountID:     "acc1",
		Symbol:        "ETH-USDT",
		Side:          SideBuy,
		PriceInt:      43000,
		QuantityInt:   10,
	}); err == nil {
		t.Fatalf("Expected symbol mismatch error")
	}
}
