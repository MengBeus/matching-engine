package engine

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"matching-engine/internal/matching"
)

// TestRouting tests that routing is stable and deterministic
func TestRouting(t *testing.T) {
	router := NewRouter(8)

	// Test 1: Same symbol always routes to same shard
	symbol := "BTC-USDT"
	shardID1 := router.Route(symbol)
	shardID2 := router.Route(symbol)
	shardID3 := router.Route(symbol)

	if shardID1 != shardID2 || shardID2 != shardID3 {
		t.Errorf("Same symbol should route to same shard: %d, %d, %d", shardID1, shardID2, shardID3)
	}

	// Test 2: Different symbols can route to different shards
	symbols := []string{"BTC-USDT", "ETH-USDT", "SOL-USDT", "DOGE-USDT", "ADA-USDT"}
	shardIDs := make(map[int]bool)
	for _, sym := range symbols {
		shardID := router.Route(sym)
		shardIDs[shardID] = true
		if shardID < 0 || shardID >= 8 {
			t.Errorf("Shard ID out of range: %d for symbol %s", shardID, sym)
		}
	}

	// At least 2 different shards should be used (probabilistic, but very likely)
	if len(shardIDs) < 2 {
		t.Logf("Warning: All symbols routed to same shard (unlikely but possible)")
	}

	// Test 3: Routing is stable across multiple calls
	for _, sym := range symbols {
		firstRoute := router.Route(sym)
		for i := 0; i < 100; i++ {
			if router.Route(sym) != firstRoute {
				t.Errorf("Routing not stable for symbol %s", sym)
			}
		}
	}
}

// TestIdempotency tests idempotency mechanism
func TestIdempotency(t *testing.T) {
	engine := NewEngine(&EngineConfig{
		ShardCount:     4,
		QueueSize:      100,
		IdempotencyTTL: 1 * time.Hour,
	})

	// Test 1: Same idempotency key + same payload = only execute once
	req1 := &matching.PlaceOrderRequest{
		OrderID:       "order1",
		ClientOrderID: "client1",
		AccountID:     "acc1",
		Symbol:        "BTC-USDT",
		Side:          matching.SideBuy,
		PriceInt:      43000,
		QuantityInt:   100,
	}

	hash1, _ := ComputePayloadHash(req1)
	envelope1 := &CommandEnvelope{
		CommandID:      "cmd1",
		CommandType:    CommandTypePlace,
		IdempotencyKey: "idem_key_1",
		Symbol:         "BTC-USDT",
		AccountID:      "acc1",
		PayloadHash:    hash1,
		Payload:        req1,
		CreatedAt:      time.Now(),
	}

	// First submission - should execute
	result1 := engine.Submit(envelope1)
	if result1.ErrorCode != ErrorCodeNone {
		t.Fatalf("First submission failed: %v", result1.Err)
	}
	if result1.Result == nil {
		t.Fatalf("First submission should return result")
	}
	if len(result1.Result.Events) == 0 {
		t.Errorf("First submission should generate events")
	}

	// Second submission - same idempotency key, same payload
	envelope2 := &CommandEnvelope{
		CommandID:      "cmd2", // Different command ID
		CommandType:    CommandTypePlace,
		IdempotencyKey: "idem_key_1", // Same idempotency key
		Symbol:         "BTC-USDT",
		AccountID:      "acc1",
		PayloadHash:    hash1, // Same payload hash
		Payload:        req1,
		CreatedAt:      time.Now(),
	}

	result2 := engine.Submit(envelope2)
	if result2.ErrorCode != ErrorCodeNone {
		t.Errorf("Second submission should succeed (cached): %v", result2.Err)
	}
	if result2.Result == nil {
		t.Errorf("Second submission should return cached result")
	}

	// Verify order was only created once (not duplicated)
	// We can't directly check the order book, but we can verify the result is identical
	if result1.Result != nil && result2.Result != nil {
		if len(result1.Result.Events) != len(result2.Result.Events) {
			t.Errorf("Cached result should match original result")
		}
	}

	// Test 2: Same idempotency key + different payload = conflict
	req3 := &matching.PlaceOrderRequest{
		OrderID:       "order1",
		ClientOrderID: "client1",
		AccountID:     "acc1",
		Symbol:        "BTC-USDT",
		Side:          matching.SideBuy,
		PriceInt:      44000, // Different price
		QuantityInt:   100,
	}

	hash3, _ := ComputePayloadHash(req3)
	envelope3 := &CommandEnvelope{
		CommandID:      "cmd3",
		CommandType:    CommandTypePlace,
		IdempotencyKey: "idem_key_1", // Same idempotency key
		Symbol:         "BTC-USDT",
		AccountID:      "acc1",
		PayloadHash:    hash3, // Different payload hash
		Payload:        req3,
		CreatedAt:      time.Now(),
	}

	result3 := engine.Submit(envelope3)
	if result3.ErrorCode != ErrorCodeDuplicateRequest {
		t.Errorf("Expected DUPLICATE_REQUEST error, got: %s", result3.ErrorCode)
	}
	if result3.Err == nil {
		t.Errorf("Expected error for conflicting idempotency key")
	}
}

// TestConcurrencyIsolation tests that different symbols don't interfere with each other
func TestConcurrencyIsolation(t *testing.T) {
	engine := NewEngine(&EngineConfig{
		ShardCount:     4,
		QueueSize:      100,
		IdempotencyTTL: 1 * time.Hour,
	})

	symbols := []string{"BTC-USDT", "ETH-USDT", "SOL-USDT", "DOGE-USDT"}
	var wg sync.WaitGroup

	// Concurrently submit orders for different symbols
	for i, symbol := range symbols {
		wg.Add(1)
		go func(idx int, sym string) {
			defer wg.Done()

			// Place buy order
			buyReq := &matching.PlaceOrderRequest{
				OrderID:       fmt.Sprintf("buy_%s_%d", sym, idx),
				ClientOrderID: fmt.Sprintf("client_buy_%d", idx),
				AccountID:     fmt.Sprintf("acc%d", idx),
				Symbol:        sym,
				Side:          matching.SideBuy,
				PriceInt:      43000,
				QuantityInt:   100,
			}

			buyHash, _ := ComputePayloadHash(buyReq)
			buyEnvelope := &CommandEnvelope{
				CommandID:      fmt.Sprintf("cmd_buy_%d", idx),
				CommandType:    CommandTypePlace,
				IdempotencyKey: fmt.Sprintf("idem_buy_%d", idx),
				Symbol:         sym,
				AccountID:      buyReq.AccountID,
				PayloadHash:    buyHash,
				Payload:        buyReq,
				CreatedAt:      time.Now(),
			}

			buyResult := engine.Submit(buyEnvelope)
			if buyResult.ErrorCode != ErrorCodeNone {
				t.Errorf("Buy order failed for %s: %v", sym, buyResult.Err)
				return
			}

			// Place sell order
			sellReq := &matching.PlaceOrderRequest{
				OrderID:       fmt.Sprintf("sell_%s_%d", sym, idx),
				ClientOrderID: fmt.Sprintf("client_sell_%d", idx),
				AccountID:     fmt.Sprintf("acc%d", idx+100),
				Symbol:        sym,
				Side:          matching.SideSell,
				PriceInt:      43000,
				QuantityInt:   50,
			}

			sellHash, _ := ComputePayloadHash(sellReq)
			sellEnvelope := &CommandEnvelope{
				CommandID:      fmt.Sprintf("cmd_sell_%d", idx),
				CommandType:    CommandTypePlace,
				IdempotencyKey: fmt.Sprintf("idem_sell_%d", idx),
				Symbol:         sym,
				AccountID:      sellReq.AccountID,
				PayloadHash:    sellHash,
				Payload:        sellReq,
				CreatedAt:      time.Now(),
			}

			sellResult := engine.Submit(sellEnvelope)
			if sellResult.ErrorCode != ErrorCodeNone {
				t.Errorf("Sell order failed for %s: %v", sym, sellResult.Err)
				return
			}

			// Verify trade occurred
			if len(sellResult.Result.Trades) != 1 {
				t.Errorf("Expected 1 trade for %s, got %d", sym, len(sellResult.Result.Trades))
			}
		}(i, symbol)
	}

	wg.Wait()
}

// TestIdempotencyScope tests that idempotency keys are scoped correctly
func TestIdempotencyScope(t *testing.T) {
	engine := NewEngine(&EngineConfig{
		ShardCount:     4,
		QueueSize:      100,
		IdempotencyTTL: 1 * time.Hour,
	})

	// Same idempotency key but different account should be allowed
	req1 := &matching.PlaceOrderRequest{
		OrderID:       "order1",
		ClientOrderID: "client1",
		AccountID:     "acc1",
		Symbol:        "BTC-USDT",
		Side:          matching.SideBuy,
		PriceInt:      43000,
		QuantityInt:   100,
	}

	hash1, _ := ComputePayloadHash(req1)
	envelope1 := &CommandEnvelope{
		CommandID:      "cmd1",
		CommandType:    CommandTypePlace,
		IdempotencyKey: "shared_key",
		Symbol:         "BTC-USDT",
		AccountID:      "acc1",
		PayloadHash:    hash1,
		Payload:        req1,
		CreatedAt:      time.Now(),
	}

	result1 := engine.Submit(envelope1)
	if result1.ErrorCode != ErrorCodeNone {
		t.Fatalf("First submission failed: %v", result1.Err)
	}

	// Same idempotency key but different account
	req2 := &matching.PlaceOrderRequest{
		OrderID:       "order2",
		ClientOrderID: "client2",
		AccountID:     "acc2", // Different account
		Symbol:        "BTC-USDT",
		Side:          matching.SideBuy,
		PriceInt:      43000,
		QuantityInt:   100,
	}

	hash2, _ := ComputePayloadHash(req2)
	envelope2 := &CommandEnvelope{
		CommandID:      "cmd2",
		CommandType:    CommandTypePlace,
		IdempotencyKey: "shared_key", // Same idempotency key
		Symbol:         "BTC-USDT",
		AccountID:      "acc2", // Different account
		PayloadHash:    hash2,
		Payload:        req2,
		CreatedAt:      time.Now(),
	}

	result2 := engine.Submit(envelope2)
	if result2.ErrorCode != ErrorCodeNone {
		t.Errorf("Second submission should succeed (different account): %v", result2.Err)
	}

	// Same idempotency key but different symbol
	req3 := &matching.PlaceOrderRequest{
		OrderID:       "order3",
		ClientOrderID: "client3",
		AccountID:     "acc1", // Same account as first
		Symbol:        "ETH-USDT", // Different symbol
		Side:          matching.SideBuy,
		PriceInt:      2000,
		QuantityInt:   100,
	}

	hash3, _ := ComputePayloadHash(req3)
	envelope3 := &CommandEnvelope{
		CommandID:      "cmd3",
		CommandType:    CommandTypePlace,
		IdempotencyKey: "shared_key", // Same idempotency key
		Symbol:         "ETH-USDT",   // Different symbol
		AccountID:      "acc1",
		PayloadHash:    hash3,
		Payload:        req3,
		CreatedAt:      time.Now(),
	}

	result3 := engine.Submit(envelope3)
	if result3.ErrorCode != ErrorCodeNone {
		t.Errorf("Third submission should succeed (different symbol): %v", result3.Err)
	}
}

// TestCancelOrder tests cancel order functionality through engine
func TestCancelOrder(t *testing.T) {
	engine := NewEngine(DefaultEngineConfig())

	// Place an order
	placeReq := &matching.PlaceOrderRequest{
		OrderID:       "order1",
		ClientOrderID: "client1",
		AccountID:     "acc1",
		Symbol:        "BTC-USDT",
		Side:          matching.SideBuy,
		PriceInt:      43000,
		QuantityInt:   100,
	}

	placeHash, _ := ComputePayloadHash(placeReq)
	placeEnvelope := &CommandEnvelope{
		CommandID:      "cmd_place",
		CommandType:    CommandTypePlace,
		IdempotencyKey: "idem_place",
		Symbol:         "BTC-USDT",
		AccountID:      "acc1",
		PayloadHash:    placeHash,
		Payload:        placeReq,
		CreatedAt:      time.Now(),
	}

	placeResult := engine.Submit(placeEnvelope)
	if placeResult.ErrorCode != ErrorCodeNone {
		t.Fatalf("Place order failed: %v", placeResult.Err)
	}

	// Cancel the order
	cancelReq := &matching.CancelOrderRequest{
		OrderID:   "order1",
		AccountID: "acc1",
		Symbol:    "BTC-USDT",
	}

	cancelHash, _ := ComputePayloadHash(cancelReq)
	cancelEnvelope := &CommandEnvelope{
		CommandID:      "cmd_cancel",
		CommandType:    CommandTypeCancel,
		IdempotencyKey: "idem_cancel",
		Symbol:         "BTC-USDT",
		AccountID:      "acc1",
		PayloadHash:    cancelHash,
		Payload:        cancelReq,
		CreatedAt:      time.Now(),
	}

	cancelResult := engine.Submit(cancelEnvelope)
	if cancelResult.ErrorCode != ErrorCodeNone {
		t.Errorf("Cancel order failed: %v", cancelResult.Err)
	}
	if cancelResult.Result == nil {
		t.Errorf("Cancel result should not be nil")
	}

	// Try to cancel again - should fail
	cancelEnvelope2 := &CommandEnvelope{
		CommandID:      "cmd_cancel2",
		CommandType:    CommandTypeCancel,
		IdempotencyKey: "idem_cancel2", // Different idempotency key
		Symbol:         "BTC-USDT",
		AccountID:      "acc1",
		PayloadHash:    cancelHash,
		Payload:        cancelReq,
		CreatedAt:      time.Now(),
	}

	cancelResult2 := engine.Submit(cancelEnvelope2)
	if cancelResult2.ErrorCode == ErrorCodeNone {
		t.Errorf("Second cancel should fail (order not found)")
	}
}

// TestMatchingAcrossEngine tests that matching works correctly through the engine
func TestMatchingAcrossEngine(t *testing.T) {
	engine := NewEngine(DefaultEngineConfig())

	// Place buy order
	buyReq := &matching.PlaceOrderRequest{
		OrderID:       "buy1",
		ClientOrderID: "client_buy1",
		AccountID:     "acc1",
		Symbol:        "BTC-USDT",
		Side:          matching.SideBuy,
		PriceInt:      43000,
		QuantityInt:   100,
	}

	buyHash, _ := ComputePayloadHash(buyReq)
	buyEnvelope := &CommandEnvelope{
		CommandID:      "cmd_buy",
		CommandType:    CommandTypePlace,
		IdempotencyKey: "idem_buy",
		Symbol:         "BTC-USDT",
		AccountID:      "acc1",
		PayloadHash:    buyHash,
		Payload:        buyReq,
		CreatedAt:      time.Now(),
	}

	buyResult := engine.Submit(buyEnvelope)
	if buyResult.ErrorCode != ErrorCodeNone {
		t.Fatalf("Buy order failed: %v", buyResult.Err)
	}
	if len(buyResult.Result.Trades) != 0 {
		t.Errorf("Buy order should not match yet")
	}

	// Place matching sell order
	sellReq := &matching.PlaceOrderRequest{
		OrderID:       "sell1",
		ClientOrderID: "client_sell1",
		AccountID:     "acc2",
		Symbol:        "BTC-USDT",
		Side:          matching.SideSell,
		PriceInt:      43000,
		QuantityInt:   50,
	}

	sellHash, _ := ComputePayloadHash(sellReq)
	sellEnvelope := &CommandEnvelope{
		CommandID:      "cmd_sell",
		CommandType:    CommandTypePlace,
		IdempotencyKey: "idem_sell",
		Symbol:         "BTC-USDT",
		AccountID:      "acc2",
		PayloadHash:    sellHash,
		Payload:        sellReq,
		CreatedAt:      time.Now(),
	}

	sellResult := engine.Submit(sellEnvelope)
	if sellResult.ErrorCode != ErrorCodeNone {
		t.Fatalf("Sell order failed: %v", sellResult.Err)
	}

	// Verify trade occurred
	if len(sellResult.Result.Trades) != 1 {
		t.Errorf("Expected 1 trade, got %d", len(sellResult.Result.Trades))
	}
	if sellResult.Result.Trades[0].Quantity != 50 {
		t.Errorf("Expected trade quantity 50, got %d", sellResult.Result.Trades[0].Quantity)
	}
	if sellResult.Result.Trades[0].MakerOrderID != "buy1" {
		t.Errorf("Expected maker order buy1, got %s", sellResult.Result.Trades[0].MakerOrderID)
	}
	if sellResult.Result.Trades[0].TakerOrderID != "sell1" {
		t.Errorf("Expected taker order sell1, got %s", sellResult.Result.Trades[0].TakerOrderID)
	}
}
