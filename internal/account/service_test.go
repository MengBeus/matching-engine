package account

import (
	"errors"
	"fmt"
	"testing"
)

func TestCheckAndFreezeForPlace_BUY(t *testing.T) {
	svc := NewMemoryService()

	// Initialize account with USDT balance
	err := svc.SetBalance("acc1", "USDT", Balance{Available: 10000000, Frozen: 0})
	if err != nil {
		t.Fatalf("SetBalance failed: %v", err)
	}

	// Place BUY order: BTC-USDT at price 43000, quantity 100
	intent := PlaceIntent{
		AccountID: "acc1",
		OrderID:   "order1",
		Symbol:    "BTC-USDT",
		Side:      "BUY",
		PriceInt:  43000,
		QtyInt:    100,
	}

	err = svc.CheckAndFreezeForPlace(intent)
	if err != nil {
		t.Fatalf("CheckAndFreezeForPlace failed: %v", err)
	}

	// Verify USDT balance: should freeze 43000 * 100 = 4,300,000
	balance, err := svc.GetBalance("acc1", "USDT")
	if err != nil {
		t.Fatalf("GetBalance failed: %v", err)
	}

	expectedFrozen := int64(4300000)
	expectedAvailable := int64(10000000 - 4300000)

	if balance.Frozen != expectedFrozen {
		t.Errorf("Expected frozen %d, got %d", expectedFrozen, balance.Frozen)
	}
	if balance.Available != expectedAvailable {
		t.Errorf("Expected available %d, got %d", expectedAvailable, balance.Available)
	}
}

func TestCheckAndFreezeForPlace_SELL(t *testing.T) {
	svc := NewMemoryService()

	// Initialize account with BTC balance
	err := svc.SetBalance("acc1", "BTC", Balance{Available: 1000, Frozen: 0})
	if err != nil {
		t.Fatalf("SetBalance failed: %v", err)
	}

	// Place SELL order: BTC-USDT, quantity 100
	intent := PlaceIntent{
		AccountID: "acc1",
		OrderID:   "order1",
		Symbol:    "BTC-USDT",
		Side:      "SELL",
		PriceInt:  43000,
		QtyInt:    100,
	}

	err = svc.CheckAndFreezeForPlace(intent)
	if err != nil {
		t.Fatalf("CheckAndFreezeForPlace failed: %v", err)
	}

	// Verify BTC balance: should freeze 100
	balance, err := svc.GetBalance("acc1", "BTC")
	if err != nil {
		t.Fatalf("GetBalance failed: %v", err)
	}

	expectedFrozen := int64(100)
	expectedAvailable := int64(900)

	if balance.Frozen != expectedFrozen {
		t.Errorf("Expected frozen %d, got %d", expectedFrozen, balance.Frozen)
	}
	if balance.Available != expectedAvailable {
		t.Errorf("Expected available %d, got %d", expectedAvailable, balance.Available)
	}
}

func TestCheckAndFreezeForPlace_InsufficientBalance(t *testing.T) {
	svc := NewMemoryService()

	// Initialize account with insufficient USDT balance
	err := svc.SetBalance("acc1", "USDT", Balance{Available: 1000000, Frozen: 0})
	if err != nil {
		t.Fatalf("SetBalance failed: %v", err)
	}

	// Try to place BUY order that requires more than available
	intent := PlaceIntent{
		AccountID: "acc1",
		OrderID:   "order1",
		Symbol:    "BTC-USDT",
		Side:      "BUY",
		PriceInt:  43000,
		QtyInt:    100, // Requires 4,300,000 but only have 1,000,000
	}

	err = svc.CheckAndFreezeForPlace(intent)
	if err == nil {
		t.Fatalf("Expected insufficient balance error, got nil")
	}

	// Verify it's an InsufficientBalanceError
	var insufficientErr *InsufficientBalanceError
	if !errors.As(err, &insufficientErr) {
		t.Errorf("Expected InsufficientBalanceError, got: %v", err)
	}

	// Verify balance unchanged
	balance, err := svc.GetBalance("acc1", "USDT")
	if err != nil {
		t.Fatalf("GetBalance failed: %v", err)
	}

	if balance.Frozen != 0 {
		t.Errorf("Expected frozen 0, got %d", balance.Frozen)
	}
	if balance.Available != 1000000 {
		t.Errorf("Expected available 1000000, got %d", balance.Available)
	}
}

func TestReleaseOnCancel(t *testing.T) {
	svc := NewMemoryService()

	// Initialize account with USDT balance
	err := svc.SetBalance("acc1", "USDT", Balance{Available: 10000000, Frozen: 0})
	if err != nil {
		t.Fatalf("SetBalance failed: %v", err)
	}

	// Place BUY order
	placeIntent := PlaceIntent{
		AccountID: "acc1",
		OrderID:   "order1",
		Symbol:    "BTC-USDT",
		Side:      "BUY",
		PriceInt:  43000,
		QtyInt:    100,
	}

	err = svc.CheckAndFreezeForPlace(placeIntent)
	if err != nil {
		t.Fatalf("CheckAndFreezeForPlace failed: %v", err)
	}

	// Verify funds are frozen
	balanceAfterPlace, _ := svc.GetBalance("acc1", "USDT")
	if balanceAfterPlace.Frozen != 4300000 {
		t.Errorf("Expected frozen 4300000, got %d", balanceAfterPlace.Frozen)
	}

	// Cancel the order
	cancelIntent := CancelIntent{
		AccountID: "acc1",
		OrderID:   "order1",
		Symbol:    "BTC-USDT",
	}

	err = svc.ReleaseOnCancel(cancelIntent)
	if err != nil {
		t.Fatalf("ReleaseOnCancel failed: %v", err)
	}

	// Verify funds are unfrozen
	balanceAfterCancel, err := svc.GetBalance("acc1", "USDT")
	if err != nil {
		t.Fatalf("GetBalance failed: %v", err)
	}

	if balanceAfterCancel.Frozen != 0 {
		t.Errorf("Expected frozen 0 after cancel, got %d", balanceAfterCancel.Frozen)
	}
	if balanceAfterCancel.Available != 10000000 {
		t.Errorf("Expected available 10000000 after cancel, got %d", balanceAfterCancel.Available)
	}
}

func TestReleaseOnCancel_OrderNotFound(t *testing.T) {
	svc := NewMemoryService()

	// Try to cancel non-existent order - should not error
	cancelIntent := CancelIntent{
		AccountID: "acc1",
		OrderID:   "nonexistent",
		Symbol:    "BTC-USDT",
	}

	err := svc.ReleaseOnCancel(cancelIntent)
	if err != nil {
		t.Errorf("ReleaseOnCancel should not error for non-existent order, got: %v", err)
	}
}

func TestConcurrentFreezeAndRelease(t *testing.T) {
	svc := NewMemoryService()

	// Initialize account with large balance
	err := svc.SetBalance("acc1", "USDT", Balance{Available: 100000000, Frozen: 0})
	if err != nil {
		t.Fatalf("SetBalance failed: %v", err)
	}

	// Concurrently freeze and release funds
	const numOps = 100
	done := make(chan bool, numOps*2)

	// Freeze operations
	for i := 0; i < numOps; i++ {
		go func(idx int) {
			intent := PlaceIntent{
				AccountID: "acc1",
				OrderID:   fmt.Sprintf("order%d", idx),
				Symbol:    "BTC-USDT",
				Side:      "BUY",
				PriceInt:  43000,
				QtyInt:    10,
			}
			_ = svc.CheckAndFreezeForPlace(intent)
			done <- true
		}(i)
	}

	// Release operations
	for i := 0; i < numOps; i++ {
		go func(idx int) {
			intent := CancelIntent{
				AccountID: "acc1",
				OrderID:   fmt.Sprintf("order%d", idx),
				Symbol:    "BTC-USDT",
			}
			_ = svc.ReleaseOnCancel(intent)
			done <- true
		}(i)
	}

	// Wait for all operations
	for i := 0; i < numOps*2; i++ {
		<-done
	}

	// Verify no negative balance
	balance, err := svc.GetBalance("acc1", "USDT")
	if err != nil {
		t.Fatalf("GetBalance failed: %v", err)
	}

	if balance.Available < 0 {
		t.Errorf("Negative available balance: %d", balance.Available)
	}
	if balance.Frozen < 0 {
		t.Errorf("Negative frozen balance: %d", balance.Frozen)
	}

	total := balance.Available + balance.Frozen
	if total != 100000000 {
		t.Errorf("Total balance changed: expected 100000000, got %d", total)
	}
}

func TestParseSymbol(t *testing.T) {
	tests := []struct {
		symbol    string
		wantBase  string
		wantQuote string
		wantErr   bool
	}{
		{"BTC-USDT", "BTC", "USDT", false},
		{"ETH-USDT", "ETH", "USDT", false},
		{"SOL-USDT", "SOL", "USDT", false},
		{"INVALID", "", "", true},
		{"BTC", "", "", true},
		{"BTC-USDT-EXTRA", "", "", true},
	}

	for _, tt := range tests {
		base, quote, err := ParseSymbol(tt.symbol)
		if tt.wantErr {
			if err == nil {
				t.Errorf("ParseSymbol(%s) expected error, got nil", tt.symbol)
			}
		} else {
			if err != nil {
				t.Errorf("ParseSymbol(%s) unexpected error: %v", tt.symbol, err)
			}
			if base != tt.wantBase {
				t.Errorf("ParseSymbol(%s) base = %s, want %s", tt.symbol, base, tt.wantBase)
			}
			if quote != tt.wantQuote {
				t.Errorf("ParseSymbol(%s) quote = %s, want %s", tt.symbol, quote, tt.wantQuote)
			}
		}
	}
}

func TestApplyTradeAndReleaseRemainingOnCancel(t *testing.T) {
	svc := NewMemoryService()

	if err := svc.SetBalance("buyer", "USDT", Balance{Available: 10000, Frozen: 0}); err != nil {
		t.Fatalf("SetBalance buyer USDT failed: %v", err)
	}
	if err := svc.SetBalance("seller", "BTC", Balance{Available: 10, Frozen: 0}); err != nil {
		t.Fatalf("SetBalance seller BTC failed: %v", err)
	}

	// buyer freezes 100 * 10 = 1000 USDT
	if err := svc.CheckAndFreezeForPlace(PlaceIntent{
		AccountID: "buyer",
		OrderID:   "buy-order-1",
		Symbol:    "BTC-USDT",
		Side:      "BUY",
		PriceInt:  100,
		QtyInt:    10,
	}); err != nil {
		t.Fatalf("freeze buyer failed: %v", err)
	}

	// seller freezes 10 BTC
	if err := svc.CheckAndFreezeForPlace(PlaceIntent{
		AccountID: "seller",
		OrderID:   "sell-order-1",
		Symbol:    "BTC-USDT",
		Side:      "SELL",
		PriceInt:  100,
		QtyInt:    10,
	}); err != nil {
		t.Fatalf("freeze seller failed: %v", err)
	}

	// Match 4 units at price 100.
	if err := svc.ApplyTrade(TradeIntent{
		TradeID:         "trd_1",
		BuyerAccountID:  "buyer",
		SellerAccountID: "seller",
		BuyerOrderID:    "buy-order-1",
		SellerOrderID:   "sell-order-1",
		Symbol:          "BTC-USDT",
		PriceInt:        100,
		QuantityInt:     4,
	}); err != nil {
		t.Fatalf("ApplyTrade failed: %v", err)
	}

	// Buyer cancel should release only remaining frozen amount (600), not full 1000.
	if err := svc.ReleaseOnCancel(CancelIntent{
		AccountID: "buyer",
		OrderID:   "buy-order-1",
		Symbol:    "BTC-USDT",
	}); err != nil {
		t.Fatalf("ReleaseOnCancel failed: %v", err)
	}

	buyerUSDT, _ := svc.GetBalance("buyer", "USDT")
	if buyerUSDT.Frozen != 0 {
		t.Fatalf("expected buyer frozen quote 0, got %d", buyerUSDT.Frozen)
	}
	// Initial 10000 - executed quote 400 = 9600
	if buyerUSDT.Available != 9600 {
		t.Fatalf("expected buyer available quote 9600, got %d", buyerUSDT.Available)
	}

	buyerBTC, _ := svc.GetBalance("buyer", "BTC")
	if buyerBTC.Available != 4 {
		t.Fatalf("expected buyer base received 4, got %d", buyerBTC.Available)
	}
}

func TestApplyTradeIsIdempotentByTradeID(t *testing.T) {
	svc := NewMemoryService()

	if err := svc.SetBalance("buyer", "USDT", Balance{Available: 1000, Frozen: 0}); err != nil {
		t.Fatalf("SetBalance buyer USDT failed: %v", err)
	}
	if err := svc.SetBalance("seller", "BTC", Balance{Available: 5, Frozen: 0}); err != nil {
		t.Fatalf("SetBalance seller BTC failed: %v", err)
	}

	if err := svc.CheckAndFreezeForPlace(PlaceIntent{
		AccountID: "buyer", OrderID: "b1", Symbol: "BTC-USDT", Side: "BUY", PriceInt: 100, QtyInt: 5,
	}); err != nil {
		t.Fatalf("freeze buyer failed: %v", err)
	}
	if err := svc.CheckAndFreezeForPlace(PlaceIntent{
		AccountID: "seller", OrderID: "s1", Symbol: "BTC-USDT", Side: "SELL", PriceInt: 100, QtyInt: 5,
	}); err != nil {
		t.Fatalf("freeze seller failed: %v", err)
	}

	trade := TradeIntent{
		TradeID:         "same-trade",
		BuyerAccountID:  "buyer",
		SellerAccountID: "seller",
		BuyerOrderID:    "b1",
		SellerOrderID:   "s1",
		Symbol:          "BTC-USDT",
		PriceInt:        100,
		QuantityInt:     1,
	}
	if err := svc.ApplyTrade(trade); err != nil {
		t.Fatalf("first ApplyTrade failed: %v", err)
	}
	if err := svc.ApplyTrade(trade); err != nil {
		t.Fatalf("second ApplyTrade should be idempotent, got: %v", err)
	}

	buyerUSDT, _ := svc.GetBalance("buyer", "USDT")
	if buyerUSDT.Available != 500 {
		t.Fatalf("expected buyer available quote 500, got %d", buyerUSDT.Available)
	}
}
