package account

import (
	"errors"
	"fmt"
	"testing"

	"matching-engine/internal/symbolspec"
)

func mustSpec(t *testing.T, symbol string) symbolspec.Spec {
	t.Helper()
	spec, err := symbolspec.Get(symbol)
	if err != nil {
		t.Fatalf("symbol spec not found: %v", err)
	}
	return spec
}

func mustPriceInt(t *testing.T, symbol, v string) int64 {
	t.Helper()
	spec := mustSpec(t, symbol)
	n, err := symbolspec.ParseScaledInt(v, spec.PriceScale)
	if err != nil {
		t.Fatalf("parse price failed: %v", err)
	}
	return n
}

func mustQtyInt(t *testing.T, symbol, v string) int64 {
	t.Helper()
	spec := mustSpec(t, symbol)
	n, err := symbolspec.ParseScaledInt(v, spec.QuantityScale)
	if err != nil {
		t.Fatalf("parse quantity failed: %v", err)
	}
	return n
}

func mustQuoteAmount(t *testing.T, symbol string, priceInt, qtyInt int64) int64 {
	t.Helper()
	spec := mustSpec(t, symbol)
	n, err := quoteAmountFromTrade(priceInt, qtyInt, spec.QuantityScale)
	if err != nil {
		t.Fatalf("quote amount failed: %v", err)
	}
	return n
}

func TestCheckAndFreezeForPlace_BUY(t *testing.T) {
	svc := NewMemoryService()
	symbol := "BTC-USDT"
	priceInt := mustPriceInt(t, symbol, "43000.25")
	qtyInt := mustQtyInt(t, symbol, "100.5")
	freezeAmount := mustQuoteAmount(t, symbol, priceInt, qtyInt)
	initialAvail := freezeAmount + 1_000_000

	if err := svc.SetBalance("acc1", "USDT", Balance{Available: initialAvail, Frozen: 0}); err != nil {
		t.Fatalf("SetBalance failed: %v", err)
	}

	err := svc.CheckAndFreezeForPlace(PlaceIntent{
		AccountID: "acc1",
		OrderID:   "order1",
		Symbol:    symbol,
		Side:      "BUY",
		PriceInt:  priceInt,
		QtyInt:    qtyInt,
	})
	if err != nil {
		t.Fatalf("CheckAndFreezeForPlace failed: %v", err)
	}

	balance, err := svc.GetBalance("acc1", "USDT")
	if err != nil {
		t.Fatalf("GetBalance failed: %v", err)
	}
	if balance.Frozen != freezeAmount {
		t.Errorf("Expected frozen %d, got %d", freezeAmount, balance.Frozen)
	}
	if balance.Available != initialAvail-freezeAmount {
		t.Errorf("Expected available %d, got %d", initialAvail-freezeAmount, balance.Available)
	}
}

func TestCheckAndFreezeForPlace_SELL(t *testing.T) {
	svc := NewMemoryService()
	symbol := "BTC-USDT"
	qtyInt := mustQtyInt(t, symbol, "12.345678")
	initialAvail := qtyInt + 1

	if err := svc.SetBalance("acc1", "BTC", Balance{Available: initialAvail, Frozen: 0}); err != nil {
		t.Fatalf("SetBalance failed: %v", err)
	}

	err := svc.CheckAndFreezeForPlace(PlaceIntent{
		AccountID: "acc1",
		OrderID:   "order1",
		Symbol:    symbol,
		Side:      "SELL",
		PriceInt:  mustPriceInt(t, symbol, "43000.123"),
		QtyInt:    qtyInt,
	})
	if err != nil {
		t.Fatalf("CheckAndFreezeForPlace failed: %v", err)
	}

	balance, err := svc.GetBalance("acc1", "BTC")
	if err != nil {
		t.Fatalf("GetBalance failed: %v", err)
	}
	if balance.Frozen != qtyInt {
		t.Errorf("Expected frozen %d, got %d", qtyInt, balance.Frozen)
	}
	if balance.Available != 1 {
		t.Errorf("Expected available 1, got %d", balance.Available)
	}
}

func TestCheckAndFreezeForPlace_InsufficientBalance(t *testing.T) {
	svc := NewMemoryService()
	symbol := "BTC-USDT"
	priceInt := mustPriceInt(t, symbol, "43000")
	qtyInt := mustQtyInt(t, symbol, "100")
	freezeAmount := mustQuoteAmount(t, symbol, priceInt, qtyInt)

	if err := svc.SetBalance("acc1", "USDT", Balance{Available: freezeAmount - 1, Frozen: 0}); err != nil {
		t.Fatalf("SetBalance failed: %v", err)
	}

	err := svc.CheckAndFreezeForPlace(PlaceIntent{
		AccountID: "acc1",
		OrderID:   "order1",
		Symbol:    symbol,
		Side:      "BUY",
		PriceInt:  priceInt,
		QtyInt:    qtyInt,
	})
	if err == nil {
		t.Fatalf("Expected insufficient balance error, got nil")
	}

	var insufficientErr *InsufficientBalanceError
	if !errors.As(err, &insufficientErr) {
		t.Errorf("Expected InsufficientBalanceError, got: %v", err)
	}
}

func TestReleaseOnCancel(t *testing.T) {
	svc := NewMemoryService()
	symbol := "BTC-USDT"
	priceInt := mustPriceInt(t, symbol, "42000.5")
	qtyInt := mustQtyInt(t, symbol, "2.5")
	freezeAmount := mustQuoteAmount(t, symbol, priceInt, qtyInt)
	initialAvail := freezeAmount + 10_000

	if err := svc.SetBalance("acc1", "USDT", Balance{Available: initialAvail, Frozen: 0}); err != nil {
		t.Fatalf("SetBalance failed: %v", err)
	}
	if err := svc.CheckAndFreezeForPlace(PlaceIntent{
		AccountID: "acc1",
		OrderID:   "order1",
		Symbol:    symbol,
		Side:      "BUY",
		PriceInt:  priceInt,
		QtyInt:    qtyInt,
	}); err != nil {
		t.Fatalf("freeze failed: %v", err)
	}
	if err := svc.ReleaseOnCancel(CancelIntent{
		AccountID: "acc1",
		OrderID:   "order1",
		Symbol:    symbol,
	}); err != nil {
		t.Fatalf("ReleaseOnCancel failed: %v", err)
	}

	balance, err := svc.GetBalance("acc1", "USDT")
	if err != nil {
		t.Fatalf("GetBalance failed: %v", err)
	}
	if balance.Frozen != 0 {
		t.Errorf("Expected frozen 0 after cancel, got %d", balance.Frozen)
	}
	if balance.Available != initialAvail {
		t.Errorf("Expected available %d after cancel, got %d", initialAvail, balance.Available)
	}
}

func TestReleaseOnCancel_OrderNotFound(t *testing.T) {
	svc := NewMemoryService()
	err := svc.ReleaseOnCancel(CancelIntent{
		AccountID: "acc1",
		OrderID:   "nonexistent",
		Symbol:    "BTC-USDT",
	})
	if err != nil {
		t.Errorf("ReleaseOnCancel should not error for non-existent order, got: %v", err)
	}
}

func TestConcurrentFreezeAndRelease(t *testing.T) {
	svc := NewMemoryService()
	symbol := "BTC-USDT"
	priceInt := mustPriceInt(t, symbol, "43000")
	qtyInt := mustQtyInt(t, symbol, "0.1")
	singleFreeze := mustQuoteAmount(t, symbol, priceInt, qtyInt)

	const numOps = 100
	initial := singleFreeze * numOps * 2
	if err := svc.SetBalance("acc1", "USDT", Balance{Available: initial, Frozen: 0}); err != nil {
		t.Fatalf("SetBalance failed: %v", err)
	}

	done := make(chan bool, numOps*2)
	for i := 0; i < numOps; i++ {
		go func(idx int) {
			_ = svc.CheckAndFreezeForPlace(PlaceIntent{
				AccountID: "acc1",
				OrderID:   fmt.Sprintf("order%d", idx),
				Symbol:    symbol,
				Side:      "BUY",
				PriceInt:  priceInt,
				QtyInt:    qtyInt,
			})
			done <- true
		}(i)
	}
	for i := 0; i < numOps; i++ {
		go func(idx int) {
			_ = svc.ReleaseOnCancel(CancelIntent{
				AccountID: "acc1",
				OrderID:   fmt.Sprintf("order%d", idx),
				Symbol:    symbol,
			})
			done <- true
		}(i)
	}
	for i := 0; i < numOps*2; i++ {
		<-done
	}

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
	if balance.Available+balance.Frozen != initial {
		t.Errorf("Total balance changed: expected %d, got %d", initial, balance.Available+balance.Frozen)
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
	symbol := "BTC-USDT"
	priceInt := mustPriceInt(t, symbol, "100")
	totalQty := mustQtyInt(t, symbol, "10")
	filledQty := mustQtyInt(t, symbol, "4")
	totalQuote := mustQuoteAmount(t, symbol, priceInt, totalQty)
	filledQuote := mustQuoteAmount(t, symbol, priceInt, filledQty)

	if err := svc.SetBalance("buyer", "USDT", Balance{Available: totalQuote + 5000, Frozen: 0}); err != nil {
		t.Fatalf("SetBalance buyer USDT failed: %v", err)
	}
	if err := svc.SetBalance("seller", "BTC", Balance{Available: totalQty + 10, Frozen: 0}); err != nil {
		t.Fatalf("SetBalance seller BTC failed: %v", err)
	}

	if err := svc.CheckAndFreezeForPlace(PlaceIntent{
		AccountID: "buyer",
		OrderID:   "buy-order-1",
		Symbol:    symbol,
		Side:      "BUY",
		PriceInt:  priceInt,
		QtyInt:    totalQty,
	}); err != nil {
		t.Fatalf("freeze buyer failed: %v", err)
	}
	if err := svc.CheckAndFreezeForPlace(PlaceIntent{
		AccountID: "seller",
		OrderID:   "sell-order-1",
		Symbol:    symbol,
		Side:      "SELL",
		PriceInt:  priceInt,
		QtyInt:    totalQty,
	}); err != nil {
		t.Fatalf("freeze seller failed: %v", err)
	}

	if err := svc.ApplyTrade(TradeIntent{
		TradeID:         "trd_1",
		BuyerAccountID:  "buyer",
		SellerAccountID: "seller",
		BuyerOrderID:    "buy-order-1",
		SellerOrderID:   "sell-order-1",
		Symbol:          symbol,
		PriceInt:        priceInt,
		QuantityInt:     filledQty,
	}); err != nil {
		t.Fatalf("ApplyTrade failed: %v", err)
	}

	if err := svc.ReleaseOnCancel(CancelIntent{
		AccountID: "buyer",
		OrderID:   "buy-order-1",
		Symbol:    symbol,
	}); err != nil {
		t.Fatalf("ReleaseOnCancel failed: %v", err)
	}

	buyerUSDT, _ := svc.GetBalance("buyer", "USDT")
	if buyerUSDT.Frozen != 0 {
		t.Fatalf("expected buyer frozen quote 0, got %d", buyerUSDT.Frozen)
	}
	wantBuyerQuoteAvail := (totalQuote + 5000) - filledQuote
	if buyerUSDT.Available != wantBuyerQuoteAvail {
		t.Fatalf("expected buyer available quote %d, got %d", wantBuyerQuoteAvail, buyerUSDT.Available)
	}

	buyerBTC, _ := svc.GetBalance("buyer", "BTC")
	if buyerBTC.Available != filledQty {
		t.Fatalf("expected buyer base received %d, got %d", filledQty, buyerBTC.Available)
	}
}

func TestApplyTradeIsIdempotentByTradeID(t *testing.T) {
	svc := NewMemoryService()
	symbol := "BTC-USDT"
	priceInt := mustPriceInt(t, symbol, "100")
	qtyInt := mustQtyInt(t, symbol, "5")
	filledQty := mustQtyInt(t, symbol, "1")
	totalQuote := mustQuoteAmount(t, symbol, priceInt, qtyInt)

	if err := svc.SetBalance("buyer", "USDT", Balance{Available: totalQuote + 1000, Frozen: 0}); err != nil {
		t.Fatalf("SetBalance buyer USDT failed: %v", err)
	}
	if err := svc.SetBalance("seller", "BTC", Balance{Available: qtyInt + 5, Frozen: 0}); err != nil {
		t.Fatalf("SetBalance seller BTC failed: %v", err)
	}

	if err := svc.CheckAndFreezeForPlace(PlaceIntent{
		AccountID: "buyer", OrderID: "b1", Symbol: symbol, Side: "BUY", PriceInt: priceInt, QtyInt: qtyInt,
	}); err != nil {
		t.Fatalf("freeze buyer failed: %v", err)
	}
	if err := svc.CheckAndFreezeForPlace(PlaceIntent{
		AccountID: "seller", OrderID: "s1", Symbol: symbol, Side: "SELL", PriceInt: priceInt, QtyInt: qtyInt,
	}); err != nil {
		t.Fatalf("freeze seller failed: %v", err)
	}

	trade := TradeIntent{
		TradeID:         "same-trade",
		BuyerAccountID:  "buyer",
		SellerAccountID: "seller",
		BuyerOrderID:    "b1",
		SellerOrderID:   "s1",
		Symbol:          symbol,
		PriceInt:        priceInt,
		QuantityInt:     filledQty,
	}
	if err := svc.ApplyTrade(trade); err != nil {
		t.Fatalf("first ApplyTrade failed: %v", err)
	}
	if err := svc.ApplyTrade(trade); err != nil {
		t.Fatalf("second ApplyTrade should be idempotent, got: %v", err)
	}

	buyerUSDT, _ := svc.GetBalance("buyer", "USDT")
	want := int64(1000)
	if buyerUSDT.Available != want {
		t.Fatalf("expected buyer available quote %d, got %d", want, buyerUSDT.Available)
	}
}
