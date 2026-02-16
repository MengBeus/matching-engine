package projection

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestMemoryOrderRepository_SaveNil(t *testing.T) {
	repo := NewMemoryOrderRepository()
	if err := repo.Save(context.Background(), nil); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("expected ErrInvalidArgument, got %v", err)
	}
}

func TestMemoryTradeRepository_SaveNil(t *testing.T) {
	repo := NewMemoryTradeRepository()
	if err := repo.Save(context.Background(), nil); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("expected ErrInvalidArgument, got %v", err)
	}
}

func TestMemoryTradeRepository_SaveIdempotent(t *testing.T) {
	repo := NewMemoryTradeRepository()
	now := time.Now().UTC()
	trade := &TradeView{
		TradeID:        "trd-1",
		Symbol:         "BTC-USDT",
		MakerOrderID:   "ord-m",
		TakerOrderID:   "ord-t",
		MakerAccountID: "acc-m",
		TakerAccountID: "acc-t",
		Price:          100,
		Quantity:       2,
		OccurredAt:     now,
		Sequence:       10,
	}

	if err := repo.Save(context.Background(), trade); err != nil {
		t.Fatalf("first save failed: %v", err)
	}
	if err := repo.Save(context.Background(), trade); err != nil {
		t.Fatalf("idempotent save failed: %v", err)
	}

	list, err := repo.ListBySymbol(context.Background(), "BTC-USDT", 0, 100)
	if err != nil {
		t.Fatalf("list failed: %v", err)
	}
	if len(list) != 1 {
		t.Fatalf("expected 1 trade after duplicate save, got %d", len(list))
	}
}

func TestMemoryTradeRepository_SaveConflict(t *testing.T) {
	repo := NewMemoryTradeRepository()
	now := time.Now().UTC()
	base := &TradeView{
		TradeID:        "trd-1",
		Symbol:         "BTC-USDT",
		MakerOrderID:   "ord-m",
		TakerOrderID:   "ord-t",
		MakerAccountID: "acc-m",
		TakerAccountID: "acc-t",
		Price:          100,
		Quantity:       2,
		OccurredAt:     now,
		Sequence:       10,
	}
	conflict := &TradeView{
		TradeID:        "trd-1",
		Symbol:         "BTC-USDT",
		MakerOrderID:   "ord-m",
		TakerOrderID:   "ord-t",
		MakerAccountID: "acc-m",
		TakerAccountID: "acc-t",
		Price:          100,
		Quantity:       3, // different
		OccurredAt:     now,
		Sequence:       10,
	}

	if err := repo.Save(context.Background(), base); err != nil {
		t.Fatalf("first save failed: %v", err)
	}
	err := repo.Save(context.Background(), conflict)
	if !errors.Is(err, ErrTradeConflict) {
		t.Fatalf("expected ErrTradeConflict, got %v", err)
	}
}

func TestMemoryRepositories_ReturnCopies(t *testing.T) {
	ctx := context.Background()

	orderRepo := NewMemoryOrderRepository()
	order := &OrderView{
		OrderID:       "ord-1",
		ClientOrderID: "cli-1",
		AccountID:     "acc-1",
		Symbol:        "BTC-USDT",
		Price:         100,
		Quantity:      5,
		RemainingQty:  5,
		FilledQty:     0,
		Status:        OrderStatusNew,
	}
	if err := orderRepo.Save(ctx, order); err != nil {
		t.Fatalf("save order failed: %v", err)
	}

	got, err := orderRepo.GetByID(ctx, "ord-1")
	if err != nil {
		t.Fatalf("get order failed: %v", err)
	}
	got.Status = OrderStatusCanceled
	gotAgain, err := orderRepo.GetByID(ctx, "ord-1")
	if err != nil {
		t.Fatalf("get order again failed: %v", err)
	}
	if gotAgain.Status != OrderStatusNew {
		t.Fatalf("repository leaked internal pointer for order status, got %s", gotAgain.Status)
	}

	list, err := orderRepo.ListByAccount(ctx, "acc-1", 10)
	if err != nil {
		t.Fatalf("list orders failed: %v", err)
	}
	list[0].RemainingQty = 0
	gotAgain, err = orderRepo.GetByID(ctx, "ord-1")
	if err != nil {
		t.Fatalf("get order after list mutation failed: %v", err)
	}
	if gotAgain.RemainingQty != 5 {
		t.Fatalf("repository leaked list element pointer for order remaining_qty, got %d", gotAgain.RemainingQty)
	}

	tradeRepo := NewMemoryTradeRepository()
	trade := &TradeView{
		TradeID:        "trd-1",
		Symbol:         "BTC-USDT",
		MakerOrderID:   "ord-1",
		TakerOrderID:   "ord-2",
		MakerAccountID: "acc-1",
		TakerAccountID: "acc-2",
		Price:          100,
		Quantity:       1,
		OccurredAt:     time.Now().UTC(),
		Sequence:       1,
	}
	if err := tradeRepo.Save(ctx, trade); err != nil {
		t.Fatalf("save trade failed: %v", err)
	}
	gotTrade, err := tradeRepo.GetByID(ctx, "trd-1")
	if err != nil {
		t.Fatalf("get trade failed: %v", err)
	}
	gotTrade.Quantity = 999
	gotTradeAgain, err := tradeRepo.GetByID(ctx, "trd-1")
	if err != nil {
		t.Fatalf("get trade again failed: %v", err)
	}
	if gotTradeAgain.Quantity != 1 {
		t.Fatalf("repository leaked internal pointer for trade quantity, got %d", gotTradeAgain.Quantity)
	}
}

func TestMemoryRepositories_SetLastSequenceMonotonic(t *testing.T) {
	ctx := context.Background()

	orderRepo := NewMemoryOrderRepository()
	if err := orderRepo.SetLastSequence(ctx, "BTC-USDT", 10); err != nil {
		t.Fatalf("set last sequence failed: %v", err)
	}
	if err := orderRepo.SetLastSequence(ctx, "BTC-USDT", 9); !errors.Is(err, ErrSequenceRegression) {
		t.Fatalf("expected ErrSequenceRegression for order repo, got %v", err)
	}

	tradeRepo := NewMemoryTradeRepository()
	if err := tradeRepo.SetLastSequence(ctx, "BTC-USDT", 10); err != nil {
		t.Fatalf("set last sequence failed: %v", err)
	}
	if err := tradeRepo.SetLastSequence(ctx, "BTC-USDT", 9); !errors.Is(err, ErrSequenceRegression) {
		t.Fatalf("expected ErrSequenceRegression for trade repo, got %v", err)
	}
}

func TestMemoryTradeRepository_ListBySymbolSortedBySequence(t *testing.T) {
	ctx := context.Background()
	repo := NewMemoryTradeRepository()
	now := time.Now().UTC()

	t2 := &TradeView{
		TradeID:        "trd-2",
		Symbol:         "BTC-USDT",
		MakerOrderID:   "ord-m2",
		TakerOrderID:   "ord-t2",
		MakerAccountID: "acc-m2",
		TakerAccountID: "acc-t2",
		Price:          100,
		Quantity:       1,
		OccurredAt:     now,
		Sequence:       2,
	}
	t1 := &TradeView{
		TradeID:        "trd-1",
		Symbol:         "BTC-USDT",
		MakerOrderID:   "ord-m1",
		TakerOrderID:   "ord-t1",
		MakerAccountID: "acc-m1",
		TakerAccountID: "acc-t1",
		Price:          99,
		Quantity:       1,
		OccurredAt:     now,
		Sequence:       1,
	}

	if err := repo.Save(ctx, t2); err != nil {
		t.Fatalf("save t2 failed: %v", err)
	}
	if err := repo.Save(ctx, t1); err != nil {
		t.Fatalf("save t1 failed: %v", err)
	}

	got, err := repo.ListBySymbol(ctx, "BTC-USDT", 0, 10)
	if err != nil {
		t.Fatalf("list by symbol failed: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 trades, got %d", len(got))
	}
	if got[0].Sequence != 1 || got[1].Sequence != 2 {
		t.Fatalf("expected sorted sequences [1,2], got [%d,%d]", got[0].Sequence, got[1].Sequence)
	}
}
