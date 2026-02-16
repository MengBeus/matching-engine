package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"matching-engine/internal/account"
	"matching-engine/internal/api"
	"matching-engine/internal/engine"
	"matching-engine/internal/matching"
	"matching-engine/internal/persistence"
)

func main() {
	ctx := context.Background()

	// Initialize persistence layer
	dataDir := getenv("DATA_DIR", "./data")
	eventStore, snapshotStore, recoveryService, err := initPersistence(dataDir)
	if err != nil {
		log.Fatalf("Failed to initialize persistence: %v", err)
	}
	defer eventStore.Close()
	defer snapshotStore.Close()

	// Initialize account service
	accountSvc := account.NewMemoryService()
	// Initialize test accounts for development/testing before replaying events.
	initTestAccounts(accountSvc)

	// Initialize engine
	eng := engine.NewEngine(&engine.EngineConfig{
		ShardCount:     8,
		QueueSize:      1000,
		IdempotencyTTL: 24 * time.Hour,
	})
	defer eng.Close()

	// Set event store for persistence
	eng.SetEventStore(eventStore)

	// Set snapshot store for periodic snapshots
	eng.SetSnapshotStore(snapshotStore)

	// Perform recovery
	if err := performRecovery(ctx, eng, accountSvc, eventStore, recoveryService); err != nil {
		log.Fatalf("Failed to recover engine state: %v", err)
	}

	// Create router
	router := api.NewRouter(accountSvc, eng)
	addr := getenv("APP_ADDR", ":8080")

	log.Printf("Starting server on %s", addr)
	if err := http.ListenAndServe(addr, router); err != nil {
		log.Fatal("Failed to start server:", err)
	}
}

func initPersistence(dataDir string) (persistence.EventStore, persistence.SnapshotStore, persistence.RecoveryService, error) {
	// Create event store
	eventStore, err := persistence.NewFileEventStore(filepath.Join(dataDir, "events"))
	if err != nil {
		return nil, nil, nil, err
	}

	// Create snapshot store
	snapshotStore, err := persistence.NewFileSnapshotStore(filepath.Join(dataDir, "snapshots"))
	if err != nil {
		eventStore.Close()
		return nil, nil, nil, err
	}

	// Create recovery service
	recoveryService := persistence.NewFileRecoveryService(eventStore, snapshotStore)

	return eventStore, snapshotStore, recoveryService, nil
}

func performRecovery(
	ctx context.Context,
	eng *engine.Engine,
	accountSvc account.Service,
	eventStore persistence.EventStore,
	recoveryService persistence.RecoveryService,
) error {
	// List all symbols that have event logs
	symbols, err := eventStore.ListSymbols(ctx)
	if err != nil {
		return err
	}

	if len(symbols) == 0 {
		log.Println("No symbols to recover, starting fresh")
		return nil
	}

	log.Printf("Recovering %d symbols...", len(symbols))

	// Recover each symbol
	for _, symbol := range symbols {
		log.Printf("Recovering symbol: %s", symbol)

		// Recover snapshot and events
		snapshot, events, err := recoveryService.Recover(ctx, symbol)
		if err != nil {
			return err
		}

		// Log recovery info
		if snapshot != nil {
			log.Printf("  Loaded snapshot at sequence %d", snapshot.LastSequence)
			state, err := decodeOrderBookState(snapshot.Orderbook, symbol)
			if err != nil {
				return fmt.Errorf("failed to decode snapshot for %s: %w", symbol, err)
			}
			if err := eng.LoadSymbolSnapshot(symbol, state, snapshot.LastSequence); err != nil {
				return fmt.Errorf("failed to load snapshot for %s: %w", symbol, err)
			}
		}
		log.Printf("  Replaying %d events", len(events))

		// Replay events to rebuild state
		if err := eng.RecoverSymbol(symbol, events); err != nil {
			return err
		}

		// Recover account balances/freezes from full event history.
		allEvents, err := eventStore.ReadFrom(ctx, symbol, 1)
		if err != nil {
			return fmt.Errorf("failed to read account recovery events for %s: %w", symbol, err)
		}
		if len(allEvents) > 0 && allEvents[0].Sequence() != 1 {
			return fmt.Errorf("account recovery start sequence mismatch for %s: expected 1, got %d", symbol, allEvents[0].Sequence())
		}
		if err := recoveryService.ValidateSequence(allEvents); err != nil {
			return fmt.Errorf("account recovery sequence validation failed for %s: %w", symbol, err)
		}
		if err := replayAccountEvents(accountSvc, symbol, allEvents); err != nil {
			return fmt.Errorf("account recovery failed for %s: %w", symbol, err)
		}

		log.Printf("  Successfully recovered %s", symbol)
	}

	log.Printf("Recovery completed for %d symbols", len(symbols))
	return nil
}

func decodeOrderBookState(raw any, symbol string) (*matching.OrderBookState, error) {
	if raw == nil {
		return nil, nil
	}

	data, err := json.Marshal(raw)
	if err != nil {
		return nil, err
	}

	var state matching.OrderBookState
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, err
	}
	if state.Symbol == "" {
		state.Symbol = symbol
	}

	return &state, nil
}

func replayAccountEvents(accountSvc account.Service, symbol string, events []matching.Event) error {
	type orderMeta struct {
		accountID string
		side      matching.Side
	}

	orderLookup := make(map[string]orderMeta)
	for _, event := range events {
		switch e := event.(type) {
		case *matching.OrderAcceptedEvent:
			intent := account.PlaceIntent{
				AccountID: e.AccountID,
				OrderID:   e.OrderID,
				Symbol:    symbol,
				Side:      string(e.Side),
				PriceInt:  e.Price,
				QtyInt:    e.Quantity,
			}
			if err := accountSvc.CheckAndFreezeForPlace(intent); err != nil {
				return fmt.Errorf("freeze failed for order %s: %w", e.OrderID, err)
			}
			orderLookup[e.OrderID] = orderMeta{accountID: e.AccountID, side: e.Side}

		case *matching.OrderMatchedEvent:
			maker, ok := orderLookup[e.MakerOrderID]
			if !ok {
				return fmt.Errorf("missing maker order metadata for %s", e.MakerOrderID)
			}
			taker, ok := orderLookup[e.TakerOrderID]
			if !ok {
				return fmt.Errorf("missing taker order metadata for %s", e.TakerOrderID)
			}

			tradeIntent := account.TradeIntent{
				TradeID:     e.TradeID,
				Symbol:      symbol,
				PriceInt:    e.Price,
				QuantityInt: e.Quantity,
			}

			if maker.side == matching.SideBuy {
				tradeIntent.BuyerAccountID = maker.accountID
				tradeIntent.BuyerOrderID = e.MakerOrderID
				tradeIntent.SellerAccountID = taker.accountID
				tradeIntent.SellerOrderID = e.TakerOrderID
			} else {
				tradeIntent.BuyerAccountID = taker.accountID
				tradeIntent.BuyerOrderID = e.TakerOrderID
				tradeIntent.SellerAccountID = maker.accountID
				tradeIntent.SellerOrderID = e.MakerOrderID
			}

			if err := accountSvc.ApplyTrade(tradeIntent); err != nil {
				return fmt.Errorf("trade apply failed for %s: %w", e.TradeID, err)
			}

		case *matching.OrderCanceledEvent:
			cancelIntent := account.CancelIntent{
				AccountID: e.AccountID,
				OrderID:   e.OrderID,
				Symbol:    symbol,
			}
			if err := accountSvc.ReleaseOnCancel(cancelIntent); err != nil {
				return fmt.Errorf("cancel release failed for order %s: %w", e.OrderID, err)
			}
		default:
			return fmt.Errorf("unknown event type: %T", e)
		}
	}

	return nil
}

func initTestAccounts(accountSvc *account.MemoryService) {
	// Initialize test accounts with balance for development/testing
	// Using fixed-point decimals (8 decimal places)

	testAccounts := []struct {
		accountID string
		balances  map[string]int64
	}{
		{
			accountID: "acc-001",
			balances: map[string]int64{
				"USDT": 100000000000000, // 1,000,000 USDT (8 decimals)
				"BTC":  10000000000,     // 100 BTC (8 decimals)
			},
		},
		{
			accountID: "acc-002",
			balances: map[string]int64{
				"USDT": 100000000000000, // 1,000,000 USDT (8 decimals)
				"BTC":  10000000000,     // 100 BTC (8 decimals)
			},
		},
	}

	for _, acc := range testAccounts {
		for asset, amount := range acc.balances {
			if err := accountSvc.SetBalance(acc.accountID, asset, account.Balance{
				Available: amount,
				Frozen:    0,
			}); err != nil {
				log.Printf("Warning: Failed to initialize test account %s with %s: %v", acc.accountID, asset, err)
			}
		}
	}

	log.Println("Test accounts initialized with balance")
}

func getenv(key, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
}
