package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"matching-engine/internal/account"
	"matching-engine/internal/api"
	"matching-engine/internal/engine"
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
	if err := performRecovery(ctx, eng, eventStore, recoveryService); err != nil {
		log.Fatalf("Failed to recover engine state: %v", err)
	}

	// Initialize test accounts for development/testing
	initTestAccounts(accountSvc)

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

func performRecovery(ctx context.Context, eng *engine.Engine, eventStore persistence.EventStore, recoveryService persistence.RecoveryService) error {
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
		}
		log.Printf("  Replaying %d events", len(events))

		// Replay events to rebuild state
		if err := eng.RecoverSymbol(symbol, events); err != nil {
			return err
		}

		log.Printf("  Successfully recovered %s", symbol)
	}

	log.Printf("Recovery completed for %d symbols", len(symbols))
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
