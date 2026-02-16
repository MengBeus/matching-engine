package persistence

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func TestFileSnapshotStore_SaveAndLoad(t *testing.T) {
	tempDir := t.TempDir()
	store, err := NewFileSnapshotStore(filepath.Join(tempDir, "snapshots"))
	if err != nil {
		t.Fatalf("failed to create snapshot store: %v", err)
	}
	defer store.Close()

	ctx := context.Background()
	symbol := "BTC-USDT"

	// Create test snapshot
	snapshot := &Snapshot{
		Version:      1,
		Symbol:       symbol,
		LastSequence: 100,
		CapturedAt:   time.Now(),
		Orderbook: map[string]any{
			"bids": []any{},
			"asks": []any{},
		},
		ClosedOrders: map[string]any{
			"order-1": map[string]any{"status": "FILLED"},
		},
		AccountBalances: map[string]any{
			"acc-1": map[string]any{"USDT": 10000},
		},
	}

	// Test: Save snapshot
	if err := store.Save(ctx, snapshot); err != nil {
		t.Fatalf("failed to save snapshot: %v", err)
	}

	// Test: Load snapshot
	loaded, err := store.Load(ctx, symbol)
	if err != nil {
		t.Fatalf("failed to load snapshot: %v", err)
	}

	if loaded == nil {
		t.Fatal("expected snapshot, got nil")
	}

	if loaded.Symbol != symbol {
		t.Errorf("expected symbol %s, got %s", symbol, loaded.Symbol)
	}
	if loaded.LastSequence != 100 {
		t.Errorf("expected last sequence 100, got %d", loaded.LastSequence)
	}
	if loaded.Version != 1 {
		t.Errorf("expected version 1, got %d", loaded.Version)
	}
}

func TestFileSnapshotStore_LoadLatest(t *testing.T) {
	tempDir := t.TempDir()
	store, err := NewFileSnapshotStore(filepath.Join(tempDir, "snapshots"))
	if err != nil {
		t.Fatalf("failed to create snapshot store: %v", err)
	}
	defer store.Close()

	ctx := context.Background()
	symbol := "BTC-USDT"

	// Save multiple snapshots with different sequences
	sequences := []int64{50, 100, 75, 120}
	for _, seq := range sequences {
		snapshot := &Snapshot{
			Version:      1,
			Symbol:       symbol,
			LastSequence: seq,
			CapturedAt:   time.Now(),
			Orderbook:    map[string]any{},
		}
		if err := store.Save(ctx, snapshot); err != nil {
			t.Fatalf("failed to save snapshot seq=%d: %v", seq, err)
		}
		time.Sleep(10 * time.Millisecond) // Ensure different timestamps
	}

	// Test: Load should return the latest (seq=120)
	loaded, err := store.Load(ctx, symbol)
	if err != nil {
		t.Fatalf("failed to load snapshot: %v", err)
	}

	if loaded == nil {
		t.Fatal("expected snapshot, got nil")
	}

	if loaded.LastSequence != 120 {
		t.Errorf("expected latest sequence 120, got %d", loaded.LastSequence)
	}
}

func TestFileSnapshotStore_ListSnapshots(t *testing.T) {
	tempDir := t.TempDir()
	store, err := NewFileSnapshotStore(filepath.Join(tempDir, "snapshots"))
	if err != nil {
		t.Fatalf("failed to create snapshot store: %v", err)
	}
	defer store.Close()

	ctx := context.Background()
	symbol := "BTC-USDT"

	// Test: Empty list
	snapshots, err := store.ListSnapshots(ctx, symbol)
	if err != nil {
		t.Fatalf("failed to list snapshots: %v", err)
	}
	if len(snapshots) != 0 {
		t.Errorf("expected 0 snapshots, got %d", len(snapshots))
	}

	// Save multiple snapshots
	sequences := []int64{50, 100, 75}
	for _, seq := range sequences {
		snapshot := &Snapshot{
			Version:      1,
			Symbol:       symbol,
			LastSequence: seq,
			CapturedAt:   time.Now(),
			Orderbook:    map[string]any{},
		}
		if err := store.Save(ctx, snapshot); err != nil {
			t.Fatalf("failed to save snapshot seq=%d: %v", seq, err)
		}
	}

	// Test: List should return 3 snapshots sorted by sequence desc
	snapshots, err = store.ListSnapshots(ctx, symbol)
	if err != nil {
		t.Fatalf("failed to list snapshots: %v", err)
	}

	if len(snapshots) != 3 {
		t.Fatalf("expected 3 snapshots, got %d", len(snapshots))
	}

	// Verify sorted descending
	if snapshots[0].LastSequence != 100 {
		t.Errorf("expected first snapshot seq=100, got %d", snapshots[0].LastSequence)
	}
	if snapshots[1].LastSequence != 75 {
		t.Errorf("expected second snapshot seq=75, got %d", snapshots[1].LastSequence)
	}
	if snapshots[2].LastSequence != 50 {
		t.Errorf("expected third snapshot seq=50, got %d", snapshots[2].LastSequence)
	}
}

func TestFileSnapshotStore_NoSnapshot(t *testing.T) {
	tempDir := t.TempDir()
	store, err := NewFileSnapshotStore(filepath.Join(tempDir, "snapshots"))
	if err != nil {
		t.Fatalf("failed to create snapshot store: %v", err)
	}
	defer store.Close()

	ctx := context.Background()

	// Test: Load non-existent symbol should return nil
	loaded, err := store.Load(ctx, "NON-EXISTENT")
	if err != nil {
		t.Fatalf("failed to load snapshot: %v", err)
	}

	if loaded != nil {
		t.Errorf("expected nil snapshot, got %v", loaded)
	}
}
