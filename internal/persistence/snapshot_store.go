package persistence

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
)

// FileSnapshotStore implements SnapshotStore using JSON files
type FileSnapshotStore struct {
	baseDir string
	mu      sync.RWMutex
}

// NewFileSnapshotStore creates a new file-based snapshot store
func NewFileSnapshotStore(baseDir string) (*FileSnapshotStore, error) {
	if err := os.MkdirAll(baseDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create base directory: %w", err)
	}

	return &FileSnapshotStore{
		baseDir: baseDir,
	}, nil
}

// Save saves a snapshot for a specific symbol
func (s *FileSnapshotStore) Save(ctx context.Context, snapshot any) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Extract symbol and last_sequence from snapshot using reflection
	// This allows us to accept any snapshot type with these fields
	symbol, lastSeq, err := extractSnapshotMetadata(snapshot)
	if err != nil {
		return fmt.Errorf("failed to extract snapshot metadata: %w", err)
	}

	// Create symbol directory
	symbolDir := filepath.Join(s.baseDir, symbol)
	if err := os.MkdirAll(symbolDir, 0755); err != nil {
		return fmt.Errorf("failed to create symbol directory: %w", err)
	}

	// Build snapshot filename: snapshot-<last_seq>.json
	filename := fmt.Sprintf("snapshot-%d.json", lastSeq)
	filePath := filepath.Join(symbolDir, filename)

	// Marshal snapshot to JSON
	data, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal snapshot: %w", err)
	}

	// Write to temporary file first
	tempPath := filePath + ".tmp"
	if err := os.WriteFile(tempPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write snapshot file: %w", err)
	}

	// Atomic rename
	if err := os.Rename(tempPath, filePath); err != nil {
		os.Remove(tempPath) // Clean up temp file
		return fmt.Errorf("failed to rename snapshot file: %w", err)
	}

	return nil
}

// extractSnapshotMetadata extracts symbol and last_sequence from a snapshot using reflection
func extractSnapshotMetadata(snapshot any) (string, int64, error) {
	// Try type assertion first for common types
	type snapshotMetadata interface {
		GetSymbol() string
		GetLastSequence() int64
	}

	// If snapshot implements the interface, use it
	if sm, ok := snapshot.(snapshotMetadata); ok {
		return sm.GetSymbol(), sm.GetLastSequence(), nil
	}

	// Otherwise, use reflection to extract fields
	v := reflect.ValueOf(snapshot)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	if v.Kind() != reflect.Struct {
		return "", 0, fmt.Errorf("snapshot must be a struct, got %T", snapshot)
	}

	// Extract Symbol field
	symbolField := v.FieldByName("Symbol")
	if !symbolField.IsValid() || symbolField.Kind() != reflect.String {
		return "", 0, fmt.Errorf("snapshot must have a Symbol field of type string")
	}
	symbol := symbolField.String()

	// Extract LastSequence field
	lastSeqField := v.FieldByName("LastSequence")
	if !lastSeqField.IsValid() || lastSeqField.Kind() != reflect.Int64 {
		return "", 0, fmt.Errorf("snapshot must have a LastSequence field of type int64")
	}
	lastSeq := lastSeqField.Int()

	return symbol, lastSeq, nil
}

// Load loads the latest snapshot for a specific symbol
func (s *FileSnapshotStore) Load(ctx context.Context, symbol string) (*Snapshot, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// List all snapshots
	snapshots, err := s.listSnapshotsInternal(symbol)
	if err != nil {
		return nil, err
	}

	if len(snapshots) == 0 {
		return nil, nil // No snapshot available
	}

	// Load the latest snapshot (first in sorted list)
	latestSnapshot := snapshots[0]
	data, err := os.ReadFile(latestSnapshot.FilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read snapshot file: %w", err)
	}

	var snapshot Snapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return nil, fmt.Errorf("failed to unmarshal snapshot: %w", err)
	}

	return &snapshot, nil
}

// ListSnapshots lists all available snapshots for a symbol (sorted by sequence desc)
func (s *FileSnapshotStore) ListSnapshots(ctx context.Context, symbol string) ([]SnapshotMetadata, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.listSnapshotsInternal(symbol)
}

// listSnapshotsInternal internal implementation without locking
func (s *FileSnapshotStore) listSnapshotsInternal(symbol string) ([]SnapshotMetadata, error) {
	symbolDir := filepath.Join(s.baseDir, symbol)

	// Check if directory exists
	if _, err := os.Stat(symbolDir); os.IsNotExist(err) {
		return []SnapshotMetadata{}, nil // No snapshots yet
	}

	// Read directory
	entries, err := os.ReadDir(symbolDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read snapshot directory: %w", err)
	}

	var snapshots []SnapshotMetadata
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		// Parse snapshot filename: snapshot-<seq>.json
		name := entry.Name()
		if !strings.HasPrefix(name, "snapshot-") || !strings.HasSuffix(name, ".json") {
			continue
		}

		var seq int64
		if _, err := fmt.Sscanf(name, "snapshot-%d.json", &seq); err != nil {
			continue // Skip invalid filenames
		}

		info, err := entry.Info()
		if err != nil {
			continue
		}

		snapshots = append(snapshots, SnapshotMetadata{
			Symbol:       symbol,
			LastSequence: seq,
			CapturedAt:   info.ModTime(),
			FilePath:     filepath.Join(symbolDir, name),
		})
	}

	// Sort by sequence descending (latest first)
	sort.Slice(snapshots, func(i, j int) bool {
		return snapshots[i].LastSequence > snapshots[j].LastSequence
	})

	return snapshots, nil
}

// Close closes the snapshot store
func (s *FileSnapshotStore) Close() error {
	// No resources to clean up for file-based store
	return nil
}
