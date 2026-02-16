package persistence

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"matching-engine/internal/matching"
)

// FileEventStore implements EventStore using JSONL files
type FileEventStore struct {
	baseDir string
	mu      sync.RWMutex
	files   map[string]*os.File // symbol -> file handle
}

// NewFileEventStore creates a new file-based event store
func NewFileEventStore(baseDir string) (*FileEventStore, error) {
	if err := os.MkdirAll(baseDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create base directory: %w", err)
	}

	return &FileEventStore{
		baseDir: baseDir,
		files:   make(map[string]*os.File),
	}, nil
}

// Append appends an event to the log for a specific symbol
func (s *FileEventStore) Append(ctx context.Context, symbol string, event matching.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Get or create file handle
	file, err := s.getOrCreateFile(symbol)
	if err != nil {
		return fmt.Errorf("failed to get file for symbol %s: %w", symbol, err)
	}

	// Convert event to EventRecord
	record := EventRecord{
		Version:    1,
		Symbol:     event.Symbol(),
		Sequence:   event.Sequence(),
		Type:       event.EventType(),
		OccurredAt: event.OccurredAt(),
		Payload:    event,
	}

	// Marshal to JSON
	data, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	// Append to file with newline
	if _, err := file.Write(append(data, '\n')); err != nil {
		return fmt.Errorf("failed to write event: %w", err)
	}

	// Sync to disk for durability
	if err := file.Sync(); err != nil {
		return fmt.Errorf("failed to sync file: %w", err)
	}

	return nil
}

// getOrCreateFile gets or creates a file handle for a symbol
func (s *FileEventStore) getOrCreateFile(symbol string) (*os.File, error) {
	if file, ok := s.files[symbol]; ok {
		return file, nil
	}

	// Create symbol directory
	symbolDir := filepath.Join(s.baseDir, symbol)
	if err := os.MkdirAll(symbolDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create symbol directory: %w", err)
	}

	// Open or create events.log file
	filePath := filepath.Join(symbolDir, "events.log")
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open events file: %w", err)
	}

	s.files[symbol] = file
	return file, nil
}

// ReadFrom reads events from a specific sequence number (inclusive)
func (s *FileEventStore) ReadFrom(ctx context.Context, symbol string, fromSeq int64) ([]matching.Event, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Build file path
	filePath := filepath.Join(s.baseDir, symbol, "events.log")

	// Check if file exists
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return []matching.Event{}, nil // No events yet
	}

	// Open file for reading
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open events file: %w", err)
	}
	defer file.Close()

	var events []matching.Event
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		// Parse EventRecord
		var record EventRecord
		if err := json.Unmarshal(line, &record); err != nil {
			return nil, fmt.Errorf("failed to unmarshal event record: %w", err)
		}

		// Skip events before fromSeq
		if record.Sequence < fromSeq {
			continue
		}

		// Deserialize payload to concrete event type
		event, err := s.deserializeEvent(&record)
		if err != nil {
			return nil, fmt.Errorf("failed to deserialize event: %w", err)
		}

		events = append(events, event)
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("failed to scan events file: %w", err)
	}

	return events, nil
}

// deserializeEvent deserializes an EventRecord to a concrete event type
func (s *FileEventStore) deserializeEvent(record *EventRecord) (matching.Event, error) {
	// Re-marshal payload to JSON for type-specific unmarshaling
	payloadBytes, err := json.Marshal(record.Payload)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal payload: %w", err)
	}

	switch record.Type {
	case "OrderAccepted":
		var event matching.OrderAcceptedEvent
		if err := json.Unmarshal(payloadBytes, &event); err != nil {
			return nil, fmt.Errorf("failed to unmarshal OrderAcceptedEvent: %w", err)
		}
		return &event, nil

	case "OrderMatched":
		var event matching.OrderMatchedEvent
		if err := json.Unmarshal(payloadBytes, &event); err != nil {
			return nil, fmt.Errorf("failed to unmarshal OrderMatchedEvent: %w", err)
		}
		return &event, nil

	case "OrderCanceled":
		var event matching.OrderCanceledEvent
		if err := json.Unmarshal(payloadBytes, &event); err != nil {
			return nil, fmt.Errorf("failed to unmarshal OrderCanceledEvent: %w", err)
		}
		return &event, nil

	default:
		return nil, fmt.Errorf("unknown event type: %s", record.Type)
	}
}

// GetLastSequence returns the last sequence number for a symbol
func (s *FileEventStore) GetLastSequence(ctx context.Context, symbol string) (int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	filePath := filepath.Join(s.baseDir, symbol, "events.log")

	// Check if file exists
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return 0, nil // No events yet, start from 0
	}

	// Open file for reading
	file, err := os.Open(filePath)
	if err != nil {
		return 0, fmt.Errorf("failed to open events file: %w", err)
	}
	defer file.Close()

	var lastSeq int64 = 0
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		var record EventRecord
		if err := json.Unmarshal(line, &record); err != nil {
			return 0, fmt.Errorf("failed to unmarshal event record: %w", err)
		}

		if record.Sequence > lastSeq {
			lastSeq = record.Sequence
		}
	}

	if err := scanner.Err(); err != nil {
		return 0, fmt.Errorf("failed to scan events file: %w", err)
	}

	return lastSeq, nil
}

// ListSymbols lists all symbols that have event logs
func (s *FileEventStore) ListSymbols(ctx context.Context) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Check if base directory exists
	if _, err := os.Stat(s.baseDir); os.IsNotExist(err) {
		return []string{}, nil // No symbols yet
	}

	// Read directory entries
	entries, err := os.ReadDir(s.baseDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read base directory: %w", err)
	}

	var symbols []string
	for _, entry := range entries {
		if entry.IsDir() {
			// Check if events.log exists in this directory
			eventsFile := filepath.Join(s.baseDir, entry.Name(), "events.log")
			if _, err := os.Stat(eventsFile); err == nil {
				symbols = append(symbols, entry.Name())
			}
		}
	}

	return symbols, nil
}

// Close closes all open file handles
func (s *FileEventStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var errs []error
	for symbol, file := range s.files {
		if err := file.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close file for symbol %s: %w", symbol, err))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors closing files: %v", errs)
	}

	return nil
}
