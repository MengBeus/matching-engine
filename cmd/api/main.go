package main

import (
	"log"
	"net/http"
	"os"
	"time"

	"matching-engine/internal/account"
	"matching-engine/internal/api"
	"matching-engine/internal/engine"
)

func main() {
	// Initialize account service
	accountSvc := account.NewMemoryService()

	// Initialize engine
	eng := engine.NewEngine(&engine.EngineConfig{
		ShardCount:     8,
		QueueSize:      1000,
		IdempotencyTTL: 24 * time.Hour,
	})
	defer eng.Close()

	// Create router
	router := api.NewRouter(accountSvc, eng)
	addr := getenv("APP_ADDR", ":8080")

	log.Printf("Starting server on %s", addr)
	if err := http.ListenAndServe(addr, router); err != nil {
		log.Fatal("Failed to start server:", err)
	}
}

func getenv(key, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
}
