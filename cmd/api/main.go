package main

import (
	"log"
	"os"

	"matching-engine/internal/api"
)

func main() {
	router := api.NewRouter()
	addr := getenv("APP_ADDR", ":8080")

	if err := router.Run(addr); err != nil {
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
