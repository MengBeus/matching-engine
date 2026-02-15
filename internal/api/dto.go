package api

import "time"

// PlaceOrderRequest represents the request body for placing an order
type PlaceOrderRequest struct {
	ClientOrderID string `json:"client_order_id"` // Client-provided order ID
	AccountID     string `json:"account_id"`      // Account ID
	Symbol        string `json:"symbol"`          // Trading symbol (e.g., "BTC-USDT")
	Side          string `json:"side"`            // Order side: "BUY" or "SELL"
	Price         string `json:"price"`           // Price as decimal string
	Quantity      string `json:"quantity"`        // Quantity as decimal string
	IdempotencyKey string `json:"idempotency_key"` // Idempotency key for deduplication
}

// PlaceOrderResponse represents the response for placing an order
type PlaceOrderResponse struct {
	OrderID       string              `json:"order_id"`        // System-generated order ID
	ClientOrderID string              `json:"client_order_id"` // Client-provided order ID
	AccountID     string              `json:"account_id"`      // Account ID
	Symbol        string              `json:"symbol"`          // Trading symbol
	Side          string              `json:"side"`            // Order side
	Price         string              `json:"price"`           // Price as decimal string
	Quantity      string              `json:"quantity"`        // Quantity as decimal string
	Status        string              `json:"status"`          // Order status
	CreatedAt     time.Time           `json:"created_at"`      // Order creation time
	Trades        []TradeDTO          `json:"trades"`          // Trades executed (if any)
}

// CancelOrderResponse represents the response for canceling an order
type CancelOrderResponse struct {
	OrderID      string `json:"order_id"`       // Order ID
	Status       string `json:"status"`         // Order status after cancellation
	RemainingQty string `json:"remaining_qty"`  // Remaining quantity at cancellation
	FilledQty    string `json:"filled_qty"`     // Filled quantity
}

// QueryOrderResponse represents the response for querying an order
type QueryOrderResponse struct {
	OrderID       string    `json:"order_id"`        // Order ID
	ClientOrderID string    `json:"client_order_id"` // Client-provided order ID
	AccountID     string    `json:"account_id"`      // Account ID
	Symbol        string    `json:"symbol"`          // Trading symbol
	Side          string    `json:"side"`            // Order side
	Price         string    `json:"price"`           // Price as decimal string
	Quantity      string    `json:"quantity"`        // Quantity as decimal string
	RemainingQty  string    `json:"remaining_qty"`   // Remaining quantity
	FilledQty     string    `json:"filled_qty"`      // Filled quantity
	Status        string    `json:"status"`          // Order status
	CreatedAt     time.Time `json:"created_at"`      // Order creation time
}

// TradeDTO represents a trade execution
type TradeDTO struct {
	TradeID   string    `json:"trade_id"`   // Trade ID
	Price     string    `json:"price"`      // Trade price
	Quantity  string    `json:"quantity"`   // Trade quantity
	Side      string    `json:"side"`       // Side of this order in the trade
	Timestamp time.Time `json:"timestamp"`  // Trade timestamp
}

// ErrorResponse represents an error response
type ErrorResponse struct {
	Code    string `json:"code"`    // Error code
	Message string `json:"message"` // Error message
}
