package projection

import (
	"time"
)

// OrderStatus represents the status of an order in the read model
type OrderStatus string

const (
	OrderStatusNew             OrderStatus = "NEW"
	OrderStatusPartiallyFilled OrderStatus = "PARTIALLY_FILLED"
	OrderStatusFilled          OrderStatus = "FILLED"
	OrderStatusCanceled        OrderStatus = "CANCELED"

	// Backward-compatible alias.
	OrderStatusPartial OrderStatus = OrderStatusPartiallyFilled
)

// OrderView represents the read model for an order
type OrderView struct {
	OrderID       string      `json:"order_id"`
	ClientOrderID string      `json:"client_order_id"`
	AccountID     string      `json:"account_id"`
	Symbol        string      `json:"symbol"`
	Side          string      `json:"side"` // "BUY" or "SELL"
	Price         int64       `json:"price"`
	Quantity      int64       `json:"quantity"`
	RemainingQty  int64       `json:"remaining_qty"`
	FilledQty     int64       `json:"filled_qty"`
	Status        OrderStatus `json:"status"`
	CreatedAt     time.Time   `json:"created_at"`
	UpdatedAt     time.Time   `json:"updated_at"`
	LastSequence  int64       `json:"last_sequence"` // Last event sequence that updated this order
}

// TradeView represents the read model for a trade
type TradeView struct {
	TradeID        string    `json:"trade_id"`
	Symbol         string    `json:"symbol"`
	MakerOrderID   string    `json:"maker_order_id"`
	TakerOrderID   string    `json:"taker_order_id"`
	MakerAccountID string    `json:"maker_account_id"`
	TakerAccountID string    `json:"taker_account_id"`
	Price          int64     `json:"price"`
	Quantity       int64     `json:"quantity"`
	OccurredAt     time.Time `json:"occurred_at"`
	Sequence       int64     `json:"sequence"` // Event sequence number
}
