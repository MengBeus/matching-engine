package matching

import (
	"errors"
	"time"
)

// Side represents order side (buy/sell)
type Side string

const (
	SideBuy  Side = "BUY"
	SideSell Side = "SELL"
)

func (s Side) IsValid() bool {
	return s == SideBuy || s == SideSell
}

// OrderStatus represents order status
type OrderStatus string

const (
	OrderStatusNew             OrderStatus = "NEW"
	OrderStatusPartiallyFilled OrderStatus = "PARTIALLY_FILLED"
	OrderStatusFilled          OrderStatus = "FILLED"
	OrderStatusCanceled        OrderStatus = "CANCELED"
)

// CancelReason represents order cancellation reason
type CancelReason string

const (
	CancelReasonUser    CancelReason = "USER"
	CancelReasonSystem  CancelReason = "SYSTEM"
	CancelReasonExpired CancelReason = "EXPIRED"
)

// PlaceOrderRequest internal place order request (converted by gateway/access layer)
type PlaceOrderRequest struct {
	OrderID       string // System-generated order ID
	ClientOrderID string // Client order ID
	AccountID     string // Account ID
	Symbol        string // Trading pair
	Side          Side   // Order side
	PriceInt      int64  // Price in minimum units
	QuantityInt   int64  // Quantity in minimum units
}

// Validate validates place order request
func (r *PlaceOrderRequest) Validate() error {
	if r.OrderID == "" {
		return errors.New("order_id required")
	}
	if r.ClientOrderID == "" {
		return errors.New("client_order_id required")
	}
	if r.AccountID == "" {
		return errors.New("account_id required")
	}
	if r.Symbol == "" {
		return errors.New("symbol required")
	}
	if !r.Side.IsValid() {
		return errors.New("invalid side")
	}
	if r.PriceInt <= 0 {
		return errors.New("price must be positive")
	}
	if r.QuantityInt <= 0 {
		return errors.New("quantity must be positive")
	}
	return nil
}

// CancelOrderRequest cancel order request
type CancelOrderRequest struct {
	OrderID   string // Order ID
	AccountID string // Account ID (for permission check)
	Symbol    string // Trading pair
}

// Validate validates cancel order request
func (r *CancelOrderRequest) Validate() error {
	if r.OrderID == "" {
		return errors.New("order_id required")
	}
	if r.AccountID == "" {
		return errors.New("account_id required")
	}
	if r.Symbol == "" {
		return errors.New("symbol required")
	}
	return nil
}

// CommandResult command execution result
type CommandResult struct {
	OrderStatusChanges []OrderStatusChange // Order status changes
	Trades             []Trade             // Trade executions
	Events             []Event             // Domain events
}

// OrderStatusChange order status change
type OrderStatusChange struct {
	OrderID      string      // Order ID
	OldStatus    OrderStatus // Old status
	NewStatus    OrderStatus // New status
	RemainingQty int64       // Remaining quantity
	FilledQty    int64       // Filled quantity
}

// Trade represents a trade execution
type Trade struct {
	TradeID      string    // Trade ID
	Symbol       string    // Trading pair
	MakerOrderID string    // Maker order ID
	TakerOrderID string    // Taker order ID
	Price        int64     // Trade price
	Quantity     int64     // Trade quantity
	MakerSide    Side      // Maker side
	TakerSide    Side      // Taker side
	OccurredAt   time.Time // Trade time
}

// Event domain event interface
type Event interface {
	EventID() string
	EventType() string
	Sequence() int64
	Symbol() string
	OccurredAt() time.Time
}

// OrderAcceptedEvent order accepted event
type OrderAcceptedEvent struct {
	EventIDValue    string      // Event ID
	SequenceValue   int64       // Sequence number
	SymbolValue     string      // Trading pair
	OccurredAtValue time.Time   // Event time
	OrderID         string      // Order ID
	ClientOrderID   string      // Client order ID
	AccountID       string      // Account ID
	Side            Side        // Order side
	Price           int64       // Price
	Quantity        int64       // Quantity
	Status          OrderStatus // Order status
}

func (e *OrderAcceptedEvent) EventID() string       { return e.EventIDValue }
func (e *OrderAcceptedEvent) EventType() string     { return "OrderAccepted" }
func (e *OrderAcceptedEvent) Sequence() int64       { return e.SequenceValue }
func (e *OrderAcceptedEvent) Symbol() string        { return e.SymbolValue }
func (e *OrderAcceptedEvent) OccurredAt() time.Time { return e.OccurredAtValue }

// OrderMatchedEvent order matched event
type OrderMatchedEvent struct {
	EventIDValue    string    // Event ID
	SequenceValue   int64     // Sequence number
	SymbolValue     string    // Trading pair
	OccurredAtValue time.Time // Event time
	TradeID         string    // Trade ID
	MakerOrderID    string    // Maker order ID
	TakerOrderID    string    // Taker order ID
	Price           int64     // Trade price
	Quantity        int64     // Trade quantity
	MakerSide       Side      // Maker side
	TakerSide       Side      // Taker side
}

func (e *OrderMatchedEvent) EventID() string       { return e.EventIDValue }
func (e *OrderMatchedEvent) EventType() string     { return "OrderMatched" }
func (e *OrderMatchedEvent) Sequence() int64       { return e.SequenceValue }
func (e *OrderMatchedEvent) Symbol() string        { return e.SymbolValue }
func (e *OrderMatchedEvent) OccurredAt() time.Time { return e.OccurredAtValue }

// OrderCanceledEvent order canceled event
type OrderCanceledEvent struct {
	EventIDValue    string       // Event ID
	SequenceValue   int64        // Sequence number
	SymbolValue     string       // Trading pair
	OccurredAtValue time.Time    // Event time
	OrderID         string       // Order ID
	AccountID       string       // Account ID
	RemainingQty    int64        // Remaining quantity at cancellation
	CanceledBy      CancelReason // Cancellation reason (USER/SYSTEM/EXPIRED)
}

func (e *OrderCanceledEvent) EventID() string       { return e.EventIDValue }
func (e *OrderCanceledEvent) EventType() string     { return "OrderCanceled" }
func (e *OrderCanceledEvent) Sequence() int64       { return e.SequenceValue }
func (e *OrderCanceledEvent) Symbol() string        { return e.SymbolValue }
func (e *OrderCanceledEvent) OccurredAt() time.Time { return e.OccurredAtValue }
