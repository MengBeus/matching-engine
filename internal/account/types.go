package account

import (
	"fmt"
	"strings"
)

// Balance represents account balance for a specific asset
type Balance struct {
	Available int64 // Available balance for new orders
	Frozen    int64 // Frozen balance locked by active orders
}

// Total returns the total balance (available + frozen)
func (b Balance) Total() int64 {
	return b.Available + b.Frozen
}

// PlaceIntent represents the intent to place an order
type PlaceIntent struct {
	AccountID   string
	OrderID     string
	Symbol      string // e.g., "BTC-USDT"
	Side        string // "BUY" or "SELL"
	PriceInt    int64  // fixed-scale price, precision from symbol spec
	QtyInt      int64  // fixed-scale quantity, precision from symbol spec
	IdemKey     string
	PayloadHash string
}

// Validate validates the place intent
func (p *PlaceIntent) Validate() error {
	if p.AccountID == "" {
		return fmt.Errorf("account_id is required")
	}
	if p.OrderID == "" {
		return fmt.Errorf("order_id is required")
	}
	if p.Symbol == "" {
		return fmt.Errorf("symbol is required")
	}
	if p.Side != "BUY" && p.Side != "SELL" {
		return fmt.Errorf("invalid side: %s", p.Side)
	}
	if p.PriceInt <= 0 {
		return fmt.Errorf("price must be positive")
	}
	if p.QtyInt <= 0 {
		return fmt.Errorf("quantity must be positive")
	}
	return nil
}

// CancelIntent represents the intent to cancel an order
type CancelIntent struct {
	AccountID string
	OrderID   string
	Symbol    string
}

// Validate validates the cancel intent
func (c *CancelIntent) Validate() error {
	if c.AccountID == "" {
		return fmt.Errorf("account_id is required")
	}
	if c.OrderID == "" {
		return fmt.Errorf("order_id is required")
	}
	if c.Symbol == "" {
		return fmt.Errorf("symbol is required")
	}
	return nil
}

// TradeIntent represents a trade execution that affects balances
type TradeIntent struct {
	TradeID         string
	BuyerAccountID  string
	SellerAccountID string
	BuyerOrderID    string
	SellerOrderID   string
	Symbol          string
	PriceInt        int64 // fixed-scale price, precision from symbol spec
	QuantityInt     int64 // fixed-scale quantity, precision from symbol spec
}

// ParseSymbol splits a symbol like "BTC-USDT" into base and quote assets
func ParseSymbol(symbol string) (base, quote string, err error) {
	parts := strings.Split(symbol, "-")
	if len(parts) != 2 {
		return "", "", fmt.Errorf("%w: %s", ErrInvalidSymbol, symbol)
	}
	return parts[0], parts[1], nil
}
