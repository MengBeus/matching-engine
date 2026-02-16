package projection

import (
	"context"
	"errors"
)

var (
	ErrOrderNotFound      = errors.New("order not found")
	ErrTradeNotFound      = errors.New("trade not found")
	ErrInvalidArgument    = errors.New("invalid argument")
	ErrSequenceRegression = errors.New("sequence regression")
	ErrTradeConflict      = errors.New("trade conflict")
)

// OrderRepository defines the interface for order read model storage
type OrderRepository interface {
	// Save creates or updates an order view
	Save(ctx context.Context, order *OrderView) error

	// GetByID retrieves an order by order_id
	GetByID(ctx context.Context, orderID string) (*OrderView, error)

	// GetByClientOrderID retrieves an order by client_order_id and account_id
	GetByClientOrderID(ctx context.Context, accountID, clientOrderID string) (*OrderView, error)

	// ListByAccount retrieves orders for a specific account
	ListByAccount(ctx context.Context, accountID string, limit int) ([]*OrderView, error)

	// ListBySymbol retrieves orders for a specific symbol
	ListBySymbol(ctx context.Context, symbol string, limit int) ([]*OrderView, error)

	// GetLastSequence returns the last applied sequence number for a symbol
	GetLastSequence(ctx context.Context, symbol string) (int64, error)

	// SetLastSequence updates the last applied sequence number for a symbol
	SetLastSequence(ctx context.Context, symbol string, sequence int64) error
}

// TradeRepository defines the interface for trade read model storage
type TradeRepository interface {
	// Save creates a trade view
	Save(ctx context.Context, trade *TradeView) error

	// GetByID retrieves a trade by trade_id
	GetByID(ctx context.Context, tradeID string) (*TradeView, error)

	// ListBySymbol retrieves trades for a specific symbol
	// fromSequence: if > 0, only return trades with sequence >= fromSequence
	ListBySymbol(ctx context.Context, symbol string, fromSequence int64, limit int) ([]*TradeView, error)

	// ListByOrder retrieves trades for a specific order
	ListByOrder(ctx context.Context, orderID string, limit int) ([]*TradeView, error)

	// GetLastSequence returns the last applied sequence number for a symbol
	GetLastSequence(ctx context.Context, symbol string) (int64, error)

	// SetLastSequence updates the last applied sequence number for a symbol
	SetLastSequence(ctx context.Context, symbol string, sequence int64) error
}
