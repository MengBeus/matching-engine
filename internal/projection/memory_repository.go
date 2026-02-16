package projection

import (
	"context"
	"fmt"
	"sort"
	"sync"
)

// MemoryOrderRepository is an in-memory implementation of OrderRepository
type MemoryOrderRepository struct {
	mu sync.RWMutex

	// Primary storage: order_id -> OrderView
	orders map[string]*OrderView

	// Indexes for efficient queries
	byClientOrderID map[string]map[string]*OrderView // account_id -> client_order_id -> OrderView
	byAccount       map[string][]*OrderView          // account_id -> []*OrderView
	bySymbol        map[string][]*OrderView          // symbol -> []*OrderView

	// Last applied sequence per symbol
	lastSequence map[string]int64 // symbol -> last_sequence
}

// NewMemoryOrderRepository creates a new in-memory order repository
func NewMemoryOrderRepository() *MemoryOrderRepository {
	return &MemoryOrderRepository{
		orders:          make(map[string]*OrderView),
		byClientOrderID: make(map[string]map[string]*OrderView),
		byAccount:       make(map[string][]*OrderView),
		bySymbol:        make(map[string][]*OrderView),
		lastSequence:    make(map[string]int64),
	}
}

// Save creates or updates an order view
func (r *MemoryOrderRepository) Save(ctx context.Context, order *OrderView) error {
	if order == nil {
		return ErrInvalidArgument
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	orderCopy := cloneOrderView(order)

	// Check if this is an update
	existing, exists := r.orders[orderCopy.OrderID]
	if exists {
		// Remove from indexes before updating
		r.removeFromIndexes(existing)
	}

	// Save to primary storage
	r.orders[orderCopy.OrderID] = orderCopy

	// Update indexes
	r.addToIndexes(orderCopy)

	return nil
}

// GetByID retrieves an order by order_id
func (r *MemoryOrderRepository) GetByID(ctx context.Context, orderID string) (*OrderView, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	order, exists := r.orders[orderID]
	if !exists {
		return nil, ErrOrderNotFound
	}

	return cloneOrderView(order), nil
}

// GetByClientOrderID retrieves an order by client_order_id and account_id
func (r *MemoryOrderRepository) GetByClientOrderID(ctx context.Context, accountID, clientOrderID string) (*OrderView, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	accountOrders, exists := r.byClientOrderID[accountID]
	if !exists {
		return nil, ErrOrderNotFound
	}

	order, exists := accountOrders[clientOrderID]
	if !exists {
		return nil, ErrOrderNotFound
	}

	return cloneOrderView(order), nil
}

// ListByAccount retrieves orders for a specific account
func (r *MemoryOrderRepository) ListByAccount(ctx context.Context, accountID string, limit int) ([]*OrderView, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	orders, exists := r.byAccount[accountID]
	if !exists {
		return []*OrderView{}, nil
	}

	// Apply limit
	if limit > 0 && len(orders) > limit {
		return cloneOrderViews(orders[:limit]), nil
	}

	return cloneOrderViews(orders), nil
}

// ListBySymbol retrieves orders for a specific symbol
func (r *MemoryOrderRepository) ListBySymbol(ctx context.Context, symbol string, limit int) ([]*OrderView, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	orders, exists := r.bySymbol[symbol]
	if !exists {
		return []*OrderView{}, nil
	}

	// Apply limit
	if limit > 0 && len(orders) > limit {
		return cloneOrderViews(orders[:limit]), nil
	}

	return cloneOrderViews(orders), nil
}

// GetLastSequence returns the last applied sequence number for a symbol
func (r *MemoryOrderRepository) GetLastSequence(ctx context.Context, symbol string) (int64, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	seq, exists := r.lastSequence[symbol]
	if !exists {
		return 0, nil
	}

	return seq, nil
}

// SetLastSequence updates the last applied sequence number for a symbol
func (r *MemoryOrderRepository) SetLastSequence(ctx context.Context, symbol string, sequence int64) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	current := r.lastSequence[symbol]
	if sequence < current {
		return fmt.Errorf("%w: symbol=%s current=%d new=%d", ErrSequenceRegression, symbol, current, sequence)
	}

	r.lastSequence[symbol] = sequence
	return nil
}

// addToIndexes adds an order to all indexes
func (r *MemoryOrderRepository) addToIndexes(order *OrderView) {
	// Index by client_order_id
	if _, exists := r.byClientOrderID[order.AccountID]; !exists {
		r.byClientOrderID[order.AccountID] = make(map[string]*OrderView)
	}
	r.byClientOrderID[order.AccountID][order.ClientOrderID] = order

	// Index by account
	r.byAccount[order.AccountID] = append(r.byAccount[order.AccountID], order)

	// Index by symbol
	r.bySymbol[order.Symbol] = append(r.bySymbol[order.Symbol], order)
}

// removeFromIndexes removes an order from all indexes
func (r *MemoryOrderRepository) removeFromIndexes(order *OrderView) {
	// Remove from client_order_id index
	if accountOrders, exists := r.byClientOrderID[order.AccountID]; exists {
		delete(accountOrders, order.ClientOrderID)
		if len(accountOrders) == 0 {
			delete(r.byClientOrderID, order.AccountID)
		}
	}

	// Remove from account index
	if orders, exists := r.byAccount[order.AccountID]; exists {
		for i, o := range orders {
			if o.OrderID == order.OrderID {
				r.byAccount[order.AccountID] = append(orders[:i], orders[i+1:]...)
				break
			}
		}
		if len(r.byAccount[order.AccountID]) == 0 {
			delete(r.byAccount, order.AccountID)
		}
	}

	// Remove from symbol index
	if orders, exists := r.bySymbol[order.Symbol]; exists {
		for i, o := range orders {
			if o.OrderID == order.OrderID {
				r.bySymbol[order.Symbol] = append(orders[:i], orders[i+1:]...)
				break
			}
		}
		if len(r.bySymbol[order.Symbol]) == 0 {
			delete(r.bySymbol, order.Symbol)
		}
	}
}

// MemoryTradeRepository is an in-memory implementation of TradeRepository
type MemoryTradeRepository struct {
	mu sync.RWMutex

	// Primary storage: trade_id -> TradeView
	trades map[string]*TradeView

	// Indexes for efficient queries
	bySymbol map[string][]*TradeView // symbol -> []*TradeView (sorted by sequence)
	byOrder  map[string][]*TradeView // order_id -> []*TradeView

	// Last applied sequence per symbol
	lastSequence map[string]int64 // symbol -> last_sequence
}

// NewMemoryTradeRepository creates a new in-memory trade repository
func NewMemoryTradeRepository() *MemoryTradeRepository {
	return &MemoryTradeRepository{
		trades:       make(map[string]*TradeView),
		bySymbol:     make(map[string][]*TradeView),
		byOrder:      make(map[string][]*TradeView),
		lastSequence: make(map[string]int64),
	}
}

// Save creates a trade view
func (r *MemoryTradeRepository) Save(ctx context.Context, trade *TradeView) error {
	if trade == nil {
		return ErrInvalidArgument
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	tradeCopy := cloneTradeView(trade)

	// Idempotency: same trade ID should not create duplicate index entries.
	if existing, exists := r.trades[tradeCopy.TradeID]; exists {
		if sameTrade(existing, tradeCopy) {
			return nil
		}
		return fmt.Errorf("%w: trade_id=%s", ErrTradeConflict, tradeCopy.TradeID)
	}

	// Save to primary storage
	r.trades[tradeCopy.TradeID] = tradeCopy

	// Index by symbol (maintain sequence order)
	r.bySymbol[tradeCopy.Symbol] = append(r.bySymbol[tradeCopy.Symbol], tradeCopy)
	sort.Slice(r.bySymbol[tradeCopy.Symbol], func(i, j int) bool {
		return r.bySymbol[tradeCopy.Symbol][i].Sequence < r.bySymbol[tradeCopy.Symbol][j].Sequence
	})

	// Index by order (both maker and taker)
	r.byOrder[tradeCopy.MakerOrderID] = append(r.byOrder[tradeCopy.MakerOrderID], tradeCopy)
	r.byOrder[tradeCopy.TakerOrderID] = append(r.byOrder[tradeCopy.TakerOrderID], tradeCopy)

	return nil
}

// GetByID retrieves a trade by trade_id
func (r *MemoryTradeRepository) GetByID(ctx context.Context, tradeID string) (*TradeView, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	trade, exists := r.trades[tradeID]
	if !exists {
		return nil, ErrTradeNotFound
	}

	return cloneTradeView(trade), nil
}

// ListBySymbol retrieves trades for a specific symbol
func (r *MemoryTradeRepository) ListBySymbol(ctx context.Context, symbol string, fromSequence int64, limit int) ([]*TradeView, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	trades, exists := r.bySymbol[symbol]
	if !exists {
		return []*TradeView{}, nil
	}

	// Filter by fromSequence if specified
	var filtered []*TradeView
	if fromSequence > 0 {
		for _, trade := range trades {
			if trade.Sequence >= fromSequence {
				filtered = append(filtered, trade)
			}
		}
	} else {
		filtered = trades
	}

	// Apply limit
	if limit > 0 && len(filtered) > limit {
		return cloneTradeViews(filtered[:limit]), nil
	}

	return cloneTradeViews(filtered), nil
}

// ListByOrder retrieves trades for a specific order
func (r *MemoryTradeRepository) ListByOrder(ctx context.Context, orderID string, limit int) ([]*TradeView, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	trades, exists := r.byOrder[orderID]
	if !exists {
		return []*TradeView{}, nil
	}

	// Apply limit
	if limit > 0 && len(trades) > limit {
		return cloneTradeViews(trades[:limit]), nil
	}

	return cloneTradeViews(trades), nil
}

// GetLastSequence returns the last applied sequence number for a symbol
func (r *MemoryTradeRepository) GetLastSequence(ctx context.Context, symbol string) (int64, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	seq, exists := r.lastSequence[symbol]
	if !exists {
		return 0, nil
	}

	return seq, nil
}

// SetLastSequence updates the last applied sequence number for a symbol
func (r *MemoryTradeRepository) SetLastSequence(ctx context.Context, symbol string, sequence int64) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	current := r.lastSequence[symbol]
	if sequence < current {
		return fmt.Errorf("%w: symbol=%s current=%d new=%d", ErrSequenceRegression, symbol, current, sequence)
	}

	r.lastSequence[symbol] = sequence
	return nil
}

func cloneOrderView(in *OrderView) *OrderView {
	if in == nil {
		return nil
	}
	cp := *in
	return &cp
}

func cloneOrderViews(in []*OrderView) []*OrderView {
	out := make([]*OrderView, 0, len(in))
	for _, v := range in {
		out = append(out, cloneOrderView(v))
	}
	return out
}

func cloneTradeView(in *TradeView) *TradeView {
	if in == nil {
		return nil
	}
	cp := *in
	return &cp
}

func cloneTradeViews(in []*TradeView) []*TradeView {
	out := make([]*TradeView, 0, len(in))
	for _, v := range in {
		out = append(out, cloneTradeView(v))
	}
	return out
}

func sameTrade(a, b *TradeView) bool {
	if a == nil || b == nil {
		return a == b
	}

	return a.TradeID == b.TradeID &&
		a.Symbol == b.Symbol &&
		a.MakerOrderID == b.MakerOrderID &&
		a.TakerOrderID == b.TakerOrderID &&
		a.MakerAccountID == b.MakerAccountID &&
		a.TakerAccountID == b.TakerAccountID &&
		a.Price == b.Price &&
		a.Quantity == b.Quantity &&
		a.Sequence == b.Sequence &&
		a.OccurredAt.Equal(b.OccurredAt)
}
