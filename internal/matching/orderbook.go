package matching

import (
	"container/list"
	"fmt"
	"time"
)

// Order represents an order in the order book
type Order struct {
	OrderID       string
	ClientOrderID string
	AccountID     string
	Symbol        string
	Side          Side
	Price         int64
	Quantity      int64
	RemainingQty  int64
	Status        OrderStatus
	CreatedAt     time.Time
	element       *list.Element // Reference to position in price level queue
}

// PriceLevel represents all orders at a specific price
type PriceLevel struct {
	Price  int64
	Queue  *list.List // FIFO queue of orders
	Volume int64      // Total quantity at this price level
}

// NewPriceLevel creates a new price level
func NewPriceLevel(price int64) *PriceLevel {
	return &PriceLevel{
		Price:  price,
		Queue:  list.New(),
		Volume: 0,
	}
}

// AddOrder adds an order to the price level
func (pl *PriceLevel) AddOrder(order *Order) {
	order.element = pl.Queue.PushBack(order)
	pl.Volume += order.RemainingQty
}

// RemoveOrder removes an order from the price level
func (pl *PriceLevel) RemoveOrder(order *Order) {
	if order.element != nil {
		pl.Queue.Remove(order.element)
		pl.Volume -= order.RemainingQty
		order.element = nil
	}
}

// IsEmpty returns true if the price level has no orders
func (pl *PriceLevel) IsEmpty() bool {
	return pl.Queue.Len() == 0
}

// OrderBook represents the order book for a symbol
type OrderBook struct {
	Symbol       string
	BidLevels    map[int64]*PriceLevel  // Buy orders (price -> level)
	AskLevels    map[int64]*PriceLevel  // Sell orders (price -> level)
	Orders       map[string]*Order      // order_id -> Order
	closedOrders map[string]OrderStatus // closed order_id -> terminal status
	eventSeq     int64                  // Event sequence number
	tradeSeq     int64                  // Trade identifier sequence
}

// NewOrderBook creates a new order book
func NewOrderBook(symbol string) *OrderBook {
	return &OrderBook{
		Symbol:       symbol,
		BidLevels:    make(map[int64]*PriceLevel),
		AskLevels:    make(map[int64]*PriceLevel),
		Orders:       make(map[string]*Order),
		closedOrders: make(map[string]OrderStatus),
		eventSeq:     0,
		tradeSeq:     0,
	}
}

func (ob *OrderBook) nextEventSequence() int64 {
	ob.eventSeq++
	return ob.eventSeq
}

func (ob *OrderBook) nextTradeID() string {
	ob.tradeSeq++
	return fmt.Sprintf("trd_%d", ob.tradeSeq)
}

// getBestBid returns the highest bid price, or 0 if no bids
func (ob *OrderBook) getBestBid() int64 {
	var best int64
	for price := range ob.BidLevels {
		if price > best {
			best = price
		}
	}
	return best
}

// getBestAsk returns the lowest ask price, or 0 if no asks
func (ob *OrderBook) getBestAsk() int64 {
	var best int64
	for price := range ob.AskLevels {
		if best == 0 || price < best {
			best = price
		}
	}
	return best
}

// getOrCreatePriceLevel gets or creates a price level
func (ob *OrderBook) getOrCreatePriceLevel(side Side, price int64) *PriceLevel {
	var levels map[int64]*PriceLevel
	if side == SideBuy {
		levels = ob.BidLevels
	} else {
		levels = ob.AskLevels
	}

	level, exists := levels[price]
	if !exists {
		level = NewPriceLevel(price)
		levels[price] = level
	}
	return level
}

// removePriceLevelIfEmpty removes a price level if it's empty
func (ob *OrderBook) removePriceLevelIfEmpty(side Side, price int64) {
	var levels map[int64]*PriceLevel
	if side == SideBuy {
		levels = ob.BidLevels
	} else {
		levels = ob.AskLevels
	}

	if level, exists := levels[price]; exists && level.IsEmpty() {
		delete(levels, price)
	}
}

func (ob *OrderBook) getPriceLevel(side Side, price int64) *PriceLevel {
	if side == SideBuy {
		return ob.BidLevels[price]
	}
	return ob.AskLevels[price]
}

// PlaceLimit places a limit order and attempts to match it
func (ob *OrderBook) PlaceLimit(req *PlaceOrderRequest) (*CommandResult, error) {
	if req == nil {
		return nil, fmt.Errorf("request is nil")
	}
	if err := req.Validate(); err != nil {
		return nil, err
	}
	if req.Symbol != ob.Symbol {
		return nil, fmt.Errorf("symbol mismatch: request %s, orderbook %s", req.Symbol, ob.Symbol)
	}
	if _, exists := ob.Orders[req.OrderID]; exists {
		return nil, fmt.Errorf("duplicate order_id: %s", req.OrderID)
	}
	if _, exists := ob.closedOrders[req.OrderID]; exists {
		return nil, fmt.Errorf("duplicate order_id: %s", req.OrderID)
	}

	result := &CommandResult{
		OrderStatusChanges: []OrderStatusChange{},
		Trades:             []Trade{},
		Events:             []Event{},
	}

	// Create order
	order := &Order{
		OrderID:       req.OrderID,
		ClientOrderID: req.ClientOrderID,
		AccountID:     req.AccountID,
		Symbol:        req.Symbol,
		Side:          req.Side,
		Price:         req.PriceInt,
		Quantity:      req.QuantityInt,
		RemainingQty:  req.QuantityInt,
		Status:        OrderStatusNew,
		CreatedAt:     time.Now(),
	}

	// Store order
	ob.Orders[order.OrderID] = order

	// Generate OrderAccepted event
	seq := ob.nextEventSequence()
	acceptedEvent := &OrderAcceptedEvent{
		EventIDValue:    fmt.Sprintf("evt_%d", seq),
		SequenceValue:   seq,
		SymbolValue:     ob.Symbol,
		OccurredAtValue: time.Now(),
		OrderID:         order.OrderID,
		ClientOrderID:   order.ClientOrderID,
		AccountID:       order.AccountID,
		Side:            order.Side,
		Price:           order.Price,
		Quantity:        order.Quantity,
		Status:          order.Status,
	}
	result.Events = append(result.Events, acceptedEvent)

	// Try to match
	if order.Side == SideBuy {
		ob.matchBuyOrder(order, result)
	} else {
		ob.matchSellOrder(order, result)
	}

	// If order still has remaining quantity, add to order book
	if order.RemainingQty > 0 {
		level := ob.getOrCreatePriceLevel(order.Side, order.Price)
		level.AddOrder(order)
	} else {
		// Order fully filled, remove from orders map
		delete(ob.Orders, order.OrderID)
		ob.closedOrders[order.OrderID] = OrderStatusFilled
	}

	return result, nil
}

// matchBuyOrder matches a buy order against sell orders
func (ob *OrderBook) matchBuyOrder(buyOrder *Order, result *CommandResult) {
	for buyOrder.RemainingQty > 0 {
		// Get best ask (lowest sell price)
		bestAsk := ob.getBestAsk()
		if bestAsk == 0 || buyOrder.Price < bestAsk {
			// No matching sell orders
			break
		}

		// Get the ask level
		askLevel := ob.AskLevels[bestAsk]
		if askLevel == nil || askLevel.IsEmpty() {
			delete(ob.AskLevels, bestAsk)
			continue
		}

		// Get first order in queue (FIFO)
		element := askLevel.Queue.Front()
		if element == nil {
			break
		}

		sellOrder := element.Value.(*Order)

		// Match orders
		matchQty := ob.executeMatch(sellOrder, buyOrder, sellOrder.Price, result)
		askLevel.Volume -= matchQty
		if askLevel.Volume < 0 {
			askLevel.Volume = 0
		}

		// If sell order is fully filled, remove it
		if sellOrder.RemainingQty == 0 {
			askLevel.RemoveOrder(sellOrder)
			delete(ob.Orders, sellOrder.OrderID)
			ob.closedOrders[sellOrder.OrderID] = OrderStatusFilled
			ob.removePriceLevelIfEmpty(SideSell, bestAsk)
		}
	}
}

// matchSellOrder matches a sell order against buy orders
func (ob *OrderBook) matchSellOrder(sellOrder *Order, result *CommandResult) {
	for sellOrder.RemainingQty > 0 {
		// Get best bid (highest buy price)
		bestBid := ob.getBestBid()
		if bestBid == 0 || sellOrder.Price > bestBid {
			// No matching buy orders
			break
		}

		// Get the bid level
		bidLevel := ob.BidLevels[bestBid]
		if bidLevel == nil || bidLevel.IsEmpty() {
			delete(ob.BidLevels, bestBid)
			continue
		}

		// Get first order in queue (FIFO)
		element := bidLevel.Queue.Front()
		if element == nil {
			break
		}

		buyOrder := element.Value.(*Order)

		// Match orders
		matchQty := ob.executeMatch(buyOrder, sellOrder, buyOrder.Price, result)
		bidLevel.Volume -= matchQty
		if bidLevel.Volume < 0 {
			bidLevel.Volume = 0
		}

		// If buy order is fully filled, remove it
		if buyOrder.RemainingQty == 0 {
			bidLevel.RemoveOrder(buyOrder)
			delete(ob.Orders, buyOrder.OrderID)
			ob.closedOrders[buyOrder.OrderID] = OrderStatusFilled
			ob.removePriceLevelIfEmpty(SideBuy, bestBid)
		}
	}
}

// executeMatch executes a match between two orders
func (ob *OrderBook) executeMatch(makerOrder, takerOrder *Order, price int64, result *CommandResult) int64 {
	// Calculate match quantity
	matchQty := makerOrder.RemainingQty
	if takerOrder.RemainingQty < matchQty {
		matchQty = takerOrder.RemainingQty
	}

	// Update remaining quantities
	makerOrder.RemainingQty -= matchQty
	takerOrder.RemainingQty -= matchQty

	// Generate trade
	trade := Trade{
		TradeID:      ob.nextTradeID(),
		Symbol:       ob.Symbol,
		MakerOrderID: makerOrder.OrderID,
		TakerOrderID: takerOrder.OrderID,
		Price:        price,
		Quantity:     matchQty,
		MakerSide:    makerOrder.Side,
		TakerSide:    takerOrder.Side,
		OccurredAt:   time.Now(),
	}
	result.Trades = append(result.Trades, trade)

	// Generate OrderMatched event
	seq := ob.nextEventSequence()
	matchedEvent := &OrderMatchedEvent{
		EventIDValue:    fmt.Sprintf("evt_%d", seq),
		SequenceValue:   seq,
		SymbolValue:     ob.Symbol,
		OccurredAtValue: time.Now(),
		TradeID:         trade.TradeID,
		MakerOrderID:    makerOrder.OrderID,
		TakerOrderID:    takerOrder.OrderID,
		Price:           price,
		Quantity:        matchQty,
		MakerSide:       makerOrder.Side,
		TakerSide:       takerOrder.Side,
	}
	result.Events = append(result.Events, matchedEvent)

	// Update order statuses
	ob.updateOrderStatus(makerOrder, result)
	ob.updateOrderStatus(takerOrder, result)

	return matchQty
}

// updateOrderStatus updates order status based on remaining quantity
func (ob *OrderBook) updateOrderStatus(order *Order, result *CommandResult) {
	oldStatus := order.Status
	var newStatus OrderStatus

	if order.RemainingQty == 0 {
		newStatus = OrderStatusFilled
	} else if order.RemainingQty < order.Quantity {
		newStatus = OrderStatusPartiallyFilled
	} else {
		newStatus = OrderStatusNew
	}

	if newStatus != oldStatus {
		order.Status = newStatus
		result.OrderStatusChanges = append(result.OrderStatusChanges, OrderStatusChange{
			OrderID:      order.OrderID,
			OldStatus:    oldStatus,
			NewStatus:    newStatus,
			RemainingQty: order.RemainingQty,
			FilledQty:    order.Quantity - order.RemainingQty,
		})
	}
}

// Cancel cancels an order
func (ob *OrderBook) Cancel(req *CancelOrderRequest) (*CommandResult, error) {
	if req == nil {
		return nil, fmt.Errorf("request is nil")
	}
	if err := req.Validate(); err != nil {
		return nil, err
	}
	if req.Symbol != ob.Symbol {
		return nil, fmt.Errorf("symbol mismatch: request %s, orderbook %s", req.Symbol, ob.Symbol)
	}
	if status, exists := ob.closedOrders[req.OrderID]; exists {
		switch status {
		case OrderStatusFilled:
			return nil, fmt.Errorf("order already filled")
		case OrderStatusCanceled:
			return nil, fmt.Errorf("order already canceled")
		default:
			return nil, fmt.Errorf("order not found: %s", req.OrderID)
		}
	}

	result := &CommandResult{
		OrderStatusChanges: []OrderStatusChange{},
		Trades:             []Trade{},
		Events:             []Event{},
	}

	// Find order
	order, exists := ob.Orders[req.OrderID]
	if !exists {
		return nil, fmt.Errorf("order not found: %s", req.OrderID)
	}

	// Verify account
	if order.AccountID != req.AccountID {
		return nil, fmt.Errorf("unauthorized: order belongs to different account")
	}

	// Check if order can be canceled
	if order.Status == OrderStatusFilled {
		return nil, fmt.Errorf("cannot cancel filled order")
	}
	if order.Status == OrderStatusCanceled {
		return nil, fmt.Errorf("order already canceled")
	}

	// Remove from order book
	level := ob.getPriceLevel(order.Side, order.Price)
	if level != nil {
		level.RemoveOrder(order)
		ob.removePriceLevelIfEmpty(order.Side, order.Price)
	}

	// Update order status
	oldStatus := order.Status
	order.Status = OrderStatusCanceled

	result.OrderStatusChanges = append(result.OrderStatusChanges, OrderStatusChange{
		OrderID:      order.OrderID,
		OldStatus:    oldStatus,
		NewStatus:    OrderStatusCanceled,
		RemainingQty: order.RemainingQty,
		FilledQty:    order.Quantity - order.RemainingQty,
	})

	// Generate OrderCanceled event
	seq := ob.nextEventSequence()
	canceledEvent := &OrderCanceledEvent{
		EventIDValue:    fmt.Sprintf("evt_%d", seq),
		SequenceValue:   seq,
		SymbolValue:     ob.Symbol,
		OccurredAtValue: time.Now(),
		OrderID:         order.OrderID,
		AccountID:       order.AccountID,
		RemainingQty:    order.RemainingQty,
		CanceledBy:      CancelReasonUser,
	}
	result.Events = append(result.Events, canceledEvent)

	// Remove from orders map
	delete(ob.Orders, order.OrderID)
	ob.closedOrders[order.OrderID] = OrderStatusCanceled

	return result, nil
}

// OrderSnapshot represents a snapshot of an order's current state
type OrderSnapshot struct {
	OrderID       string
	ClientOrderID string
	AccountID     string
	Symbol        string
	Side          Side
	Price         int64
	Quantity      int64
	RemainingQty  int64
	FilledQty     int64
	Status        OrderStatus
	CreatedAt     time.Time
}

// GetOrderSnapshot returns a snapshot of an order's current state
func (ob *OrderBook) GetOrderSnapshot(orderID string) (*OrderSnapshot, error) {
	// Check active orders first
	if order, exists := ob.Orders[orderID]; exists {
		return &OrderSnapshot{
			OrderID:       order.OrderID,
			ClientOrderID: order.ClientOrderID,
			AccountID:     order.AccountID,
			Symbol:        order.Symbol,
			Side:          order.Side,
			Price:         order.Price,
			Quantity:      order.Quantity,
			RemainingQty:  order.RemainingQty,
			FilledQty:     order.Quantity - order.RemainingQty,
			Status:        order.Status,
			CreatedAt:     order.CreatedAt,
		}, nil
	}

	// Check closed orders
	if status, exists := ob.closedOrders[orderID]; exists {
		// For closed orders, we only have the status
		// Return a minimal snapshot
		return &OrderSnapshot{
			OrderID:      orderID,
			Symbol:       ob.Symbol,
			Status:       status,
			RemainingQty: 0,
		}, nil
	}

	return nil, fmt.Errorf("order not found: %s", orderID)
}
