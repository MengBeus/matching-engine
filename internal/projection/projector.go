package projection

import (
	"context"
	"errors"
	"fmt"
	"time"

	"matching-engine/internal/matching"
)

// Projector consumes domain events and updates read models
type Projector struct {
	orderRepo OrderRepository
	tradeRepo TradeRepository
}

// NewProjector creates a new projector
func NewProjector(orderRepo OrderRepository, tradeRepo TradeRepository) *Projector {
	return &Projector{
		orderRepo: orderRepo,
		tradeRepo: tradeRepo,
	}
}

// Project applies a single event to the read models
// Returns error if sequence validation fails or projection fails
func (p *Projector) Project(ctx context.Context, event matching.Event) error {
	if event == nil {
		return fmt.Errorf("event is nil")
	}

	symbol := event.Symbol()
	sequence := event.Sequence()

	// Validate sequence continuity
	if err := p.validateSequence(ctx, symbol, sequence); err != nil {
		return err
	}

	// Apply event based on type
	switch e := event.(type) {
	case *matching.OrderAcceptedEvent:
		if err := p.projectOrderAccepted(ctx, e); err != nil {
			return fmt.Errorf("failed to project OrderAccepted: %w", err)
		}
	case *matching.OrderMatchedEvent:
		if err := p.projectOrderMatched(ctx, e); err != nil {
			return fmt.Errorf("failed to project OrderMatched: %w", err)
		}
	case *matching.OrderCanceledEvent:
		if err := p.projectOrderCanceled(ctx, e); err != nil {
			return fmt.Errorf("failed to project OrderCanceled: %w", err)
		}
	default:
		return fmt.Errorf("unknown event type: %T", event)
	}

	// Advance sequence cursor after successful projection.
	// IMPORTANT: advance trade first, then order.
	// Sequence validation uses orderRepo as source of truth; if order advances first
	// and trade fails, replay would be blocked by sequence regression.
	if err := p.tradeRepo.SetLastSequence(ctx, symbol, sequence); err != nil {
		return fmt.Errorf("failed to advance trade sequence: %w", err)
	}
	if err := p.orderRepo.SetLastSequence(ctx, symbol, sequence); err != nil {
		return fmt.Errorf("failed to advance order sequence: %w", err)
	}

	return nil
}

// validateSequence checks if the event sequence is valid (must be last + 1)
func (p *Projector) validateSequence(ctx context.Context, symbol string, sequence int64) error {
	orderLastSeq, err := p.orderRepo.GetLastSequence(ctx, symbol)
	if err != nil {
		return fmt.Errorf("failed to get order last sequence: %w", err)
	}
	tradeLastSeq, err := p.tradeRepo.GetLastSequence(ctx, symbol)
	if err != nil {
		return fmt.Errorf("failed to get trade last sequence: %w", err)
	}
	if orderLastSeq != tradeLastSeq {
		return fmt.Errorf("projection sequence mismatch: symbol=%s order_last=%d trade_last=%d",
			symbol, orderLastSeq, tradeLastSeq)
	}
	lastSeq := orderLastSeq

	// First event for this symbol should have sequence 1
	if lastSeq == 0 && sequence != 1 {
		return fmt.Errorf("first event must have sequence 1, got %d", sequence)
	}

	// Subsequent events must be exactly last + 1
	if lastSeq > 0 && sequence != lastSeq+1 {
		if sequence < lastSeq+1 {
			return fmt.Errorf("sequence regression: symbol=%s last=%d event=%d", symbol, lastSeq, sequence)
		}
		return fmt.Errorf("sequence gap detected: symbol=%s last=%d event=%d", symbol, lastSeq, sequence)
	}

	return nil
}

// projectOrderAccepted creates a new order view
func (p *Projector) projectOrderAccepted(ctx context.Context, event *matching.OrderAcceptedEvent) error {
	existing, err := p.orderRepo.GetByID(ctx, event.OrderID)
	if err == nil {
		if existing.LastSequence >= event.Sequence() {
			return nil
		}
	} else if !errors.Is(err, ErrOrderNotFound) {
		return fmt.Errorf("failed to get order: %w", err)
	}

	order := &OrderView{
		OrderID:       event.OrderID,
		ClientOrderID: event.ClientOrderID,
		AccountID:     event.AccountID,
		Symbol:        event.Symbol(),
		Side:          string(event.Side),
		Price:         event.Price,
		Quantity:      event.Quantity,
		RemainingQty:  event.Quantity, // Initially all quantity is remaining
		FilledQty:     0,
		Status:        OrderStatusNew,
		CreatedAt:     event.OccurredAt(),
		UpdatedAt:     event.OccurredAt(),
		LastSequence:  event.Sequence(),
	}

	return p.orderRepo.Save(ctx, order)
}

// projectOrderMatched updates order views and creates a trade view
func (p *Projector) projectOrderMatched(ctx context.Context, event *matching.OrderMatchedEvent) error {
	now := event.OccurredAt()
	seq := event.Sequence()

	// Update maker order
	makerOrder, err := p.orderRepo.GetByID(ctx, event.MakerOrderID)
	if err != nil {
		return fmt.Errorf("failed to get maker order: %w", err)
	}

	// Update taker order
	takerOrder, err := p.orderRepo.GetByID(ctx, event.TakerOrderID)
	if err != nil {
		return fmt.Errorf("failed to get taker order: %w", err)
	}
	makerOrder = applyMatchToOrder(makerOrder, event.Quantity, now, seq)
	takerOrder = applyMatchToOrder(takerOrder, event.Quantity, now, seq)
	if makerOrder == nil || takerOrder == nil {
		return fmt.Errorf("failed to apply match to order state")
	}
	if makerOrder.RemainingQty < 0 || takerOrder.RemainingQty < 0 {
		return fmt.Errorf("invalid match result: negative remaining quantity")
	}
	if makerOrder.FilledQty > makerOrder.Quantity || takerOrder.FilledQty > takerOrder.Quantity {
		return fmt.Errorf("invalid match result: filled quantity exceeds order quantity")
	}
	if err := p.orderRepo.Save(ctx, makerOrder); err != nil {
		return fmt.Errorf("failed to update maker order: %w", err)
	}
	if err := p.orderRepo.Save(ctx, takerOrder); err != nil {
		return fmt.Errorf("failed to update taker order: %w", err)
	}

	// Create trade view
	trade := &TradeView{
		TradeID:        event.TradeID,
		Symbol:         event.Symbol(),
		MakerOrderID:   event.MakerOrderID,
		TakerOrderID:   event.TakerOrderID,
		MakerAccountID: makerOrder.AccountID,
		TakerAccountID: takerOrder.AccountID,
		Price:          event.Price,
		Quantity:       event.Quantity,
		OccurredAt:     event.OccurredAt(),
		Sequence:       seq,
	}

	return p.tradeRepo.Save(ctx, trade)
}

// projectOrderCanceled updates order status to canceled
func (p *Projector) projectOrderCanceled(ctx context.Context, event *matching.OrderCanceledEvent) error {
	order, err := p.orderRepo.GetByID(ctx, event.OrderID)
	if err != nil {
		return fmt.Errorf("failed to get order: %w", err)
	}
	if order.LastSequence >= event.Sequence() {
		return nil
	}

	order.Status = OrderStatusCanceled
	order.UpdatedAt = event.OccurredAt()
	order.LastSequence = event.Sequence()

	return p.orderRepo.Save(ctx, order)
}

func applyMatchToOrder(order *OrderView, matchQty int64, at time.Time, seq int64) *OrderView {
	if order == nil {
		return nil
	}
	// Idempotent retry: this event has already been applied to this order.
	if order.LastSequence >= seq {
		return order
	}

	order.FilledQty += matchQty
	order.RemainingQty -= matchQty
	order.UpdatedAt = at
	order.LastSequence = seq
	if order.RemainingQty == 0 {
		order.Status = OrderStatusFilled
	} else {
		order.Status = OrderStatusPartiallyFilled
	}
	return order
}
