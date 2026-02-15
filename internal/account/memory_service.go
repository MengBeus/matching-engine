package account

import (
	"fmt"
	"sync"
)

// MemoryService is an in-memory implementation of the account service
type MemoryService struct {
	mu            sync.RWMutex
	balances      map[string]map[string]*Balance // accountID -> asset -> Balance
	freezes       map[string]*FreezeRecord       // orderID -> FreezeRecord
	appliedTrades map[string]struct{}            // symbol|tradeID -> applied marker
}

// FreezeRecord tracks frozen funds for an order
type FreezeRecord struct {
	AccountID            string
	Asset                string
	OriginalFrozenAmount int64
	FrozenAmount         int64
}

// NewMemoryService creates a new in-memory account service
func NewMemoryService() *MemoryService {
	return &MemoryService{
		balances:      make(map[string]map[string]*Balance),
		freezes:       make(map[string]*FreezeRecord),
		appliedTrades: make(map[string]struct{}),
	}
}

// CheckAndFreezeForPlace checks balance and freezes funds for placing an order
func (s *MemoryService) CheckAndFreezeForPlace(intent PlaceIntent) error {
	if err := intent.Validate(); err != nil {
		return err
	}

	// Parse symbol to get base and quote assets
	base, quote, err := ParseSymbol(intent.Symbol)
	if err != nil {
		return err
	}

	assetToFreeze, amountToFreeze, err := freezeAmountForPlace(base, quote, intent.Side, intent.PriceInt, intent.QtyInt)
	if err != nil {
		return err
	}
	if amountToFreeze <= 0 {
		return ErrInvalidAmount
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if this order has already been frozen (idempotency)
	if existingFreeze, exists := s.freezes[intent.OrderID]; exists {
		// Verify it's the same request shape; treat as idempotent.
		if existingFreeze.AccountID == intent.AccountID &&
			existingFreeze.Asset == assetToFreeze &&
			existingFreeze.OriginalFrozenAmount == amountToFreeze {
			// Already frozen, return success (idempotent)
			return nil
		}
		// Different parameters for same order ID - this is an error
		return fmt.Errorf("order_id %s already exists with different parameters", intent.OrderID)
	}

	// Get or create account balances
	accountBalances, exists := s.balances[intent.AccountID]
	if !exists {
		accountBalances = make(map[string]*Balance)
		s.balances[intent.AccountID] = accountBalances
	}

	// Get or create asset balance
	balance, exists := accountBalances[assetToFreeze]
	if !exists {
		balance = &Balance{Available: 0, Frozen: 0}
		accountBalances[assetToFreeze] = balance
	}

	// Check if sufficient balance
	if balance.Available < amountToFreeze {
		return &InsufficientBalanceError{
			AccountID: intent.AccountID,
			Asset:     assetToFreeze,
			Required:  amountToFreeze,
			Available: balance.Available,
		}
	}

	// Freeze the funds
	balance.Available -= amountToFreeze
	balance.Frozen += amountToFreeze

	// Record the freeze
	s.freezes[intent.OrderID] = &FreezeRecord{
		AccountID:            intent.AccountID,
		Asset:                assetToFreeze,
		OriginalFrozenAmount: amountToFreeze,
		FrozenAmount:         amountToFreeze,
	}

	return nil
}

// ReleaseOnCancel releases frozen funds when an order is canceled
func (s *MemoryService) ReleaseOnCancel(intent CancelIntent) error {
	if err := intent.Validate(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Find the freeze record
	freeze, exists := s.freezes[intent.OrderID]
	if !exists {
		// Order not found in freeze records - might have been fully filled
		// This is not an error, just return nil
		return nil
	}

	// Verify account matches
	if freeze.AccountID != intent.AccountID {
		return fmt.Errorf("account mismatch: freeze belongs to %s, cancel from %s",
			freeze.AccountID, intent.AccountID)
	}

	// Get account balances
	accountBalances, exists := s.balances[freeze.AccountID]
	if !exists {
		return ErrAccountNotFound
	}

	// Get asset balance
	balance, exists := accountBalances[freeze.Asset]
	if !exists {
		return fmt.Errorf("asset balance not found: %s", freeze.Asset)
	}

	if freeze.FrozenAmount <= 0 {
		return nil
	}
	if balance.Frozen < freeze.FrozenAmount {
		return fmt.Errorf("frozen balance underflow for order %s", intent.OrderID)
	}

	// Unfreeze remaining reserved funds.
	balance.Frozen -= freeze.FrozenAmount
	balance.Available += freeze.FrozenAmount
	freeze.FrozenAmount = 0

	return nil
}

// ApplyTrade applies balance changes after a trade execution
// Week 4: minimal implementation
func (s *MemoryService) ApplyTrade(intent TradeIntent) error {
	if intent.TradeID == "" {
		return ErrInvalidAmount
	}
	if intent.BuyerAccountID == "" || intent.SellerAccountID == "" {
		return ErrInvalidAmount
	}
	if intent.BuyerOrderID == "" || intent.SellerOrderID == "" {
		return ErrInvalidAmount
	}
	if intent.PriceInt <= 0 || intent.QuantityInt <= 0 {
		return ErrInvalidAmount
	}

	// Parse symbol
	base, quote, err := ParseSymbol(intent.Symbol)
	if err != nil {
		return err
	}
	if intent.QuantityInt > (1<<63-1)/intent.PriceInt {
		return ErrInvalidAmount
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	tradeKey := intent.Symbol + "|" + intent.TradeID
	if _, exists := s.appliedTrades[tradeKey]; exists {
		return nil
	}

	// Calculate trade amounts
	quoteAmount := intent.PriceInt * intent.QuantityInt
	baseAmount := intent.QuantityInt

	// Update buyer balances: pay QUOTE, receive BASE
	buyerBalances := s.getOrCreateAccountBalances(intent.BuyerAccountID)
	buyerQuote := s.getOrCreateAssetBalance(buyerBalances, quote)
	buyerBase := s.getOrCreateAssetBalance(buyerBalances, base)

	// Buyer: reduce frozen QUOTE, increase available BASE.
	if buyerQuote.Frozen < quoteAmount {
		return fmt.Errorf("insufficient buyer frozen quote for trade")
	}
	buyerQuote.Frozen -= quoteAmount
	buyerBase.Available += baseAmount

	// Update seller balances: pay BASE, receive QUOTE
	sellerBalances := s.getOrCreateAccountBalances(intent.SellerAccountID)
	sellerBase := s.getOrCreateAssetBalance(sellerBalances, base)
	sellerQuote := s.getOrCreateAssetBalance(sellerBalances, quote)

	// Seller: reduce frozen BASE, increase available QUOTE.
	if sellerBase.Frozen < baseAmount {
		return fmt.Errorf("insufficient seller frozen base for trade")
	}
	sellerBase.Frozen -= baseAmount
	sellerQuote.Available += quoteAmount

	// Update per-order freeze trackers for future cancel release correctness.
	buyerFreeze, buyerExists := s.freezes[intent.BuyerOrderID]
	if buyerExists {
		if buyerFreeze.FrozenAmount < quoteAmount {
			return fmt.Errorf("buyer freeze record underflow for order %s", intent.BuyerOrderID)
		}
		buyerFreeze.FrozenAmount -= quoteAmount
	}

	sellerFreeze, sellerExists := s.freezes[intent.SellerOrderID]
	if sellerExists {
		if sellerFreeze.FrozenAmount < baseAmount {
			return fmt.Errorf("seller freeze record underflow for order %s", intent.SellerOrderID)
		}
		sellerFreeze.FrozenAmount -= baseAmount
	}
	s.appliedTrades[tradeKey] = struct{}{}

	return nil
}

// GetBalance returns the balance for a specific account and asset
func (s *MemoryService) GetBalance(accountID, asset string) (Balance, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	accountBalances, exists := s.balances[accountID]
	if !exists {
		return Balance{Available: 0, Frozen: 0}, nil
	}

	balance, exists := accountBalances[asset]
	if !exists {
		return Balance{Available: 0, Frozen: 0}, nil
	}

	return *balance, nil
}

// SetBalance sets the balance for a specific account and asset
func (s *MemoryService) SetBalance(accountID, asset string, balance Balance) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	accountBalances, exists := s.balances[accountID]
	if !exists {
		accountBalances = make(map[string]*Balance)
		s.balances[accountID] = accountBalances
	}

	accountBalances[asset] = &Balance{
		Available: balance.Available,
		Frozen:    balance.Frozen,
	}

	return nil
}

// Helper methods

func (s *MemoryService) getOrCreateAccountBalances(accountID string) map[string]*Balance {
	accountBalances, exists := s.balances[accountID]
	if !exists {
		accountBalances = make(map[string]*Balance)
		s.balances[accountID] = accountBalances
	}
	return accountBalances
}

func (s *MemoryService) getOrCreateAssetBalance(accountBalances map[string]*Balance, asset string) *Balance {
	balance, exists := accountBalances[asset]
	if !exists {
		balance = &Balance{Available: 0, Frozen: 0}
		accountBalances[asset] = balance
	}
	return balance
}

func freezeAmountForPlace(base, quote, side string, priceInt, qtyInt int64) (string, int64, error) {
	if side == "BUY" {
		if priceInt > 0 && qtyInt > 0 && qtyInt > (1<<63-1)/priceInt {
			return "", 0, ErrInvalidAmount
		}
		return quote, priceInt * qtyInt, nil
	}
	return base, qtyInt, nil
}
