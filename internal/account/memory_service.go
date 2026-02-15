package account

import (
	"fmt"
	"sync"
)

// MemoryService is an in-memory implementation of the account service
type MemoryService struct {
	mu       sync.RWMutex
	balances map[string]map[string]*Balance // accountID -> asset -> Balance
	freezes  map[string]*FreezeRecord       // orderID -> FreezeRecord
}

// FreezeRecord tracks frozen funds for an order
type FreezeRecord struct {
	AccountID string
	Asset     string
	Amount    int64
}

// NewMemoryService creates a new in-memory account service
func NewMemoryService() *MemoryService {
	return &MemoryService{
		balances: make(map[string]map[string]*Balance),
		freezes:  make(map[string]*FreezeRecord),
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

	// Determine which asset to freeze and how much
	var assetToFreeze string
	var amountToFreeze int64

	if intent.Side == "BUY" {
		// BUY freezes QUOTE (price * quantity)
		assetToFreeze = quote
		// Check for overflow
		if intent.PriceInt > 0 && intent.QtyInt > 0 {
			if intent.QtyInt > (1<<63-1)/intent.PriceInt {
				return ErrInvalidAmount
			}
		}
		amountToFreeze = intent.PriceInt * intent.QtyInt
	} else {
		// SELL freezes BASE (quantity)
		assetToFreeze = base
		amountToFreeze = intent.QtyInt
	}

	if amountToFreeze <= 0 {
		return ErrInvalidAmount
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if this order has already been frozen (idempotency)
	if existingFreeze, exists := s.freezes[intent.OrderID]; exists {
		// Verify it's the same account and amount
		if existingFreeze.AccountID == intent.AccountID &&
			existingFreeze.Asset == assetToFreeze &&
			existingFreeze.Amount == amountToFreeze {
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
		AccountID: intent.AccountID,
		Asset:     assetToFreeze,
		Amount:    amountToFreeze,
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

	// Unfreeze the funds
	balance.Frozen -= freeze.Amount
	balance.Available += freeze.Amount

	// Remove freeze record
	delete(s.freezes, intent.OrderID)

	return nil
}

// ApplyTrade applies balance changes after a trade execution
// Week 4: minimal implementation
func (s *MemoryService) ApplyTrade(intent TradeIntent) error {
	// Parse symbol
	base, quote, err := ParseSymbol(intent.Symbol)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Calculate trade amounts
	quoteAmount := intent.PriceInt * intent.QuantityInt
	baseAmount := intent.QuantityInt

	// Update buyer balances: pay QUOTE, receive BASE
	buyerBalances := s.getOrCreateAccountBalances(intent.BuyerAccountID)
	buyerQuote := s.getOrCreateAssetBalance(buyerBalances, quote)
	buyerBase := s.getOrCreateAssetBalance(buyerBalances, base)

	// Buyer: reduce frozen QUOTE, increase available BASE
	if buyerQuote.Frozen >= quoteAmount {
		buyerQuote.Frozen -= quoteAmount
	}
	buyerBase.Available += baseAmount

	// Update seller balances: pay BASE, receive QUOTE
	sellerBalances := s.getOrCreateAccountBalances(intent.SellerAccountID)
	sellerBase := s.getOrCreateAssetBalance(sellerBalances, base)
	sellerQuote := s.getOrCreateAssetBalance(sellerBalances, quote)

	// Seller: reduce frozen BASE, increase available QUOTE
	if sellerBase.Frozen >= baseAmount {
		sellerBase.Frozen -= baseAmount
	}
	sellerQuote.Available += quoteAmount

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
