package account

// Service defines the account service interface
type Service interface {
	// CheckAndFreezeForPlace checks balance and freezes funds for placing an order
	// Returns ErrInsufficientBalance if balance is insufficient
	CheckAndFreezeForPlace(intent PlaceIntent) error

	// ReleaseOnCancel releases frozen funds when an order is canceled
	ReleaseOnCancel(intent CancelIntent) error

	// ApplyTrade applies balance changes after a trade execution
	// Week 4: minimal implementation, can be enhanced later
	ApplyTrade(intent TradeIntent) error

	// GetBalance returns the balance for a specific account and asset
	GetBalance(accountID, asset string) (Balance, error)

	// SetBalance sets the balance for a specific account and asset (for testing/initialization)
	SetBalance(accountID, asset string, balance Balance) error
}
