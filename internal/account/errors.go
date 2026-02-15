package account

import (
	"errors"
	"fmt"
)

// Common errors
var (
	ErrInsufficientBalance = errors.New("insufficient balance")
	ErrAccountNotFound     = errors.New("account not found")
	ErrInvalidSymbol       = errors.New("invalid symbol format")
	ErrInvalidAmount       = errors.New("invalid amount")
	ErrOrderNotFound       = errors.New("order not found")
)

// InsufficientBalanceError represents insufficient balance error with details
type InsufficientBalanceError struct {
	AccountID string
	Asset     string
	Required  int64
	Available int64
}

func (e *InsufficientBalanceError) Error() string {
	return fmt.Sprintf("insufficient balance: account=%s asset=%s required=%d available=%d",
		e.AccountID, e.Asset, e.Required, e.Available)
}

func (e *InsufficientBalanceError) Is(target error) bool {
	return target == ErrInsufficientBalance
}
