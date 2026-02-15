package engine

import (
	"time"

	"matching-engine/internal/matching"
)

// CommandType represents the type of command
type CommandType string

const (
	CommandTypePlace  CommandType = "PLACE"
	CommandTypeCancel CommandType = "CANCEL"
)

// CommandEnvelope wraps a command with metadata
type CommandEnvelope struct {
	CommandID      string      // Unique command ID
	CommandType    CommandType // PLACE / CANCEL
	IdempotencyKey string      // Idempotency key for deduplication
	Symbol         string      // Trading symbol
	AccountID      string      // Account ID
	PayloadHash    string      // Hash of payload for conflict detection
	Payload        any         // Actual command payload (PlaceOrderRequest or CancelOrderRequest)
	CreatedAt      time.Time   // Command creation time
}

// ErrorCode represents command execution error codes
type ErrorCode string

const (
	ErrorCodeNone                 ErrorCode = ""
	ErrorCodeDuplicateRequest     ErrorCode = "DUPLICATE_REQUEST"
	ErrorCodeInvalidArgument      ErrorCode = "INVALID_ARGUMENT"
	ErrorCodeOrderNotFound        ErrorCode = "ORDER_NOT_FOUND"
	ErrorCodeOrderAlreadyFilled   ErrorCode = "ORDER_ALREADY_FILLED"
	ErrorCodeOrderAlreadyCanceled ErrorCode = "ORDER_ALREADY_CANCELED"
	ErrorCodeUnauthorized         ErrorCode = "UNAUTHORIZED"
)

// CommandExecResult represents the result of command execution
type CommandExecResult struct {
	Result    *matching.CommandResult // Matching engine result
	ErrorCode ErrorCode               // Error code if execution failed
	Err       error                   // Detailed error message
}
