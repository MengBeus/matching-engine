package api

import (
	"errors"
	"net/http"

	"matching-engine/internal/account"
	"matching-engine/internal/engine"
)

// ErrorCode represents unified API error codes
type ErrorCode string

const (
	ErrorCodeInsufficientBalance  ErrorCode = "INSUFFICIENT_BALANCE"
	ErrorCodeInvalidArgument      ErrorCode = "INVALID_ARGUMENT"
	ErrorCodeOrderNotFound        ErrorCode = "ORDER_NOT_FOUND"
	ErrorCodeOrderAlreadyFilled   ErrorCode = "ORDER_ALREADY_FILLED"
	ErrorCodeOrderAlreadyCanceled ErrorCode = "ORDER_ALREADY_CANCELED"
	ErrorCodeUnauthorized         ErrorCode = "UNAUTHORIZED"
	ErrorCodeDuplicateRequest     ErrorCode = "DUPLICATE_REQUEST"
	ErrorCodeInternalError        ErrorCode = "INTERNAL_ERROR"
)

// MapErrorToHTTP maps errors to HTTP status codes and error responses
func MapErrorToHTTP(err error) (int, ErrorResponse) {
	if err == nil {
		return http.StatusOK, ErrorResponse{}
	}

	// Check for account service errors
	var insufficientBalanceErr *account.InsufficientBalanceError
	if errors.As(err, &insufficientBalanceErr) {
		return http.StatusBadRequest, ErrorResponse{
			Code:    string(ErrorCodeInsufficientBalance),
			Message: err.Error(),
		}
	}

	if errors.Is(err, account.ErrInsufficientBalance) {
		return http.StatusBadRequest, ErrorResponse{
			Code:    string(ErrorCodeInsufficientBalance),
			Message: "insufficient balance",
		}
	}

	if errors.Is(err, account.ErrInvalidAmount) {
		return http.StatusBadRequest, ErrorResponse{
			Code:    string(ErrorCodeInvalidArgument),
			Message: "invalid amount",
		}
	}

	if errors.Is(err, account.ErrInvalidSymbol) {
		return http.StatusBadRequest, ErrorResponse{
			Code:    string(ErrorCodeInvalidArgument),
			Message: "invalid symbol format",
		}
	}

	if errors.Is(err, account.ErrAccountNotFound) {
		return http.StatusNotFound, ErrorResponse{
			Code:    string(ErrorCodeInvalidArgument),
			Message: "account not found",
		}
	}

	// Default to internal error
	return http.StatusInternalServerError, ErrorResponse{
		Code:    string(ErrorCodeInternalError),
		Message: err.Error(),
	}
}

// MapEngineErrorToHTTP maps engine error codes to HTTP status codes and error responses
func MapEngineErrorToHTTP(errorCode engine.ErrorCode, err error) (int, ErrorResponse) {
	switch errorCode {
	case engine.ErrorCodeNone:
		return http.StatusOK, ErrorResponse{}

	case engine.ErrorCodeInvalidArgument:
		return http.StatusBadRequest, ErrorResponse{
			Code:    string(ErrorCodeInvalidArgument),
			Message: getErrorMessage(err, "invalid argument"),
		}

	case engine.ErrorCodeOrderNotFound:
		return http.StatusNotFound, ErrorResponse{
			Code:    string(ErrorCodeOrderNotFound),
			Message: getErrorMessage(err, "order not found"),
		}

	case engine.ErrorCodeOrderAlreadyFilled:
		return http.StatusBadRequest, ErrorResponse{
			Code:    string(ErrorCodeOrderAlreadyFilled),
			Message: getErrorMessage(err, "order already filled"),
		}

	case engine.ErrorCodeOrderAlreadyCanceled:
		return http.StatusBadRequest, ErrorResponse{
			Code:    string(ErrorCodeOrderAlreadyCanceled),
			Message: getErrorMessage(err, "order already canceled"),
		}

	case engine.ErrorCodeUnauthorized:
		return http.StatusForbidden, ErrorResponse{
			Code:    string(ErrorCodeUnauthorized),
			Message: getErrorMessage(err, "unauthorized"),
		}

	case engine.ErrorCodeDuplicateRequest:
		return http.StatusConflict, ErrorResponse{
			Code:    string(ErrorCodeDuplicateRequest),
			Message: getErrorMessage(err, "duplicate request with different payload"),
		}

	default:
		return http.StatusInternalServerError, ErrorResponse{
			Code:    string(ErrorCodeInternalError),
			Message: getErrorMessage(err, "internal error"),
		}
	}
}

func getErrorMessage(err error, defaultMsg string) string {
	if err != nil {
		return err.Error()
	}
	return defaultMsg
}
