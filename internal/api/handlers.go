package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"matching-engine/internal/account"
	"matching-engine/internal/engine"
	"matching-engine/internal/matching"

	"github.com/google/uuid"
)

// Handler handles HTTP requests for the order API
type Handler struct {
	accountSvc account.Service
	engine     *engine.Engine
}

// NewHandler creates a new API handler
func NewHandler(accountSvc account.Service, engine *engine.Engine) *Handler {
	return &Handler{
		accountSvc: accountSvc,
		engine:     engine,
	}
}

// PlaceOrder handles POST /v1/orders
func (h *Handler) PlaceOrder(w http.ResponseWriter, r *http.Request) {
	// Parse request body
	var req PlaceOrderRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErrorResponse(w, http.StatusBadRequest, ErrorCodeInvalidArgument, "invalid request body")
		return
	}

	// Validate required fields
	if err := h.validatePlaceOrderRequest(&req); err != nil {
		writeErrorResponse(w, http.StatusBadRequest, ErrorCodeInvalidArgument, err.Error())
		return
	}

	// Convert price and quantity from decimal strings to int64
	priceInt, err := decimalToInt64(req.Price)
	if err != nil {
		writeErrorResponse(w, http.StatusBadRequest, ErrorCodeInvalidArgument, fmt.Sprintf("invalid price: %v", err))
		return
	}

	qtyInt, err := decimalToInt64(req.Quantity)
	if err != nil {
		writeErrorResponse(w, http.StatusBadRequest, ErrorCodeInvalidArgument, fmt.Sprintf("invalid quantity: %v", err))
		return
	}

	// Generate system order ID (deterministic based on idempotency key for proper idempotency)
	orderID := generateOrderIDFromIdempotencyKey(req.IdempotencyKey)

	// Step 1: Check and freeze balance
	placeIntent := account.PlaceIntent{
		AccountID: req.AccountID,
		OrderID:   orderID,
		Symbol:    req.Symbol,
		Side:      req.Side,
		PriceInt:  priceInt,
		QtyInt:    qtyInt,
	}

	if err := h.accountSvc.CheckAndFreezeForPlace(placeIntent); err != nil {
		statusCode, errResp := MapErrorToHTTP(err)
		writeJSONResponse(w, statusCode, errResp)
		return
	}

	// Step 2: Submit to engine
	placeReq := &matching.PlaceOrderRequest{
		OrderID:       orderID,
		ClientOrderID: req.ClientOrderID,
		AccountID:     req.AccountID,
		Symbol:        req.Symbol,
		Side:          matching.Side(req.Side),
		PriceInt:      priceInt,
		QuantityInt:   qtyInt,
	}

	payloadHash, err := engine.ComputePayloadHash(placeReq)
	if err != nil {
		// Rollback freeze
		h.rollbackFreeze(orderID, req.AccountID, req.Symbol)
		writeErrorResponse(w, http.StatusInternalServerError, ErrorCodeInternalError, "failed to compute payload hash")
		return
	}

	envelope := &engine.CommandEnvelope{
		CommandID:      generateCommandID(),
		CommandType:    engine.CommandTypePlace,
		IdempotencyKey: req.IdempotencyKey,
		Symbol:         req.Symbol,
		AccountID:      req.AccountID,
		PayloadHash:    payloadHash,
		Payload:        placeReq,
		CreatedAt:      time.Now(),
	}

	result := h.engine.Submit(envelope)

	// Step 3: Handle engine result
	if result.ErrorCode != engine.ErrorCodeNone {
		// Rollback freeze
		h.rollbackFreeze(orderID, req.AccountID, req.Symbol)
		statusCode, errResp := MapEngineErrorToHTTP(result.ErrorCode, result.Err)
		writeJSONResponse(w, statusCode, errResp)
		return
	}

	// Success: convert result to response
	matchResult, ok := result.Result.(*matching.CommandResult)
	if !ok {
		// Rollback freeze
		h.rollbackFreeze(orderID, req.AccountID, req.Symbol)
		writeErrorResponse(w, http.StatusInternalServerError, ErrorCodeInternalError, "invalid result type")
		return
	}

	// Build response
	resp := h.buildPlaceOrderResponse(orderID, &req, priceInt, qtyInt, matchResult)
	writeJSONResponse(w, http.StatusOK, resp)
}

// CancelOrder handles DELETE /v1/orders/{order_id}
func (h *Handler) CancelOrder(w http.ResponseWriter, r *http.Request) {
	// Extract order_id from URL path
	orderID := extractOrderID(r.URL.Path)
	if orderID == "" {
		writeErrorResponse(w, http.StatusBadRequest, ErrorCodeInvalidArgument, "order_id required")
		return
	}

	// Get query parameters
	accountID := r.URL.Query().Get("account_id")
	symbol := r.URL.Query().Get("symbol")

	if accountID == "" {
		writeErrorResponse(w, http.StatusBadRequest, ErrorCodeInvalidArgument, "account_id required")
		return
	}
	if symbol == "" {
		writeErrorResponse(w, http.StatusBadRequest, ErrorCodeInvalidArgument, "symbol required")
		return
	}

	// Submit cancel command to engine
	cancelReq := &matching.CancelOrderRequest{
		OrderID:   orderID,
		AccountID: accountID,
		Symbol:    symbol,
	}

	payloadHash, err := engine.ComputePayloadHash(cancelReq)
	if err != nil {
		writeErrorResponse(w, http.StatusInternalServerError, ErrorCodeInternalError, "failed to compute payload hash")
		return
	}

	envelope := &engine.CommandEnvelope{
		CommandID:      generateCommandID(),
		CommandType:    engine.CommandTypeCancel,
		IdempotencyKey: fmt.Sprintf("cancel_%s_%s", accountID, orderID),
		Symbol:         symbol,
		AccountID:      accountID,
		PayloadHash:    payloadHash,
		Payload:        cancelReq,
		CreatedAt:      time.Now(),
	}

	result := h.engine.Submit(envelope)

	// Handle engine result
	if result.ErrorCode != engine.ErrorCodeNone {
		statusCode, errResp := MapEngineErrorToHTTP(result.ErrorCode, result.Err)
		writeJSONResponse(w, statusCode, errResp)
		return
	}

	// Release frozen funds
	cancelIntent := account.CancelIntent{
		AccountID: accountID,
		OrderID:   orderID,
		Symbol:    symbol,
	}
	if err := h.accountSvc.ReleaseOnCancel(cancelIntent); err != nil {
		// Log error but don't fail the request (order is already canceled in engine)
		// In production, this should be handled with retry or compensation
	}

	// Build response
	matchResult, ok := result.Result.(*matching.CommandResult)
	if !ok {
		writeErrorResponse(w, http.StatusInternalServerError, ErrorCodeInternalError, "invalid result type")
		return
	}

	resp := h.buildCancelOrderResponse(orderID, matchResult)
	writeJSONResponse(w, http.StatusOK, resp)
}

// QueryOrder handles GET /v1/orders/{order_id}
func (h *Handler) QueryOrder(w http.ResponseWriter, r *http.Request) {
	// Extract order_id from URL path
	orderID := extractOrderID(r.URL.Path)
	if orderID == "" {
		writeErrorResponse(w, http.StatusBadRequest, ErrorCodeInvalidArgument, "order_id required")
		return
	}

	// Get query parameters
	accountID := r.URL.Query().Get("account_id")
	symbol := r.URL.Query().Get("symbol")

	if accountID == "" {
		writeErrorResponse(w, http.StatusBadRequest, ErrorCodeInvalidArgument, "account_id required")
		return
	}
	if symbol == "" {
		writeErrorResponse(w, http.StatusBadRequest, ErrorCodeInvalidArgument, "symbol required")
		return
	}

	// Submit query command to engine
	queryReq := &matching.QueryOrderRequest{
		OrderID:   orderID,
		AccountID: accountID,
		Symbol:    symbol,
	}

	payloadHash, err := engine.ComputePayloadHash(queryReq)
	if err != nil {
		writeErrorResponse(w, http.StatusInternalServerError, ErrorCodeInternalError, "failed to compute payload hash")
		return
	}

	envelope := &engine.CommandEnvelope{
		CommandID:      generateCommandID(),
		CommandType:    engine.CommandTypeQuery,
		IdempotencyKey: fmt.Sprintf("query_%s_%s_%d", accountID, orderID, time.Now().UnixNano()),
		Symbol:         symbol,
		AccountID:      accountID,
		PayloadHash:    payloadHash,
		Payload:        queryReq,
		CreatedAt:      time.Now(),
	}

	result := h.engine.Submit(envelope)

	// Handle engine result
	if result.ErrorCode != engine.ErrorCodeNone {
		statusCode, errResp := MapEngineErrorToHTTP(result.ErrorCode, result.Err)
		writeJSONResponse(w, statusCode, errResp)
		return
	}

	// Extract snapshot from result
	snapshot, ok := result.Result.(*matching.OrderSnapshot)
	if !ok {
		writeErrorResponse(w, http.StatusInternalServerError, ErrorCodeInternalError, "invalid result type")
		return
	}

	// Build response
	resp := h.buildQueryOrderResponse(snapshot)
	writeJSONResponse(w, http.StatusOK, resp)
}

// Helper functions

func (h *Handler) validatePlaceOrderRequest(req *PlaceOrderRequest) error {
	if req.ClientOrderID == "" {
		return fmt.Errorf("client_order_id required")
	}
	if req.AccountID == "" {
		return fmt.Errorf("account_id required")
	}
	if req.Symbol == "" {
		return fmt.Errorf("symbol required")
	}
	if req.Side != "BUY" && req.Side != "SELL" {
		return fmt.Errorf("side must be BUY or SELL")
	}
	if req.Price == "" {
		return fmt.Errorf("price required")
	}
	if req.Quantity == "" {
		return fmt.Errorf("quantity required")
	}
	if req.IdempotencyKey == "" {
		return fmt.Errorf("idempotency_key required")
	}
	return nil
}

func (h *Handler) rollbackFreeze(orderID, accountID, symbol string) {
	cancelIntent := account.CancelIntent{
		AccountID: accountID,
		OrderID:   orderID,
		Symbol:    symbol,
	}
	_ = h.accountSvc.ReleaseOnCancel(cancelIntent)
}

func (h *Handler) buildPlaceOrderResponse(orderID string, req *PlaceOrderRequest, priceInt, qtyInt int64, result *matching.CommandResult) PlaceOrderResponse {
	// Determine final status
	status := "NEW"
	if len(result.OrderStatusChanges) > 0 {
		status = string(result.OrderStatusChanges[len(result.OrderStatusChanges)-1].NewStatus)
	}

	// Convert trades
	trades := make([]TradeDTO, 0, len(result.Trades))
	for _, trade := range result.Trades {
		// Determine which side this order is in the trade
		side := req.Side
		if trade.TakerOrderID == orderID {
			side = string(trade.TakerSide)
		} else {
			side = string(trade.MakerSide)
		}

		trades = append(trades, TradeDTO{
			TradeID:   trade.TradeID,
			Price:     int64ToDecimal(trade.Price),
			Quantity:  int64ToDecimal(trade.Quantity),
			Side:      side,
			Timestamp: trade.OccurredAt,
		})
	}

	return PlaceOrderResponse{
		OrderID:       orderID,
		ClientOrderID: req.ClientOrderID,
		AccountID:     req.AccountID,
		Symbol:        req.Symbol,
		Side:          req.Side,
		Price:         req.Price,
		Quantity:      req.Quantity,
		Status:        status,
		CreatedAt:     time.Now(),
		Trades:        trades,
	}
}

func (h *Handler) buildCancelOrderResponse(orderID string, result *matching.CommandResult) CancelOrderResponse {
	// Get final status change
	var remainingQty, filledQty int64
	status := "CANCELED"

	if len(result.OrderStatusChanges) > 0 {
		lastChange := result.OrderStatusChanges[len(result.OrderStatusChanges)-1]
		status = string(lastChange.NewStatus)
		remainingQty = lastChange.RemainingQty
		filledQty = lastChange.FilledQty
	}

	return CancelOrderResponse{
		OrderID:      orderID,
		Status:       status,
		RemainingQty: int64ToDecimal(remainingQty),
		FilledQty:    int64ToDecimal(filledQty),
	}
}

func (h *Handler) buildQueryOrderResponse(snapshot *matching.OrderSnapshot) QueryOrderResponse {
	return QueryOrderResponse{
		OrderID:       snapshot.OrderID,
		ClientOrderID: snapshot.ClientOrderID,
		AccountID:     snapshot.AccountID,
		Symbol:        snapshot.Symbol,
		Side:          string(snapshot.Side),
		Price:         int64ToDecimal(snapshot.Price),
		Quantity:      int64ToDecimal(snapshot.Quantity),
		RemainingQty:  int64ToDecimal(snapshot.RemainingQty),
		FilledQty:     int64ToDecimal(snapshot.FilledQty),
		Status:        string(snapshot.Status),
		CreatedAt:     snapshot.CreatedAt,
	}
}

// Utility functions

func decimalToInt64(s string) (int64, error) {
	// Remove any whitespace
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("empty string")
	}

	// Parse as float and convert to int64 (assuming no decimal places for now)
	// In production, you'd handle decimal precision properly
	val, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, err
	}

	if val <= 0 {
		return 0, fmt.Errorf("value must be positive")
	}

	return val, nil
}

func int64ToDecimal(val int64) string {
	return strconv.FormatInt(val, 10)
}

func generateOrderID() string {
	return "ord_" + uuid.New().String()
}

func generateOrderIDFromIdempotencyKey(idempotencyKey string) string {
	// Generate deterministic UUID from idempotency key using UUID v5 (SHA-1 based)
	// This ensures the same idempotency key always generates the same order ID
	namespace := uuid.MustParse("6ba7b810-9dad-11d1-80b4-00c04fd430c8") // DNS namespace UUID
	deterministicUUID := uuid.NewSHA1(namespace, []byte(idempotencyKey))
	return "ord_" + deterministicUUID.String()
}

func generateCommandID() string {
	return "cmd_" + uuid.New().String()
}

func extractOrderID(path string) string {
	// Extract order_id from path like /v1/orders/{order_id}
	parts := strings.Split(path, "/")
	if len(parts) >= 4 {
		return parts[3]
	}
	return ""
}

func writeJSONResponse(w http.ResponseWriter, statusCode int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(data)
}

func writeErrorResponse(w http.ResponseWriter, statusCode int, code ErrorCode, message string) {
	writeJSONResponse(w, statusCode, ErrorResponse{
		Code:    string(code),
		Message: message,
	})
}
