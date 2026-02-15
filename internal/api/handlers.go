package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"matching-engine/internal/account"
	"matching-engine/internal/engine"
	"matching-engine/internal/matching"
	"matching-engine/internal/symbolspec"

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
	requestID := generateRequestID()

	// Parse request body
	var req PlaceOrderRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErrorResponse(w, http.StatusBadRequest, requestID, ErrorCodeInvalidArgument, "invalid request body")
		return
	}

	// Validate required fields
	if err := h.validatePlaceOrderRequest(&req); err != nil {
		writeErrorResponse(w, http.StatusBadRequest, requestID, ErrorCodeInvalidArgument, err.Error())
		return
	}

	// Parse symbol precision spec.
	spec, err := symbolspec.Get(req.Symbol)
	if err != nil {
		writeErrorResponse(w, http.StatusBadRequest, requestID, ErrorCodeInvalidArgument, err.Error())
		return
	}

	// Convert decimal price/quantity strings into fixed-scale int64.
	priceInt, err := symbolspec.ParseScaledInt(req.Price, spec.PriceScale)
	if err != nil {
		writeErrorResponse(w, http.StatusBadRequest, requestID, ErrorCodeInvalidArgument, fmt.Sprintf("invalid price: %v", err))
		return
	}

	qtyInt, err := symbolspec.ParseScaledInt(req.Quantity, spec.QuantityScale)
	if err != nil {
		writeErrorResponse(w, http.StatusBadRequest, requestID, ErrorCodeInvalidArgument, fmt.Sprintf("invalid quantity: %v", err))
		return
	}

	if spec.PriceTickInt > 0 && priceInt%spec.PriceTickInt != 0 {
		writeErrorResponse(w, http.StatusBadRequest, requestID, ErrorCodeInvalidArgument, "price does not match tick size")
		return
	}
	if spec.QtyStepInt > 0 && qtyInt%spec.QtyStepInt != 0 {
		writeErrorResponse(w, http.StatusBadRequest, requestID, ErrorCodeInvalidArgument, "quantity does not match lot size")
		return
	}

	// Generate deterministic order ID in scoped namespace to avoid cross-account collisions.
	orderID := generateOrderIDFromIdempotencyKey(req.AccountID, req.Symbol, req.IdempotencyKey)

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
		writeMappedErrorResponse(w, statusCode, requestID, errResp)
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
		writeErrorResponse(w, http.StatusInternalServerError, requestID, ErrorCodeInternalError, "failed to compute payload hash")
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
		writeMappedErrorResponse(w, statusCode, requestID, errResp)
		return
	}

	// Success: convert result to response
	matchResult, ok := result.Result.(*matching.CommandResult)
	if !ok {
		// Rollback freeze
		h.rollbackFreeze(orderID, req.AccountID, req.Symbol)
		writeErrorResponse(w, http.StatusInternalServerError, requestID, ErrorCodeInternalError, "invalid result type")
		return
	}
	if err := h.applyTrades(matchResult.Trades); err != nil {
		writeErrorResponse(w, http.StatusInternalServerError, requestID, ErrorCodeInternalError, "failed to settle trade balances")
		return
	}

	// Build response
	resp := h.buildPlaceOrderResponse(orderID, &req, priceInt, qtyInt, matchResult, spec)
	writeSuccessResponse(w, http.StatusOK, requestID, resp)
}

// CancelOrder handles DELETE /v1/orders/{order_id}
func (h *Handler) CancelOrder(w http.ResponseWriter, r *http.Request) {
	requestID := generateRequestID()

	// Extract order_id from URL path
	orderID := extractOrderID(r.URL.Path)
	if orderID == "" {
		writeErrorResponse(w, http.StatusBadRequest, requestID, ErrorCodeInvalidArgument, "order_id required")
		return
	}

	// Get query parameters
	accountID := r.URL.Query().Get("account_id")
	symbol := r.URL.Query().Get("symbol")

	if accountID == "" {
		writeErrorResponse(w, http.StatusBadRequest, requestID, ErrorCodeInvalidArgument, "account_id required")
		return
	}
	if symbol == "" {
		writeErrorResponse(w, http.StatusBadRequest, requestID, ErrorCodeInvalidArgument, "symbol required")
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
		writeErrorResponse(w, http.StatusInternalServerError, requestID, ErrorCodeInternalError, "failed to compute payload hash")
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
		writeMappedErrorResponse(w, statusCode, requestID, errResp)
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
		writeErrorResponse(w, http.StatusInternalServerError, requestID, ErrorCodeInternalError, "invalid result type")
		return
	}

	resp := h.buildCancelOrderResponse(orderID, symbol, matchResult)
	writeSuccessResponse(w, http.StatusOK, requestID, resp)
}

// QueryOrder handles GET /v1/orders/{order_id}
func (h *Handler) QueryOrder(w http.ResponseWriter, r *http.Request) {
	requestID := generateRequestID()

	// Extract order_id from URL path
	orderID := extractOrderID(r.URL.Path)
	if orderID == "" {
		writeErrorResponse(w, http.StatusBadRequest, requestID, ErrorCodeInvalidArgument, "order_id required")
		return
	}

	// Get query parameters
	accountID := r.URL.Query().Get("account_id")
	symbol := r.URL.Query().Get("symbol")

	if accountID == "" {
		writeErrorResponse(w, http.StatusBadRequest, requestID, ErrorCodeInvalidArgument, "account_id required")
		return
	}
	if symbol == "" {
		writeErrorResponse(w, http.StatusBadRequest, requestID, ErrorCodeInvalidArgument, "symbol required")
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
		writeErrorResponse(w, http.StatusInternalServerError, requestID, ErrorCodeInternalError, "failed to compute payload hash")
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
		writeMappedErrorResponse(w, statusCode, requestID, errResp)
		return
	}

	// Extract snapshot from result
	snapshot, ok := result.Result.(*matching.OrderSnapshot)
	if !ok {
		writeErrorResponse(w, http.StatusInternalServerError, requestID, ErrorCodeInternalError, "invalid result type")
		return
	}

	// Build response
	resp := h.buildQueryOrderResponse(snapshot)
	writeSuccessResponse(w, http.StatusOK, requestID, resp)
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
	if strings.TrimSpace(req.IdempotencyKey) == "" {
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

func (h *Handler) applyTrades(trades []matching.Trade) error {
	for _, trade := range trades {
		intent := account.TradeIntent{
			TradeID:     trade.TradeID,
			Symbol:      trade.Symbol,
			PriceInt:    trade.Price,
			QuantityInt: trade.Quantity,
		}

		if trade.MakerSide == matching.SideBuy {
			intent.BuyerAccountID = trade.MakerAccountID
			intent.BuyerOrderID = trade.MakerOrderID
			intent.SellerAccountID = trade.TakerAccountID
			intent.SellerOrderID = trade.TakerOrderID
		} else {
			intent.BuyerAccountID = trade.TakerAccountID
			intent.BuyerOrderID = trade.TakerOrderID
			intent.SellerAccountID = trade.MakerAccountID
			intent.SellerOrderID = trade.MakerOrderID
		}

		if err := h.accountSvc.ApplyTrade(intent); err != nil {
			return err
		}
	}
	return nil
}

func (h *Handler) buildPlaceOrderResponse(orderID string, req *PlaceOrderRequest, priceInt, qtyInt int64, result *matching.CommandResult, spec symbolspec.Spec) PlaceOrderResponse {
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
			Price:     symbolspec.FormatScaledInt(trade.Price, spec.PriceScale),
			Quantity:  symbolspec.FormatScaledInt(trade.Quantity, spec.QuantityScale),
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
		Price:         symbolspec.FormatScaledInt(priceInt, spec.PriceScale),
		Quantity:      symbolspec.FormatScaledInt(qtyInt, spec.QuantityScale),
		Status:        status,
		CreatedAt:     time.Now(),
		Trades:        trades,
	}
}

func (h *Handler) buildCancelOrderResponse(orderID, symbol string, result *matching.CommandResult) CancelOrderResponse {
	// Get final status change
	var remainingQty, filledQty int64
	status := "CANCELED"
	spec, err := symbolspec.Get(symbol)
	if err != nil {
		spec = symbolspec.Spec{QuantityScale: 0}
	}

	if len(result.OrderStatusChanges) > 0 {
		lastChange := result.OrderStatusChanges[len(result.OrderStatusChanges)-1]
		status = string(lastChange.NewStatus)
		remainingQty = lastChange.RemainingQty
		filledQty = lastChange.FilledQty
	}

	return CancelOrderResponse{
		OrderID:      orderID,
		Status:       status,
		RemainingQty: symbolspec.FormatScaledInt(remainingQty, spec.QuantityScale),
		FilledQty:    symbolspec.FormatScaledInt(filledQty, spec.QuantityScale),
	}
}

func (h *Handler) buildQueryOrderResponse(snapshot *matching.OrderSnapshot) QueryOrderResponse {
	spec, err := symbolspec.Get(snapshot.Symbol)
	if err != nil {
		spec = symbolspec.Spec{}
	}
	return QueryOrderResponse{
		OrderID:       snapshot.OrderID,
		ClientOrderID: snapshot.ClientOrderID,
		AccountID:     snapshot.AccountID,
		Symbol:        snapshot.Symbol,
		Side:          string(snapshot.Side),
		Price:         symbolspec.FormatScaledInt(snapshot.Price, spec.PriceScale),
		Quantity:      symbolspec.FormatScaledInt(snapshot.Quantity, spec.QuantityScale),
		RemainingQty:  symbolspec.FormatScaledInt(snapshot.RemainingQty, spec.QuantityScale),
		FilledQty:     symbolspec.FormatScaledInt(snapshot.FilledQty, spec.QuantityScale),
		Status:        string(snapshot.Status),
		CreatedAt:     snapshot.CreatedAt,
	}
}

// Utility functions

func generateOrderID() string {
	return "ord_" + uuid.New().String()
}

func generateOrderIDFromIdempotencyKey(accountID, symbol, idempotencyKey string) string {
	// Scope deterministic ID by account+symbol+idempotency_key to avoid cross-account collisions.
	keyMaterial := accountID + "|" + symbol + "|" + idempotencyKey
	namespace := uuid.MustParse("6ba7b810-9dad-11d1-80b4-00c04fd430c8") // DNS namespace UUID
	deterministicUUID := uuid.NewSHA1(namespace, []byte(keyMaterial))
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

func writeSuccessResponse(w http.ResponseWriter, statusCode int, requestID string, data interface{}) {
	writeJSONResponse(w, statusCode, SuccessResponse{
		Code:      "OK",
		Data:      data,
		RequestID: requestID,
	})
}

func writeMappedErrorResponse(w http.ResponseWriter, statusCode int, requestID string, errResp ErrorResponse) {
	errResp.RequestID = requestID
	writeJSONResponse(w, statusCode, errResp)
}

func writeErrorResponse(w http.ResponseWriter, statusCode int, requestID string, code ErrorCode, message string) {
	writeJSONResponse(w, statusCode, ErrorResponse{
		Code:      string(code),
		Message:   message,
		RequestID: requestID,
	})
}

func generateRequestID() string {
	return "req_" + uuid.NewString()
}
