package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"matching-engine/internal/account"
	"matching-engine/internal/engine"
)

func TestPlaceOrder_Success(t *testing.T) {
	// Setup
	accountSvc := account.NewMemoryService()
	eng := engine.NewEngine(&engine.EngineConfig{
		ShardCount:     1,
		QueueSize:      100,
		IdempotencyTTL: time.Minute,
	})
	defer eng.Close()

	router := NewRouter(accountSvc, eng)

	// Initialize account with USDT balance
	err := accountSvc.SetBalance("acc1", "USDT", account.Balance{Available: 10000000, Frozen: 0})
	if err != nil {
		t.Fatalf("SetBalance failed: %v", err)
	}

	// Create place order request
	reqBody := PlaceOrderRequest{
		ClientOrderID:  "client_order_1",
		AccountID:      "acc1",
		Symbol:         "BTC-USDT",
		Side:           "BUY",
		Price:          "43000",
		Quantity:       "100",
		IdempotencyKey: "idem_key_1",
	}

	body, _ := json.Marshal(reqBody)
	req := httptest.NewRequest(http.MethodPost, "/v1/orders", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	// Execute
	router.ServeHTTP(w, req)

	// Assert
	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
		t.Logf("Response body: %s", w.Body.String())
	}

	var resp PlaceOrderResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if resp.OrderID == "" {
		t.Error("Expected order_id to be set")
	}
	if resp.ClientOrderID != "client_order_1" {
		t.Errorf("Expected client_order_id 'client_order_1', got '%s'", resp.ClientOrderID)
	}
	if resp.Status != "NEW" {
		t.Errorf("Expected status 'NEW', got '%s'", resp.Status)
	}

	// Verify balance frozen
	balance, err := accountSvc.GetBalance("acc1", "USDT")
	if err != nil {
		t.Fatalf("GetBalance failed: %v", err)
	}

	expectedFrozen := int64(4300000) // 43000 * 100
	if balance.Frozen != expectedFrozen {
		t.Errorf("Expected frozen %d, got %d", expectedFrozen, balance.Frozen)
	}
}

func TestPlaceOrder_InsufficientBalance(t *testing.T) {
	// Setup
	accountSvc := account.NewMemoryService()
	eng := engine.NewEngine(&engine.EngineConfig{
		ShardCount:     1,
		QueueSize:      100,
		IdempotencyTTL: time.Minute,
	})
	defer eng.Close()

	router := NewRouter(accountSvc, eng)

	// Initialize account with insufficient USDT balance
	err := accountSvc.SetBalance("acc1", "USDT", account.Balance{Available: 1000000, Frozen: 0})
	if err != nil {
		t.Fatalf("SetBalance failed: %v", err)
	}

	// Create place order request that requires more than available
	reqBody := PlaceOrderRequest{
		ClientOrderID:  "client_order_1",
		AccountID:      "acc1",
		Symbol:         "BTC-USDT",
		Side:           "BUY",
		Price:          "43000",
		Quantity:       "100", // Requires 4,300,000 but only have 1,000,000
		IdempotencyKey: "idem_key_1",
	}

	body, _ := json.Marshal(reqBody)
	req := httptest.NewRequest(http.MethodPost, "/v1/orders", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	// Execute
	router.ServeHTTP(w, req)

	// Assert
	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected status 400, got %d", w.Code)
	}

	var errResp ErrorResponse
	if err := json.NewDecoder(w.Body).Decode(&errResp); err != nil {
		t.Fatalf("Failed to decode error response: %v", err)
	}

	if errResp.Code != string(ErrorCodeInsufficientBalance) {
		t.Errorf("Expected error code '%s', got '%s'", ErrorCodeInsufficientBalance, errResp.Code)
	}

	// Verify balance unchanged
	balance, err := accountSvc.GetBalance("acc1", "USDT")
	if err != nil {
		t.Fatalf("GetBalance failed: %v", err)
	}

	if balance.Frozen != 0 {
		t.Errorf("Expected frozen 0, got %d", balance.Frozen)
	}
	if balance.Available != 1000000 {
		t.Errorf("Expected available 1000000, got %d", balance.Available)
	}
}

func TestPlaceOrder_InvalidRequest(t *testing.T) {
	// Setup
	accountSvc := account.NewMemoryService()
	eng := engine.NewEngine(&engine.EngineConfig{
		ShardCount:     1,
		QueueSize:      100,
		IdempotencyTTL: time.Minute,
	})
	defer eng.Close()

	router := NewRouter(accountSvc, eng)

	tests := []struct {
		name    string
		reqBody PlaceOrderRequest
		wantErr string
	}{
		{
			name: "missing client_order_id",
			reqBody: PlaceOrderRequest{
				AccountID:      "acc1",
				Symbol:         "BTC-USDT",
				Side:           "BUY",
				Price:          "43000",
				Quantity:       "100",
				IdempotencyKey: "idem_key_1",
			},
			wantErr: "client_order_id required",
		},
		{
			name: "missing account_id",
			reqBody: PlaceOrderRequest{
				ClientOrderID:  "client_order_1",
				Symbol:         "BTC-USDT",
				Side:           "BUY",
				Price:          "43000",
				Quantity:       "100",
				IdempotencyKey: "idem_key_1",
			},
			wantErr: "account_id required",
		},
		{
			name: "invalid side",
			reqBody: PlaceOrderRequest{
				ClientOrderID:  "client_order_1",
				AccountID:      "acc1",
				Symbol:         "BTC-USDT",
				Side:           "INVALID",
				Price:          "43000",
				Quantity:       "100",
				IdempotencyKey: "idem_key_1",
			},
			wantErr: "side must be BUY or SELL",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body, _ := json.Marshal(tt.reqBody)
			req := httptest.NewRequest(http.MethodPost, "/v1/orders", bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()

			router.ServeHTTP(w, req)

			if w.Code != http.StatusBadRequest {
				t.Errorf("Expected status 400, got %d", w.Code)
			}

			var errResp ErrorResponse
			if err := json.NewDecoder(w.Body).Decode(&errResp); err != nil {
				t.Fatalf("Failed to decode error response: %v", err)
			}

			if errResp.Code != string(ErrorCodeInvalidArgument) {
				t.Errorf("Expected error code '%s', got '%s'", ErrorCodeInvalidArgument, errResp.Code)
			}
		})
	}
}

func TestCancelOrder_Success(t *testing.T) {
	// Setup
	accountSvc := account.NewMemoryService()
	eng := engine.NewEngine(&engine.EngineConfig{
		ShardCount:     1,
		QueueSize:      100,
		IdempotencyTTL: time.Minute,
	})
	defer eng.Close()

	router := NewRouter(accountSvc, eng)

	// Initialize account with USDT balance
	err := accountSvc.SetBalance("acc1", "USDT", account.Balance{Available: 10000000, Frozen: 0})
	if err != nil {
		t.Fatalf("SetBalance failed: %v", err)
	}

	// First, place an order
	placeReqBody := PlaceOrderRequest{
		ClientOrderID:  "client_order_1",
		AccountID:      "acc1",
		Symbol:         "BTC-USDT",
		Side:           "BUY",
		Price:          "43000",
		Quantity:       "100",
		IdempotencyKey: "idem_key_1",
	}

	body, _ := json.Marshal(placeReqBody)
	placeReq := httptest.NewRequest(http.MethodPost, "/v1/orders", bytes.NewReader(body))
	placeReq.Header.Set("Content-Type", "application/json")
	placeW := httptest.NewRecorder()

	router.ServeHTTP(placeW, placeReq)

	var placeResp PlaceOrderResponse
	if err := json.NewDecoder(placeW.Body).Decode(&placeResp); err != nil {
		t.Fatalf("Failed to decode place response: %v", err)
	}

	orderID := placeResp.OrderID

	// Verify balance frozen
	balanceAfterPlace, _ := accountSvc.GetBalance("acc1", "USDT")
	if balanceAfterPlace.Frozen != 4300000 {
		t.Errorf("Expected frozen 4300000 after place, got %d", balanceAfterPlace.Frozen)
	}

	// Now cancel the order
	cancelReq := httptest.NewRequest(
		http.MethodDelete,
		fmt.Sprintf("/v1/orders/%s?account_id=acc1&symbol=BTC-USDT", orderID),
		nil,
	)
	cancelW := httptest.NewRecorder()

	router.ServeHTTP(cancelW, cancelReq)

	// Assert
	if cancelW.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", cancelW.Code)
		t.Logf("Response body: %s", cancelW.Body.String())
	}

	var cancelResp CancelOrderResponse
	if err := json.NewDecoder(cancelW.Body).Decode(&cancelResp); err != nil {
		t.Fatalf("Failed to decode cancel response: %v", err)
	}

	if cancelResp.Status != "CANCELED" {
		t.Errorf("Expected status 'CANCELED', got '%s'", cancelResp.Status)
	}

	// Verify balance unfrozen
	balanceAfterCancel, err := accountSvc.GetBalance("acc1", "USDT")
	if err != nil {
		t.Fatalf("GetBalance failed: %v", err)
	}

	if balanceAfterCancel.Frozen != 0 {
		t.Errorf("Expected frozen 0 after cancel, got %d", balanceAfterCancel.Frozen)
	}
	if balanceAfterCancel.Available != 10000000 {
		t.Errorf("Expected available 10000000 after cancel, got %d", balanceAfterCancel.Available)
	}
}

func TestCancelOrder_NotFound(t *testing.T) {
	// Setup
	accountSvc := account.NewMemoryService()
	eng := engine.NewEngine(&engine.EngineConfig{
		ShardCount:     1,
		QueueSize:      100,
		IdempotencyTTL: time.Minute,
	})
	defer eng.Close()

	router := NewRouter(accountSvc, eng)

	// Try to cancel non-existent order
	cancelReq := httptest.NewRequest(
		http.MethodDelete,
		"/v1/orders/nonexistent_order?account_id=acc1&symbol=BTC-USDT",
		nil,
	)
	cancelW := httptest.NewRecorder()

	router.ServeHTTP(cancelW, cancelReq)

	// Assert
	if cancelW.Code != http.StatusNotFound {
		t.Errorf("Expected status 404, got %d", cancelW.Code)
	}

	var errResp ErrorResponse
	if err := json.NewDecoder(cancelW.Body).Decode(&errResp); err != nil {
		t.Fatalf("Failed to decode error response: %v", err)
	}

	if errResp.Code != string(ErrorCodeOrderNotFound) {
		t.Errorf("Expected error code '%s', got '%s'", ErrorCodeOrderNotFound, errResp.Code)
	}
}

func TestQueryOrder_Success(t *testing.T) {
	// Setup
	accountSvc := account.NewMemoryService()
	eng := engine.NewEngine(&engine.EngineConfig{
		ShardCount:     1,
		QueueSize:      100,
		IdempotencyTTL: time.Minute,
	})
	defer eng.Close()

	router := NewRouter(accountSvc, eng)

	// Initialize account with USDT balance
	err := accountSvc.SetBalance("acc1", "USDT", account.Balance{Available: 10000000, Frozen: 0})
	if err != nil {
		t.Fatalf("SetBalance failed: %v", err)
	}

	// First, place an order
	placeReqBody := PlaceOrderRequest{
		ClientOrderID:  "client_order_1",
		AccountID:      "acc1",
		Symbol:         "BTC-USDT",
		Side:           "BUY",
		Price:          "43000",
		Quantity:       "100",
		IdempotencyKey: "idem_key_1",
	}

	body, _ := json.Marshal(placeReqBody)
	placeReq := httptest.NewRequest(http.MethodPost, "/v1/orders", bytes.NewReader(body))
	placeReq.Header.Set("Content-Type", "application/json")
	placeW := httptest.NewRecorder()

	router.ServeHTTP(placeW, placeReq)

	var placeResp PlaceOrderResponse
	if err := json.NewDecoder(placeW.Body).Decode(&placeResp); err != nil {
		t.Fatalf("Failed to decode place response: %v", err)
	}

	orderID := placeResp.OrderID

	// Now query the order
	queryReq := httptest.NewRequest(
		http.MethodGet,
		fmt.Sprintf("/v1/orders/%s?account_id=acc1&symbol=BTC-USDT", orderID),
		nil,
	)
	queryW := httptest.NewRecorder()

	router.ServeHTTP(queryW, queryReq)

	// Assert
	if queryW.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", queryW.Code)
		t.Logf("Response body: %s", queryW.Body.String())
	}

	var queryResp QueryOrderResponse
	if err := json.NewDecoder(queryW.Body).Decode(&queryResp); err != nil {
		t.Fatalf("Failed to decode query response: %v", err)
	}

	if queryResp.OrderID != orderID {
		t.Errorf("Expected order_id '%s', got '%s'", orderID, queryResp.OrderID)
	}
	if queryResp.ClientOrderID != "client_order_1" {
		t.Errorf("Expected client_order_id 'client_order_1', got '%s'", queryResp.ClientOrderID)
	}
	if queryResp.Status != "NEW" {
		t.Errorf("Expected status 'NEW', got '%s'", queryResp.Status)
	}
	if queryResp.Price != "43000" {
		t.Errorf("Expected price '43000', got '%s'", queryResp.Price)
	}
	if queryResp.Quantity != "100" {
		t.Errorf("Expected quantity '100', got '%s'", queryResp.Quantity)
	}
}

func TestQueryOrder_NotFound(t *testing.T) {
	// Setup
	accountSvc := account.NewMemoryService()
	eng := engine.NewEngine(&engine.EngineConfig{
		ShardCount:     1,
		QueueSize:      100,
		IdempotencyTTL: time.Minute,
	})
	defer eng.Close()

	router := NewRouter(accountSvc, eng)

	// Try to query non-existent order
	queryReq := httptest.NewRequest(
		http.MethodGet,
		"/v1/orders/nonexistent_order?account_id=acc1&symbol=BTC-USDT",
		nil,
	)
	queryW := httptest.NewRecorder()

	router.ServeHTTP(queryW, queryReq)

	// Assert
	if queryW.Code != http.StatusNotFound {
		t.Errorf("Expected status 404, got %d", queryW.Code)
	}

	var errResp ErrorResponse
	if err := json.NewDecoder(queryW.Body).Decode(&errResp); err != nil {
		t.Fatalf("Failed to decode error response: %v", err)
	}

	if errResp.Code != string(ErrorCodeOrderNotFound) {
		t.Errorf("Expected error code '%s', got '%s'", ErrorCodeOrderNotFound, errResp.Code)
	}
}

func TestIdempotency(t *testing.T) {
	// Setup
	accountSvc := account.NewMemoryService()
	eng := engine.NewEngine(&engine.EngineConfig{
		ShardCount:     1,
		QueueSize:      100,
		IdempotencyTTL: time.Minute,
	})
	defer eng.Close()

	router := NewRouter(accountSvc, eng)

	// Initialize account with USDT balance
	err := accountSvc.SetBalance("acc1", "USDT", account.Balance{Available: 10000000, Frozen: 0})
	if err != nil {
		t.Fatalf("SetBalance failed: %v", err)
	}

	// Create place order request
	reqBody := PlaceOrderRequest{
		ClientOrderID:  "client_order_1",
		AccountID:      "acc1",
		Symbol:         "BTC-USDT",
		Side:           "BUY",
		Price:          "43000",
		Quantity:       "100",
		IdempotencyKey: "idem_key_1",
	}

	// First request
	body1, _ := json.Marshal(reqBody)
	req1 := httptest.NewRequest(http.MethodPost, "/v1/orders", bytes.NewReader(body1))
	req1.Header.Set("Content-Type", "application/json")
	w1 := httptest.NewRecorder()

	router.ServeHTTP(w1, req1)

	if w1.Code != http.StatusOK {
		t.Fatalf("First request failed with status %d", w1.Code)
	}

	var resp1 PlaceOrderResponse
	if err := json.NewDecoder(w1.Body).Decode(&resp1); err != nil {
		t.Fatalf("Failed to decode first response: %v", err)
	}

	// Second request with same idempotency key
	body2, _ := json.Marshal(reqBody)
	req2 := httptest.NewRequest(http.MethodPost, "/v1/orders", bytes.NewReader(body2))
	req2.Header.Set("Content-Type", "application/json")
	w2 := httptest.NewRecorder()

	router.ServeHTTP(w2, req2)

	if w2.Code != http.StatusOK {
		t.Fatalf("Second request failed with status %d", w2.Code)
	}

	var resp2 PlaceOrderResponse
	if err := json.NewDecoder(w2.Body).Decode(&resp2); err != nil {
		t.Fatalf("Failed to decode second response: %v", err)
	}

	// Both responses should have the same order_id (idempotency)
	if resp1.OrderID != resp2.OrderID {
		t.Errorf("Expected same order_id for idempotent requests, got '%s' and '%s'", resp1.OrderID, resp2.OrderID)
	}

	// Verify balance only frozen once
	balance, err := accountSvc.GetBalance("acc1", "USDT")
	if err != nil {
		t.Fatalf("GetBalance failed: %v", err)
	}

	expectedFrozen := int64(4300000) // Should only be frozen once
	if balance.Frozen != expectedFrozen {
		t.Errorf("Expected frozen %d (only once), got %d", expectedFrozen, balance.Frozen)
	}
}

func TestIdempotencyKeyScopedByAccountAndSymbolForOrderID(t *testing.T) {
	accountSvc := account.NewMemoryService()
	eng := engine.NewEngine(&engine.EngineConfig{
		ShardCount:     2,
		QueueSize:      100,
		IdempotencyTTL: time.Minute,
	})
	defer eng.Close()

	router := NewRouter(accountSvc, eng)
	_ = accountSvc.SetBalance("acc1", "USDT", account.Balance{Available: 10000000})
	_ = accountSvc.SetBalance("acc2", "USDT", account.Balance{Available: 10000000})

	makeReq := func(accountID string) PlaceOrderResponse {
		body, _ := json.Marshal(PlaceOrderRequest{
			ClientOrderID:  "client_" + accountID,
			AccountID:      accountID,
			Symbol:         "BTC-USDT",
			Side:           "BUY",
			Price:          "43000",
			Quantity:       "100",
			IdempotencyKey: "same_key",
		})
		req := httptest.NewRequest(http.MethodPost, "/v1/orders", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("expected 200 for %s, got %d body=%s", accountID, w.Code, w.Body.String())
		}
		var resp PlaceOrderResponse
		if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
			t.Fatalf("decode response failed: %v", err)
		}
		return resp
	}

	resp1 := makeReq("acc1")
	resp2 := makeReq("acc2")
	if resp1.OrderID == resp2.OrderID {
		t.Fatalf("order_id should differ across accounts with same idempotency key")
	}
}

func TestPlaceOrder_InvalidSymbolMappedToInvalidArgument(t *testing.T) {
	accountSvc := account.NewMemoryService()
	eng := engine.NewEngine(&engine.EngineConfig{
		ShardCount:     1,
		QueueSize:      100,
		IdempotencyTTL: time.Minute,
	})
	defer eng.Close()

	router := NewRouter(accountSvc, eng)
	_ = accountSvc.SetBalance("acc1", "USDT", account.Balance{Available: 10000000})

	body, _ := json.Marshal(PlaceOrderRequest{
		ClientOrderID:  "client1",
		AccountID:      "acc1",
		Symbol:         "BTCUSDT", // invalid format
		Side:           "BUY",
		Price:          "43000",
		Quantity:       "100",
		IdempotencyKey: "idem_invalid_symbol",
	})
	req := httptest.NewRequest(http.MethodPost, "/v1/orders", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d body=%s", w.Code, w.Body.String())
	}
	var errResp ErrorResponse
	if err := json.NewDecoder(w.Body).Decode(&errResp); err != nil {
		t.Fatalf("decode error response failed: %v", err)
	}
	if errResp.Code != string(ErrorCodeInvalidArgument) {
		t.Fatalf("expected INVALID_ARGUMENT, got %s", errResp.Code)
	}
}

func TestQueryClosedOrderAfterCancel(t *testing.T) {
	accountSvc := account.NewMemoryService()
	eng := engine.NewEngine(&engine.EngineConfig{
		ShardCount:     1,
		QueueSize:      100,
		IdempotencyTTL: time.Minute,
	})
	defer eng.Close()

	router := NewRouter(accountSvc, eng)
	_ = accountSvc.SetBalance("acc1", "USDT", account.Balance{Available: 10000000})

	placeBody, _ := json.Marshal(PlaceOrderRequest{
		ClientOrderID:  "client_order_closed",
		AccountID:      "acc1",
		Symbol:         "BTC-USDT",
		Side:           "BUY",
		Price:          "43000",
		Quantity:       "100",
		IdempotencyKey: "idem_closed",
	})
	placeReq := httptest.NewRequest(http.MethodPost, "/v1/orders", bytes.NewReader(placeBody))
	placeReq.Header.Set("Content-Type", "application/json")
	placeW := httptest.NewRecorder()
	router.ServeHTTP(placeW, placeReq)
	if placeW.Code != http.StatusOK {
		t.Fatalf("place failed: %d %s", placeW.Code, placeW.Body.String())
	}
	var placeResp PlaceOrderResponse
	if err := json.NewDecoder(placeW.Body).Decode(&placeResp); err != nil {
		t.Fatalf("decode place response failed: %v", err)
	}

	cancelReq := httptest.NewRequest(
		http.MethodDelete,
		fmt.Sprintf("/v1/orders/%s?account_id=acc1&symbol=BTC-USDT", placeResp.OrderID),
		nil,
	)
	cancelW := httptest.NewRecorder()
	router.ServeHTTP(cancelW, cancelReq)
	if cancelW.Code != http.StatusOK {
		t.Fatalf("cancel failed: %d %s", cancelW.Code, cancelW.Body.String())
	}

	queryReq := httptest.NewRequest(
		http.MethodGet,
		fmt.Sprintf("/v1/orders/%s?account_id=acc1&symbol=BTC-USDT", placeResp.OrderID),
		nil,
	)
	queryW := httptest.NewRecorder()
	router.ServeHTTP(queryW, queryReq)
	if queryW.Code != http.StatusOK {
		t.Fatalf("query closed order failed: %d %s", queryW.Code, queryW.Body.String())
	}

	var queryResp QueryOrderResponse
	if err := json.NewDecoder(queryW.Body).Decode(&queryResp); err != nil {
		t.Fatalf("decode query response failed: %v", err)
	}
	if queryResp.Status != "CANCELED" {
		t.Fatalf("expected CANCELED, got %s", queryResp.Status)
	}
}
