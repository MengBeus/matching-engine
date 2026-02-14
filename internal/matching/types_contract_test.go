package matching

import (
	"strings"
	"testing"
)

// TestPlaceOrderRequestContract 测试下单请求合同
func TestPlaceOrderRequestContract(t *testing.T) {
	tests := []struct {
		name    string
		req     PlaceOrderRequest
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid request",
			req: PlaceOrderRequest{
				OrderID:       "ord_001",
				ClientOrderID: "cli_001",
				AccountID:     "acc_001",
				Symbol:        "BTC-USDT",
				Side:          SideBuy,
				PriceInt:      4300000,
				QuantityInt:   10000000,
			},
			wantErr: false,
		},
		{
			name: "missing order_id",
			req: PlaceOrderRequest{
				ClientOrderID: "cli_001",
				AccountID:     "acc_001",
				Symbol:        "BTC-USDT",
				Side:          SideBuy,
				PriceInt:      4300000,
				QuantityInt:   10000000,
			},
			wantErr: true,
			errMsg:  "order_id required",
		},
		{
			name: "missing client_order_id",
			req: PlaceOrderRequest{
				OrderID:     "ord_001",
				AccountID:   "acc_001",
				Symbol:      "BTC-USDT",
				Side:        SideBuy,
				PriceInt:    4300000,
				QuantityInt: 10000000,
			},
			wantErr: true,
			errMsg:  "client_order_id required",
		},
		{
			name: "missing account_id",
			req: PlaceOrderRequest{
				OrderID:       "ord_001",
				ClientOrderID: "cli_001",
				Symbol:        "BTC-USDT",
				Side:          SideBuy,
				PriceInt:      4300000,
				QuantityInt:   10000000,
			},
			wantErr: true,
			errMsg:  "account_id required",
		},
		{
			name: "missing symbol",
			req: PlaceOrderRequest{
				OrderID:       "ord_001",
				ClientOrderID: "cli_001",
				AccountID:     "acc_001",
				Side:          SideBuy,
				PriceInt:      4300000,
				QuantityInt:   10000000,
			},
			wantErr: true,
			errMsg:  "symbol required",
		},
		{
			name: "invalid price (zero)",
			req: PlaceOrderRequest{
				OrderID:       "ord_001",
				ClientOrderID: "cli_001",
				AccountID:     "acc_001",
				Symbol:        "BTC-USDT",
				Side:          SideBuy,
				PriceInt:      0,
				QuantityInt:   10000000,
			},
			wantErr: true,
			errMsg:  "price must be positive",
		},
		{
			name: "invalid price (negative)",
			req: PlaceOrderRequest{
				OrderID:       "ord_001",
				ClientOrderID: "cli_001",
				AccountID:     "acc_001",
				Symbol:        "BTC-USDT",
				Side:          SideBuy,
				PriceInt:      -100,
				QuantityInt:   10000000,
			},
			wantErr: true,
			errMsg:  "price must be positive",
		},
		{
			name: "invalid quantity (zero)",
			req: PlaceOrderRequest{
				OrderID:       "ord_001",
				ClientOrderID: "cli_001",
				AccountID:     "acc_001",
				Symbol:        "BTC-USDT",
				Side:          SideBuy,
				PriceInt:      4300000,
				QuantityInt:   0,
			},
			wantErr: true,
			errMsg:  "quantity must be positive",
		},
		{
			name: "invalid quantity (negative)",
			req: PlaceOrderRequest{
				OrderID:       "ord_001",
				ClientOrderID: "cli_001",
				AccountID:     "acc_001",
				Symbol:        "BTC-USDT",
				Side:          SideBuy,
				PriceInt:      4300000,
				QuantityInt:   -100,
			},
			wantErr: true,
			errMsg:  "quantity must be positive",
		},
		{
			name: "invalid side",
			req: PlaceOrderRequest{
				OrderID:       "ord_001",
				ClientOrderID: "cli_001",
				AccountID:     "acc_001",
				Symbol:        "BTC-USDT",
				Side:          Side("INVALID"),
				PriceInt:      4300000,
				QuantityInt:   10000000,
			},
			wantErr: true,
			errMsg:  "invalid side",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.req.Validate()
			if tt.wantErr {
				if err == nil {
					t.Errorf("expected error but got nil")
					return
				}
				if tt.errMsg != "" && !strings.Contains(err.Error(), tt.errMsg) {
					t.Errorf("expected error message containing %q, got %q", tt.errMsg, err.Error())
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			}
		})
	}
}

// TestCancelOrderRequestContract 测试撤单请求合同
func TestCancelOrderRequestContract(t *testing.T) {
	tests := []struct {
		name    string
		req     CancelOrderRequest
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid request",
			req: CancelOrderRequest{
				OrderID:   "ord_001",
				AccountID: "acc_001",
				Symbol:    "BTC-USDT",
			},
			wantErr: false,
		},
		{
			name: "missing order_id",
			req: CancelOrderRequest{
				AccountID: "acc_001",
				Symbol:    "BTC-USDT",
			},
			wantErr: true,
			errMsg:  "order_id required",
		},
		{
			name: "missing account_id",
			req: CancelOrderRequest{
				OrderID: "ord_001",
				Symbol:  "BTC-USDT",
			},
			wantErr: true,
			errMsg:  "account_id required",
		},
		{
			name: "missing symbol",
			req: CancelOrderRequest{
				OrderID:   "ord_001",
				AccountID: "acc_001",
			},
			wantErr: true,
			errMsg:  "symbol required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.req.Validate()
			if tt.wantErr {
				if err == nil {
					t.Errorf("expected error but got nil")
					return
				}
				if tt.errMsg != "" && !strings.Contains(err.Error(), tt.errMsg) {
					t.Errorf("expected error message containing %q, got %q", tt.errMsg, err.Error())
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			}
		})
	}
}

// TestSideContract 测试买卖方向枚举合同
func TestSideContract(t *testing.T) {
	// 确保枚举值不被修改
	if SideBuy != "BUY" {
		t.Errorf("SideBuy value changed: expected BUY, got %s", SideBuy)
	}
	if SideSell != "SELL" {
		t.Errorf("SideSell value changed: expected SELL, got %s", SideSell)
	}
}

// TestOrderStatusContract 测试订单状态枚举合同
func TestOrderStatusContract(t *testing.T) {
	// 确保枚举值不被修改
	if OrderStatusNew != "NEW" {
		t.Errorf("OrderStatusNew value changed: expected NEW, got %s", OrderStatusNew)
	}
	if OrderStatusPartiallyFilled != "PARTIALLY_FILLED" {
		t.Errorf("OrderStatusPartiallyFilled value changed: expected PARTIALLY_FILLED, got %s", OrderStatusPartiallyFilled)
	}
	if OrderStatusFilled != "FILLED" {
		t.Errorf("OrderStatusFilled value changed: expected FILLED, got %s", OrderStatusFilled)
	}
	if OrderStatusCanceled != "CANCELED" {
		t.Errorf("OrderStatusCanceled value changed: expected CANCELED, got %s", OrderStatusCanceled)
	}
}

// TestEventContract 测试事件接口合同
func TestEventContract(t *testing.T) {
	// 确保所有事件类型实现了Event接口
	var _ Event = (*OrderAcceptedEvent)(nil)
	var _ Event = (*OrderMatchedEvent)(nil)
	var _ Event = (*OrderCanceledEvent)(nil)
}

// TestEventTypeContract 测试事件类型名称合同
func TestEventTypeContract(t *testing.T) {
	acceptedEvent := &OrderAcceptedEvent{}
	if acceptedEvent.EventType() != "OrderAccepted" {
		t.Errorf("OrderAcceptedEvent type changed: expected OrderAccepted, got %s", acceptedEvent.EventType())
	}

	matchedEvent := &OrderMatchedEvent{}
	if matchedEvent.EventType() != "OrderMatched" {
		t.Errorf("OrderMatchedEvent type changed: expected OrderMatched, got %s", matchedEvent.EventType())
	}

	canceledEvent := &OrderCanceledEvent{}
	if canceledEvent.EventType() != "OrderCanceled" {
		t.Errorf("OrderCanceledEvent type changed: expected OrderCanceled, got %s", canceledEvent.EventType())
	}
}

// TestCancelReasonContract 测试撤单原因枚举合同
func TestCancelReasonContract(t *testing.T) {
	if CancelReasonUser != "USER" {
		t.Errorf("CancelReasonUser value changed: expected USER, got %s", CancelReasonUser)
	}
	if CancelReasonSystem != "SYSTEM" {
		t.Errorf("CancelReasonSystem value changed: expected SYSTEM, got %s", CancelReasonSystem)
	}
	if CancelReasonExpired != "EXPIRED" {
		t.Errorf("CancelReasonExpired value changed: expected EXPIRED, got %s", CancelReasonExpired)
	}
}
