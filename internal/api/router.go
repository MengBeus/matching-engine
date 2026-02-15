package api

import (
	"net/http"

	"matching-engine/internal/account"
	"matching-engine/internal/engine"
)

// Router sets up HTTP routes for the API
type Router struct {
	handler *Handler
	mux     *http.ServeMux
}

// NewRouter creates a new API router
func NewRouter(accountSvc account.Service, engine *engine.Engine) *Router {
	handler := NewHandler(accountSvc, engine)
	mux := http.NewServeMux()

	router := &Router{
		handler: handler,
		mux:     mux,
	}

	router.setupRoutes()
	return router
}

// setupRoutes configures all HTTP routes
func (r *Router) setupRoutes() {
	// Order endpoints
	r.mux.HandleFunc("/v1/orders", r.routeOrders)
	r.mux.HandleFunc("/v1/orders/", r.routeOrderByID)
}

// routeOrders handles /v1/orders endpoint
func (r *Router) routeOrders(w http.ResponseWriter, req *http.Request) {
	switch req.Method {
	case http.MethodPost:
		r.handler.PlaceOrder(w, req)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// routeOrderByID handles /v1/orders/{order_id} endpoint
func (r *Router) routeOrderByID(w http.ResponseWriter, req *http.Request) {
	switch req.Method {
	case http.MethodGet:
		r.handler.QueryOrder(w, req)
	case http.MethodDelete:
		r.handler.CancelOrder(w, req)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// ServeHTTP implements http.Handler interface
func (r *Router) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	r.mux.ServeHTTP(w, req)
}

// Handler returns the underlying HTTP handler
func (r *Router) Handler() http.Handler {
	return r.mux
}
