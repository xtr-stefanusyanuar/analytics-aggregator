package handler

import (
	"analytics-aggregator/internal/config"
	"analytics-aggregator/internal/core/domain"
	"analytics-aggregator/internal/ctxkey"
	"context"
	"net/http"
)

type HandlerFunc func(w http.ResponseWriter, r *http.Request) error

// EventProcessor is the part of the pipeline the handler depends on.
// It is defined here, where it is consumed, so tests can supply a fake.
type EventProcessor interface {
	ProcessAndStore(ctx context.Context, e *domain.Event) (int64, error)
}

type Handler struct {
	service EventProcessor
	config  *config.Config
}

func NewHandler(s EventProcessor, cfg *config.Config) *Handler {
	return &Handler{
		service: s,
		config:  cfg,
	}
}

func (h *Handler) MakeHandler(fn HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := fn(w, r); err != nil {
			// centralized error handling
			h.HandleError(w, r, err)
		}
	}
}

// GetIdempotencyKey safely retrieves the key from context
func GetIdempotencyKey(ctx context.Context) string {
	val, ok := ctx.Value(ctxkey.IdempotencyKey).(string)
	if !ok {
		return ""
	}
	return val
}
