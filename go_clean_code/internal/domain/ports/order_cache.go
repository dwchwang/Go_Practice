package ports

import (
	"context"

	"order-processing/internal/domain"
)

// OrderCache định nghĩa thao tác cache order.
// Implemented by: *cache.RedisCache.
type OrderCache interface {
	SetOrder(ctx context.Context, order *domain.Order) error
	GetOrder(ctx context.Context, id string) (*domain.Order, error)
}
