package ports

import (
	"context"

	"order-processing/internal/domain"

	"github.com/google/uuid"
)

// OrderStore định nghĩa các thao tác với order repository.
// Implemented by: *repository.OrderRepository, *repository.CachedOrderRepository (Proxy).
type OrderStore interface {
	CreateWithOutbox(ctx context.Context, order *domain.Order) error
	GetByID(ctx context.Context, id uuid.UUID) (*domain.Order, error)
	UpdateStatus(ctx context.Context, id uuid.UUID, status domain.OrderStatus) error
}
