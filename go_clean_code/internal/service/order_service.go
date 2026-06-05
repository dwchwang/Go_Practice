package service

import (
	"context"
	"errors"
	"fmt"

	"order-processing/internal/domain"
	"order-processing/internal/domain/ports"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

var (
	ErrInvalidOrderID = errors.New("invalid order id")
	ErrOrderNotFound  = errors.New("order not found")
)

type CreateOrderInput struct {
	UserID    string
	ProductID string
	Amount    float64
}

// OrderService xử lý business logic cho order. Cache được xử lý bởi Proxy (CachedOrderRepository),
// service chỉ gọi OrderStore interface và không biết cache tồn tại.
type OrderService struct {
	orderStore ports.OrderStore
}

func NewOrderService(orderStore ports.OrderStore) *OrderService {
	return &OrderService{
		orderStore: orderStore,
	}
}

func (s *OrderService) CreateOrder(ctx context.Context, input CreateOrderInput) (*domain.Order, error) {
	order := &domain.Order{
		ID:        uuid.New(),
		UserID:    input.UserID,
		ProductID: input.ProductID,
		Amount:    input.Amount,
		Status:    domain.StatusPending,
	}

	// Proxy (CachedOrderRepository) xử lý insert + outbox + cache trong 1 lần gọi.
	if err := s.orderStore.CreateWithOutbox(ctx, order); err != nil {
		return nil, fmt.Errorf("create order with outbox: %w", err)
	}

	return order, nil
}

func (s *OrderService) GetOrderByID(ctx context.Context, idStr string) (*domain.Order, error) {
	id, err := uuid.Parse(idStr)
	if err != nil {
		return nil, ErrInvalidOrderID
	}

	// Proxy (CachedOrderRepository) xử lý cache-aside trong suốt.
	order, err := s.orderStore.GetByID(ctx, id)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrOrderNotFound
		}

		return nil, fmt.Errorf("get order from db: %w", err)
	}

	return order, nil
}
