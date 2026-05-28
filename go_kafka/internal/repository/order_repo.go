package repository

import (
	"context"
	"fmt"
	"order-processing/internal/domain"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

type OrderRepository struct {
	db *gorm.DB
}

func NewOrderRepository(db *gorm.DB) *OrderRepository {
	return &OrderRepository{
		db: db,
	}
}

func (r *OrderRepository) Create(ctx context.Context, order *domain.Order) error {
	if err := r.db.WithContext(ctx).Create(order).Error; err != nil {
		return fmt.Errorf("create order: %w", err)
	}

	return nil
}

func (r *OrderRepository) GetByID(ctx context.Context, id uuid.UUID) (*domain.Order, error) {
	var order domain.Order

	if err := r.db.WithContext(ctx).
		First(&order, "id = ?", id).
		Error; err != nil {
		return nil, err
	}

	return &order, nil
}
