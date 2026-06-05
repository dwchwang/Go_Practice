package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"order-processing/internal/domain"

	"github.com/google/uuid"
	"gorm.io/datatypes"
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

// CreateWithOutbox ghi order + outbox event trong cùng 1 transaction.
// Nếu insert order fail -> rollback.
// Nếu insert outbox fail -> rollback.
// Nếu cả hai thành công -> commit.
func (r *OrderRepository) CreateWithOutbox(ctx context.Context, order *domain.Order) error {
	outboxID := uuid.New()
	payload, err := json.Marshal(domain.OrderCreatedEvent{
		EventID:   outboxID.String(),
		OrderID:   order.ID.String(),
		UserID:    order.UserID,
		ProductID: order.ProductID,
		Amount:    order.Amount,
	})
	if err != nil {
		return fmt.Errorf("marshal order created event: %w", err)
	}

	outboxEvent := &domain.OutboxEvent{
		ID:          outboxID,
		AggregateID: order.ID.String(),
		EventType:   domain.EventTypeOrderCreated,
		Payload:     datatypes.JSON(payload),
		Status:      domain.OutboxStatusPending,
		RetryCount:  0,
	}

	err = r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(order).Error; err != nil {
			return fmt.Errorf("insert order: %w", err)
		}

		if err := tx.Create(outboxEvent).Error; err != nil {
			return fmt.Errorf("insert outbox event: %w", err)
		}

		return nil
	})

	if err != nil {
		return fmt.Errorf("create order with outbox: %w", err)
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

func (r *OrderRepository) UpdateStatus(ctx context.Context, id uuid.UUID, status domain.OrderStatus) error {
	result := r.db.WithContext(ctx).
		Model(&domain.Order{}).
		Where("id = ?", id).
		Update("status", status)

	if result.Error != nil {
		return fmt.Errorf("update order status: %w", result.Error)
	}

	if result.RowsAffected == 0 {
		return gorm.ErrRecordNotFound
	}

	return nil
}
