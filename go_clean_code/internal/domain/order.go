package domain

import (
	"time"

	"github.com/google/uuid"
)

type OrderStatus string

const (
	StatusPending   OrderStatus = "pending"
	StatusPaid      OrderStatus = "paid"
	StatusCancelled OrderStatus = "cancelled"
)

type Order struct {
	ID        uuid.UUID   `gorm:"type:uuid;primaryKey" json:"id"`
	UserID    string      `gorm:"column:user_id;type:varchar(100);not null" json:"user_id"`
	ProductID string      `gorm:"column:product_id;type:varchar(100);not null" json:"product_id"`
	Amount    float64     `gorm:"column:amount;type:numeric(10,2);not null" json:"amount"`
	Status    OrderStatus `gorm:"column:status;type:varchar(50);not null" json:"status"`
	CreatedAt time.Time   `gorm:"column:created_at" json:"created_at"`
	UpdatedAt time.Time   `gorm:"column:updated_at" json:"updated_at"`
}

func (Order) TableName() string {
	return "orders"
}
