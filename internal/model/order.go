package model

import "time"

type Order struct {
	ID          string      `json:"id" gorm:"type:uuid;default:gen_random_uuid();primaryKey"`
	UserID      string      `json:"user_id" gorm:"type:uuid;not null;index"`
	User        User        `json:"-" gorm:"foreignKey:UserID"`
	Status      string      `json:"status" gorm:"not null;default:created"`
	TotalAmount float64     `json:"total_amount" gorm:"not null;default:0"`
	Items       []OrderItem `json:"items" gorm:"foreignKey:OrderID"`
	CreatedAt   time.Time   `json:"created_at"`
	UpdatedAt   time.Time   `json:"updated_at"`
}

type OrderItem struct {
	ID        string    `json:"id" gorm:"type:uuid;default:gen_random_uuid();primaryKey"`
	OrderID   string    `json:"order_id" gorm:"type:uuid;not null;index"`
	ProductID string    `json:"product_id" gorm:"not null;index"`
	Product   Product   `json:"-" gorm:"foreignKey:ProductID"`
	Quantity  int64     `json:"quantity" gorm:"not null"`
	UnitPrice float64   `json:"unit_price" gorm:"not null"`
	CreatedAt time.Time `json:"created_at"`
}
