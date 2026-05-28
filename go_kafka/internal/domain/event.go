package domain

const (
	EventTypeOrderCreated     = "OrderCreated"
	EventTypePaymentProcessed = "PaymentProcessed"
)

type OrderCreatedEvent struct {
	OrderID   string  `json:"order_id"`
	UserID    string  `json:"user_id"`
	ProductID string  `json:"product_id"`
	Amount    float64 `json:"amount"`
}

type PaymentProcessedEvent struct {
	OrderID string `json:"order_id"`
	UserID  string `json:"user_id"`
	Success bool   `json:"success"`
}
