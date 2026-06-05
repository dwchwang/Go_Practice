package notification

import "order-processing/internal/domain"

// MessageFormatter tạo nội dung thông báo từ PaymentProcessedEvent.
// Mỗi kênh (Email, Console, SMS...) có cách format riêng.
type MessageFormatter interface {
	Format(event domain.PaymentProcessedEvent) string
}
