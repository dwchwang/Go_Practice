package ports

import "context"

// EventPublisher định nghĩa thao tác publish event lên message broker.
// Implemented by: *kafka.Producer.
type EventPublisher interface {
	PublishEvent(ctx context.Context, topic string, key string, payload any) error
}
