package ports

import (
	"context"

	"order-processing/internal/domain"
)

// ProcessedMessageStore định nghĩa thao tác idempotency check.
// Implemented by: *repository.ProcessedMessageRepository.
type ProcessedMessageStore interface {
	IsProcessed(ctx context.Context, messageID string, consumerGroup string) (bool, error)
	MarkProcessed(ctx context.Context, message domain.ProcessedMessage) error
}
