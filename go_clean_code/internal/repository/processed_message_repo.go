package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"order-processing/internal/domain"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type ProcessedMessageRepository struct {
	db *gorm.DB
}

func NewProcessedMessageRepository(db *gorm.DB) *ProcessedMessageRepository {
	return &ProcessedMessageRepository{
		db: db,
	}
}

func (r *ProcessedMessageRepository) IsProcessed(
	ctx context.Context,
	messageID string,
	consumerGroup string,
) (bool, error) {
	var msg domain.ProcessedMessage

	err := r.db.WithContext(ctx).
		First(&msg, "message_id = ? AND consumer_group = ?", messageID, consumerGroup).
		Error

	if err == nil {
		return true, nil
	}

	if errors.Is(err, gorm.ErrRecordNotFound) {
		return false, nil
	}

	return false, fmt.Errorf("check processed message: %w", err)
}

func (r *ProcessedMessageRepository) MarkProcessed(
	ctx context.Context,
	message domain.ProcessedMessage,
) error {
	message.ProcessedAt = time.Now()

	err := r.db.WithContext(ctx).
		Clauses(clause.OnConflict{
			DoNothing: true,
		}).
		Create(&message).
		Error

	if err != nil {
		return fmt.Errorf("mark message processed: %w", err)
	}

	return nil
}