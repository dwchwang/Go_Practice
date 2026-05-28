package repository

import (
	"context"
	"fmt"
	"log"
	"time"

	"order-processing/internal/domain"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type OutboxRepository struct {
	db *gorm.DB
}

func NewOutboxRepository(db *gorm.DB) *OutboxRepository {
	return &OutboxRepository{
		db: db,
	}
}

// ProcessPending lấy các event pending và xử lý trong transaction.
//
// FOR UPDATE SKIP LOCKED giúp tránh việc nhiều relay instance
// xử lý cùng một outbox event.
func (r *OutboxRepository) ProcessPending(
	ctx context.Context,
	limit int,
	handler func(ctx context.Context, event domain.OutboxEvent) error,
) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var events []domain.OutboxEvent

		err := tx.
			Clauses(clause.Locking{
				Strength: "UPDATE",
				Options:  "SKIP LOCKED",
			}).
			Where("status = ?", domain.OutboxStatusPending).
			Order("created_at ASC").
			Limit(limit).
			Find(&events).
			Error

		if err != nil {
			return fmt.Errorf("find pending outbox events: %w", err)
		}

		if len(events) == 0 {
			return nil
		}

		for _, event := range events {
			if err := handler(ctx, event); err != nil {
				log.Printf(
					"[OutboxRepository] handle event failed id=%s event_type=%s retry_count=%d error=%v",
					event.ID.String(),
					event.EventType,
					event.RetryCount,
					err,
				)

				if updateErr := tx.Model(&domain.OutboxEvent{}).
					Where("id = ?", event.ID).
					UpdateColumn("retry_count", gorm.Expr("retry_count + ?", 1)).
					Error; updateErr != nil {
					return fmt.Errorf("increment retry count: %w", updateErr)
				}

				// Giữ status = pending để lần sau relay thử lại.
				continue
			}

			now := time.Now()

			if err := tx.Model(&domain.OutboxEvent{}).
				Where("id = ?", event.ID).
				Updates(map[string]any{
					"status":  domain.OutboxStatusSent,
					"sent_at": now,
				}).
				Error; err != nil {
				return fmt.Errorf("mark outbox event sent: %w", err)
			}

			log.Printf(
				"[OutboxRepository] event marked sent id=%s event_type=%s",
				event.ID.String(),
				event.EventType,
			)
		}

		return nil
	})
}
