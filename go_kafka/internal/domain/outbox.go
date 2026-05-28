package domain

import (
	"time"

	"github.com/google/uuid"
	"gorm.io/datatypes"
)

const (
	OutboxStatusPending = "pending"
	OutboxStatusSent    = "sent"
	OutboxStatusFailed  = "failed"
)

type OutboxEvent struct {
	ID          uuid.UUID      `gorm:"type:uuid;primaryKey" json:"id"`
	AggregateID string         `gorm:"column:aggregate_id;type:varchar(100);not null" json:"aggregate_id"`
	EventType   string         `gorm:"column:event_type;type:varchar(100);not null" json:"event_type"`
	Payload     datatypes.JSON `gorm:"column:payload;type:jsonb;not null" json:"payload"`
	Status      string         `gorm:"column:status;type:varchar(20);not null" json:"status"`
	RetryCount  int            `gorm:"column:retry_count;not null" json:"retry_count"`
	CreatedAt   time.Time      `gorm:"column:created_at" json:"created_at"`
	SentAt      *time.Time     `gorm:"column:sent_at" json:"sent_at"`
}

func (OutboxEvent) TableName() string {
	return "outbox"
}
