package domain

import "time"

type ProcessedMessage struct {
	MessageID     string    `gorm:"column:message_id;primaryKey"`
	ConsumerGroup string    `gorm:"column:consumer_group;primaryKey"`
	Topic         string    `gorm:"column:topic"`
	Partition     int       `gorm:"column:partition"`
	OffsetValue   int64     `gorm:"column:offset_value"`
	ProcessedAt   time.Time `gorm:"column:processed_at"`
}

func (ProcessedMessage) TableName() string {
	return "processed_messages"
}