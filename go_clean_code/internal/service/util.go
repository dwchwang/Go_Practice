package service

import (
	"fmt"

	kafkago "github.com/segmentio/kafka-go"
)

// fallbackMessageID tạo message ID từ Kafka metadata khi event không có EventID.
func fallbackMessageID(msg kafkago.Message) string {
	return fmt.Sprintf("%s:%d:%d", msg.Topic, msg.Partition, msg.Offset)
}
