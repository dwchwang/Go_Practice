package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

type Producer struct {
	writer *kafkago.Writer
}

func NewProducer(brokers []string) *Producer {
	writer := &kafkago.Writer{
		Addr: kafkago.TCP(brokers...),

		// Dùng Hash để message có cùng key đi vào cùng partition.
		// Ví dụ key = order_id.
		Balancer: &kafkago.Hash{},

		// RequireAll tương đương tư duy acks=all:
		// broker chỉ ack khi message được ghi an toàn hơn.
		RequiredAcks: kafkago.RequireAll,

		// false nghĩa là WriteMessages sẽ chờ broker trả kết quả.
		Async: false,

		// Retry khi gửi lỗi tạm thời.
		MaxAttempts: 10,

		// Batching nhẹ để demo producer tuning.
		BatchSize:    100,
		BatchTimeout: 10 * time.Millisecond,
	}

	return &Producer{
		writer: writer,
	}
}

func (p *Producer) PublishEvent(ctx context.Context, topic string, key string, payload any) error {
	data, err := encodePayload(payload)
	if err != nil {
		return err
	}

	msg := kafkago.Message{
		Topic: topic,
		Key:   []byte(key),
		Value: data,
		Time:  time.Now(),
	}

	if err := p.writer.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("write kafka message: %w", err)
	}

	return nil
}

func (p *Producer) Close() error {
	return p.writer.Close()
}

func encodePayload(payload any) ([]byte, error) {
	switch v := payload.(type) {
	case []byte:
		return v, nil

	case json.RawMessage:
		return v, nil

	default:
		data, err := json.Marshal(payload)
		if err != nil {
			return nil, fmt.Errorf("marshal kafka payload: %w", err)
		}

		return data, nil
	}
}
