package kafka

import (
	"context"
	"errors"
	"io"
	"log"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

type HandlerFunc func(ctx context.Context, msg kafkago.Message) error

type Consumer struct {
	reader *kafkago.Reader
}

func NewConsumer(brokers []string, topic string, groupID string) *Consumer {
	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers: brokers,
		Topic:   topic,
		GroupID: groupID,

		// Khi dùng consumer group:
		// - không set Partition
		// - Kafka sẽ tự assign partition cho consumer trong group
		StartOffset: kafkago.FirstOffset,

		MinBytes: 1,
		MaxBytes: 10e6,
		MaxWait:  500 * time.Millisecond,

		// CommitInterval = 0 nghĩa là commit sync.
		// Ta sẽ gọi CommitMessages sau khi xử lý xong.
		CommitInterval: 0,
	})

	return &Consumer{
		reader: reader,
	}
}

func (c *Consumer) Consume(ctx context.Context, handler HandlerFunc) error {
	cfg := c.reader.Config()

	log.Printf(
		"[KafkaConsumer] started topic=%s group_id=%s",
		cfg.Topic,
		cfg.GroupID,
	)

	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) ||
				errors.Is(err, context.DeadlineExceeded) ||
				errors.Is(err, io.EOF) {
				log.Println("[KafkaConsumer] stopping...")
				return nil
			}

			log.Printf("[KafkaConsumer] fetch error: %v", err)
			continue
		}

		log.Printf(
			"[KafkaConsumer] received topic=%s partition=%d offset=%d key=%s",
			msg.Topic,
			msg.Partition,
			msg.Offset,
			string(msg.Key),
		)

		// nếu xử lý lỗi, retry tại chỗ và KHÔNG commit.
		// sau sẽ thêm retry limit + DLQ.
		for {
			if err := handler(ctx, msg); err != nil {
				log.Printf(
					"[KafkaConsumer] handle failed topic=%s partition=%d offset=%d error=%v",
					msg.Topic,
					msg.Partition,
					msg.Offset,
					err,
				)

				if err := sleepWithContext(ctx, 2*time.Second); err != nil {
					return nil
				}

				continue
			}

			break
		}

		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			log.Printf(
				"[KafkaConsumer] commit failed topic=%s partition=%d offset=%d error=%v",
				msg.Topic,
				msg.Partition,
				msg.Offset,
				err,
			)
			continue
		}

		log.Printf(
			"[KafkaConsumer] committed topic=%s partition=%d offset=%d",
			msg.Topic,
			msg.Partition,
			msg.Offset,
		)
	}
}

func (c *Consumer) Close() error {
	return c.reader.Close()
}

func sleepWithContext(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}