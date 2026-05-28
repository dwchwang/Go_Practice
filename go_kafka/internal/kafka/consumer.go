package kafka

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

const defaultMaxRetry = 3

type HandlerFunc func(ctx context.Context, msg kafkago.Message) error

type Consumer struct {
	reader   *kafkago.Reader
	producer *Producer
	dlqTopic string
	maxRetry int
}

func NewConsumer(brokers []string, topic string, groupID string, dlqTopic string) *Consumer {
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
		reader:   reader,
		producer: NewProducer(brokers),
		dlqTopic: dlqTopic,
		maxRetry: defaultMaxRetry,
	}
}

func (c *Consumer) Consume(ctx context.Context, handler HandlerFunc) error {
	cfg := c.reader.Config()

	log.Printf(
		"[KafkaConsumer] started topic=%s group_id=%s dlq_topic=%s",
		cfg.Topic,
		cfg.GroupID,
		c.dlqTopic,
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

		processErr := c.processWithRetry(ctx, msg, handler)
		if processErr != nil {
			// Nếu lỗi ở đây thường là gửi DLQ thất bại hoặc context cancelled.
			// Không commit offset để message gốc được xử lý lại sau.
			log.Printf(
				"[KafkaConsumer] process failed, offset not committed topic=%s partition=%d offset=%d error=%v",
				msg.Topic,
				msg.Partition,
				msg.Offset,
				processErr,
			)
			continue
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

func (c *Consumer) processWithRetry(
	ctx context.Context,
	msg kafkago.Message,
	handler HandlerFunc,
) error {
	var lastErr error

	for attempt := 1; attempt <= c.maxRetry; attempt++ {
		err := handler(ctx, msg)
		if err == nil {
			return nil
		}

		lastErr = err

		log.Printf(
			"[KafkaConsumer] attempt %d/%d failed topic=%s partition=%d offset=%d error=%v",
			attempt,
			c.maxRetry,
			msg.Topic,
			msg.Partition,
			msg.Offset,
			err,
		)

		if attempt < c.maxRetry {
			backoff := time.Duration(attempt) * time.Second

			if err := sleepWithContext(ctx, backoff); err != nil {
				return err
			}
		}
	}

	log.Printf(
		"[KafkaConsumer] max retry exceeded, sending to DLQ topic=%s partition=%d offset=%d",
		msg.Topic,
		msg.Partition,
		msg.Offset,
	)

	if err := c.sendToDLQ(ctx, msg, lastErr); err != nil {
		return err
	}

	// Gửi DLQ thành công thì coi như xử lý xong message gốc.
	// Sau đó Consume() sẽ commit offset message gốc.
	return nil
}

type DLQMessage struct {
	OriginalTopic   string    `json:"original_topic"`
	Partition       int       `json:"partition"`
	Offset          int64     `json:"offset"`
	Key             string    `json:"key"`
	OriginalPayload string    `json:"original_payload"`
	ErrorMessage    string    `json:"error_message"`
	RetryCount      int       `json:"retry_count"`
	FailedAt        time.Time `json:"failed_at"`
}

func (c *Consumer) sendToDLQ(
	ctx context.Context,
	msg kafkago.Message,
	err error,
) error {
	if c.dlqTopic == "" {
		return fmt.Errorf("dlq topic is empty")
	}

	dlqMessage := DLQMessage{
		OriginalTopic:   msg.Topic,
		Partition:       msg.Partition,
		Offset:          msg.Offset,
		Key:             string(msg.Key),
		OriginalPayload: string(msg.Value),
		ErrorMessage:    err.Error(),
		RetryCount:      c.maxRetry,
		FailedAt:        time.Now(),
	}

	if err := c.producer.PublishEvent(ctx, c.dlqTopic, string(msg.Key), dlqMessage); err != nil {
		return fmt.Errorf("publish DLQ message: %w", err)
	}

	log.Printf(
		"[KafkaConsumer] sent to DLQ original_topic=%s dlq_topic=%s partition=%d offset=%d",
		msg.Topic,
		c.dlqTopic,
		msg.Partition,
		msg.Offset,
	)

	return nil
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
