package outbox

import (
	"context"
	"fmt"
	"log"
	"time"

	"order-processing/internal/domain"
	appkafka "order-processing/internal/kafka"
	"order-processing/internal/repository"
)

type Relay struct {
	outboxRepo *repository.OutboxRepository
	producer   *appkafka.Producer
	interval   time.Duration
	batchSize  int
}

func NewRelay(
	outboxRepo *repository.OutboxRepository,
	producer *appkafka.Producer,
	interval time.Duration,
	batchSize int,
) *Relay {
	return &Relay{
		outboxRepo: outboxRepo,
		producer:   producer,
		interval:   interval,
		batchSize:  batchSize,
	}
}

func (r *Relay) Start(ctx context.Context) {
	log.Printf(
		"[OutboxRelay] started interval=%s batch_size=%d",
		r.interval.String(),
		r.batchSize,
	)

	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("[OutboxRelay] shutting down...")
			return

		case <-ticker.C:
			if err := r.ProcessOnce(ctx); err != nil {
				log.Printf("[OutboxRelay] process error: %v", err)
			}
		}
	}
}

func (r *Relay) ProcessOnce(ctx context.Context) error {
	return r.outboxRepo.ProcessPending(ctx, r.batchSize, func(ctx context.Context, event domain.OutboxEvent) error {
		topic, err := eventTypeToTopic(event.EventType)
		if err != nil {
			return err
		}

		// event.Payload là JSONB lấy từ PostgreSQL.
		// Ta convert sang []byte để tránh marshal JSON lần nữa.
		payload := []byte(event.Payload)

		if err := r.producer.PublishEvent(ctx, topic, event.AggregateID, payload); err != nil {
			return fmt.Errorf("publish kafka event: %w", err)
		}

		log.Printf(
			"[OutboxRelay] published event_id=%s event_type=%s topic=%s key=%s",
			event.ID.String(),
			event.EventType,
			topic,
			event.AggregateID,
		)

		return nil
	})
}

func eventTypeToTopic(eventType string) (string, error) {
	switch eventType {
	case domain.EventTypeOrderCreated:
		return appkafka.TopicOrderCreated, nil

	default:
		return "", fmt.Errorf("unknown event type: %s", eventType)
	}
}