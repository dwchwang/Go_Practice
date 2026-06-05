package service

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"time"

	"order-processing/internal/domain"
	"order-processing/internal/domain/ports"
	appkafka "order-processing/internal/kafka"

	"github.com/google/uuid"
	kafkago "github.com/segmentio/kafka-go"
)

type PaymentService struct {
	orderStore     ports.OrderStore
	processedStore ports.ProcessedMessageStore
	eventPublisher ports.EventPublisher
	consumerGroup  string
	rng            *rand.Rand
}

func NewPaymentService(
	orderStore ports.OrderStore,
	processedStore ports.ProcessedMessageStore,
	eventPublisher ports.EventPublisher,
	consumerGroup string,
) *PaymentService {
	return &PaymentService{
		orderStore:     orderStore,
		processedStore: processedStore,
		eventPublisher: eventPublisher,
		consumerGroup:  consumerGroup,
		rng:            rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (s *PaymentService) HandleOrderCreated(ctx context.Context, msg kafkago.Message) error {
	var event domain.OrderCreatedEvent

	if err := json.Unmarshal(msg.Value, &event); err != nil {
		return fmt.Errorf("unmarshal OrderCreatedEvent: %w", err)
	}

	messageID := event.EventID
	if messageID == "" {
		messageID = fallbackMessageID(msg)
	}

	processed, err := s.processedStore.IsProcessed(ctx, messageID, s.consumerGroup)
	if err != nil {
		return err
	}

	if processed {
		log.Printf(
			"[PaymentService] duplicate message ignored message_id=%s order_id=%s",
			messageID,
			event.OrderID,
		)
		return nil
	}

	log.Printf(
		"[PaymentService] processing payment message_id=%s order_id=%s user_id=%s amount=%.2f",
		messageID,
		event.OrderID,
		event.UserID,
		event.Amount,
	)

	orderID, err := uuid.Parse(event.OrderID)
	if err != nil {
		return fmt.Errorf("parse order id: %w", err)
	}

	success := s.rng.Float32() > 0.1

	newStatus := domain.StatusPaid
	if !success {
		newStatus = domain.StatusCancelled
	}

	if err := s.orderStore.UpdateStatus(ctx, orderID, newStatus); err != nil {
		return fmt.Errorf("update order status: %w", err)
	}

	// Proxy (CachedOrderRepository) đã tự động warm cache sau UpdateStatus.

	paymentEvent := domain.PaymentProcessedEvent{
		EventID: messageID + ":payment-processed",
		OrderID: event.OrderID,
		UserID:  event.UserID,
		Success: success,
	}

	if err := s.eventPublisher.PublishEvent(
		ctx,
		appkafka.TopicOrderPaymentProcessed,
		event.OrderID,
		paymentEvent,
	); err != nil {
		return fmt.Errorf("publish PaymentProcessedEvent: %w", err)
	}

	if err := s.processedStore.MarkProcessed(ctx, domain.ProcessedMessage{
		MessageID:     messageID,
		ConsumerGroup: s.consumerGroup,
		Topic:         msg.Topic,
		Partition:     msg.Partition,
		OffsetValue:   msg.Offset,
	}); err != nil {
		return err
	}

	if success {
		log.Printf("[PaymentService] payment SUCCESS order_id=%s", event.OrderID)
	} else {
		log.Printf("[PaymentService] payment FAILED order_id=%s", event.OrderID)
	}

	return nil
}

// fallbackMessageID đã được chuyển sang util.go