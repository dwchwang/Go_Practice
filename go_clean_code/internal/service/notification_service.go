package service

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"order-processing/internal/domain"
	"order-processing/internal/domain/ports"
	"order-processing/internal/factory/notification"

	kafkago "github.com/segmentio/kafka-go"
)

type NotificationService struct {
	processedStore ports.ProcessedMessageStore
	factory        notification.NotificationFactory
	consumerGroup  string
}

func NewNotificationService(
	processedStore ports.ProcessedMessageStore,
	factory notification.NotificationFactory,
	consumerGroup string,
) *NotificationService {
	return &NotificationService{
		processedStore: processedStore,
		factory:        factory,
		consumerGroup:  consumerGroup,
	}
}

func (s *NotificationService) HandlePaymentProcessed(ctx context.Context, msg kafkago.Message) error {
	var event domain.PaymentProcessedEvent

	if err := json.Unmarshal(msg.Value, &event); err != nil {
		return fmt.Errorf("unmarshal PaymentProcessedEvent: %w", err)
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
			"[NotificationService] duplicate message ignored message_id=%s order_id=%s",
			messageID,
			event.OrderID,
		)
		return nil
	}

	log.Printf(
		"[NotificationService] received payment event message_id=%s order_id=%s user_id=%s success=%v",
		messageID,
		event.OrderID,
		event.UserID,
		event.Success,
	)

	// Abstract Factory: tạo Formatter + Sender từ factory (không biết Email hay Console)
	formatter := s.factory.CreateFormatter()
	sender := s.factory.CreateSender()

	content := formatter.Format(event)
	if err := sender.Send(ctx, event.UserID, content); err != nil {
		log.Printf("[NotificationService] sender error (non-fatal): %v", err)
		// Không return error — sender thất bại không nên chặn pipeline.
		// Vẫn MarkProcessed để tránh Kafka retry vô hạn.
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

	return nil
}
