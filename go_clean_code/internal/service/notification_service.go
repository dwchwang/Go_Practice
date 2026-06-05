package service

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"order-processing/internal/domain"
	"order-processing/internal/repository"

	kafkago "github.com/segmentio/kafka-go"
)

type NotificationService struct {
	processedRepo *repository.ProcessedMessageRepository
	consumerGroup string
}

func NewNotificationService(
	processedRepo *repository.ProcessedMessageRepository,
	consumerGroup string,
) *NotificationService {
	return &NotificationService{
		processedRepo: processedRepo,
		consumerGroup: consumerGroup,
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

	processed, err := s.processedRepo.IsProcessed(ctx, messageID, s.consumerGroup)
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

	if err := s.simulateSendNotification(ctx, event); err != nil {
		return err
	}

	if err := s.processedRepo.MarkProcessed(ctx, domain.ProcessedMessage{
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

func (s *NotificationService) simulateSendNotification(
	ctx context.Context,
	event domain.PaymentProcessedEvent,
) error {
	select {
	case <-ctx.Done():
		return ctx.Err()

	case <-time.After(300 * time.Millisecond):
	}

	if event.Success {
		log.Printf(
			"[NotificationService] EMAIL SENT: user=%s order=%s payment success, order confirmed",
			event.UserID,
			event.OrderID,
		)

		return nil
	}

	log.Printf(
		"[NotificationService] EMAIL SENT: user=%s order=%s payment failed, please try again",
		event.UserID,
		event.OrderID,
	)

	return nil
}