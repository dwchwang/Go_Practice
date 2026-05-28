package service

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"order-processing/internal/domain"

	kafkago "github.com/segmentio/kafka-go"
)

type NotificationService struct{}

func NewNotificationService() *NotificationService {
	return &NotificationService{}
}

func (s *NotificationService) HandlePaymentProcessed(ctx context.Context, msg kafkago.Message) error {
	var event domain.PaymentProcessedEvent

	if err := json.Unmarshal(msg.Value, &event); err != nil {
		return fmt.Errorf("unmarshal PaymentProcessedEvent: %w", err)
	}

	log.Printf(
		"[NotificationService] received payment event order_id=%s user_id=%s success=%v",
		event.OrderID,
		event.UserID,
		event.Success,
	)

	// Giả lập thời gian gửi email / push notification.
	if err := s.simulateSendNotification(ctx, event); err != nil {
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