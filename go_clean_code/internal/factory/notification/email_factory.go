package notification

import (
	"context"
	"fmt"
	"log"
	"order-processing/internal/domain"
	"time"
)

// EmailNotificationFactory tạo family Email: EmailFormatter + EmailSender.
type EmailNotificationFactory struct{}

func (f *EmailNotificationFactory) CreateFormatter() MessageFormatter {
	return &EmailFormatter{}
}

func (f *EmailNotificationFactory) CreateSender() MessageSender {
	return &EmailSender{}
}

// EmailFormatter format nội dung email.
type EmailFormatter struct{}

func (f *EmailFormatter) Format(event domain.PaymentProcessedEvent) string {
	status := "success"
	if !event.Success {
		status = "failed"
	}
	return fmt.Sprintf(
		"[EMAIL] Subject: Order %s - Payment %s | Dear user %s, your order has been processed.",
		event.OrderID,
		status,
		event.UserID,
	)
}

// EmailSender giả lập gửi email.
type EmailSender struct{}

func (s *EmailSender) Send(ctx context.Context, userID string, content string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(200 * time.Millisecond):
	}

	log.Printf("[EmailSender] Sending email to user %s: %s", userID, content)
	return nil
}
