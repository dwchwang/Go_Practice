package notification

import (
	"context"
	"fmt"
	"log"
	"order-processing/internal/domain"
)

// ConsoleNotificationFactory tạo family Console: ConsoleFormatter + ConsoleSender.
type ConsoleNotificationFactory struct{}

func (f *ConsoleNotificationFactory) CreateFormatter() MessageFormatter {
	return &ConsoleFormatter{}
}

func (f *ConsoleNotificationFactory) CreateSender() MessageSender {
	return &ConsoleSender{}
}

// ConsoleFormatter format nội dung console.
type ConsoleFormatter struct{}

func (f *ConsoleFormatter) Format(event domain.PaymentProcessedEvent) string {
	status := "PAID ✅"
	if !event.Success {
		status = "CANCELLED ❌"
	}
	return fmt.Sprintf(
		"[CONSOLE] User %s: Order %s is %s",
		event.UserID,
		event.OrderID,
		status,
	)
}

// ConsoleSender gửi thông báo ra console (instant, không delay).
type ConsoleSender struct{}

func (s *ConsoleSender) Send(_ context.Context, userID string, content string) error {
	log.Printf("[ConsoleSender] %s", content)
	return nil
}
