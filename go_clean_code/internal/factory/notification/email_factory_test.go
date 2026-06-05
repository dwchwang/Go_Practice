package notification

import (
	"context"
	"order-processing/internal/domain"
	"strings"
	"testing"
)

func TestEmailFormatter_Format_Success(t *testing.T) {
	formatter := &EmailFormatter{}
	event := domain.PaymentProcessedEvent{
		EventID: "evt-001",
		OrderID: "order-123",
		UserID:  "user-456",
		Success: true,
	}

	result := formatter.Format(event)

	if !strings.Contains(result, "order-123") {
		t.Errorf("expected result to contain order ID, got: %s", result)
	}
	if !strings.Contains(result, "success") {
		t.Errorf("expected result to contain 'success', got: %s", result)
	}
	if !strings.Contains(result, "[EMAIL]") {
		t.Errorf("expected result to contain '[EMAIL]', got: %s", result)
	}
}

func TestEmailFormatter_Format_Failure(t *testing.T) {
	formatter := &EmailFormatter{}
	event := domain.PaymentProcessedEvent{
		EventID: "evt-002",
		OrderID: "order-456",
		UserID:  "user-789",
		Success: false,
	}

	result := formatter.Format(event)

	if !strings.Contains(result, "failed") {
		t.Errorf("expected result to contain 'failed' for unsuccessful payment, got: %s", result)
	}
}

func TestEmailSender_Send(t *testing.T) {
	sender := &EmailSender{}
	err := sender.Send(context.Background(), "user-001", "test message")
	if err != nil {
		t.Errorf("EmailSender.Send() returned error: %v", err)
	}
}

func TestEmailSender_Send_ContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // pre-cancel

	sender := &EmailSender{}
	err := sender.Send(ctx, "user-001", "test message")
	if err == nil {
		t.Error("expected error for cancelled context")
	}
}

func TestConsoleFormatter_Format(t *testing.T) {
	formatter := &ConsoleFormatter{}
	event := domain.PaymentProcessedEvent{
		OrderID: "order-789",
		UserID:  "user-999",
		Success: true,
	}

	result := formatter.Format(event)

	if !strings.Contains(result, "[CONSOLE]") {
		t.Errorf("expected result to contain '[CONSOLE]', got: %s", result)
	}
	if !strings.Contains(result, "PAID") {
		t.Errorf("expected result to contain 'PAID', got: %s", result)
	}
}

func TestConsoleSender_Send(t *testing.T) {
	sender := &ConsoleSender{}
	err := sender.Send(context.Background(), "user-001", "test")
	if err != nil {
		t.Errorf("ConsoleSender.Send() returned error: %v", err)
	}
}

func TestEmailNotificationFactory_CreatesCorrectProducts(t *testing.T) {
	factory := &EmailNotificationFactory{}

	formatter := factory.CreateFormatter()
	if _, ok := formatter.(*EmailFormatter); !ok {
		t.Error("CreateFormatter should return *EmailFormatter")
	}

	sender := factory.CreateSender()
	if _, ok := sender.(*EmailSender); !ok {
		t.Error("CreateSender should return *EmailSender")
	}
}

func TestConsoleNotificationFactory_CreatesCorrectProducts(t *testing.T) {
	factory := &ConsoleNotificationFactory{}

	formatter := factory.CreateFormatter()
	if _, ok := formatter.(*ConsoleFormatter); !ok {
		t.Error("CreateFormatter should return *ConsoleFormatter")
	}

	sender := factory.CreateSender()
	if _, ok := sender.(*ConsoleSender); !ok {
		t.Error("CreateSender should return *ConsoleSender")
	}
}
