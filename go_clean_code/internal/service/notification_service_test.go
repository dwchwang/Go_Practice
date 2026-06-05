package service

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"order-processing/internal/domain"
	"order-processing/internal/factory/notification"

	kafkago "github.com/segmentio/kafka-go"
)

// --- Mock notification factory + products ---

type mockNotificationFactory struct {
	createFormatterFunc func() notification.MessageFormatter
	createSenderFunc    func() notification.MessageSender
}

func (m *mockNotificationFactory) CreateFormatter() notification.MessageFormatter {
	return m.createFormatterFunc()
}

func (m *mockNotificationFactory) CreateSender() notification.MessageSender {
	return m.createSenderFunc()
}

type mockFormatter struct {
	formatFunc func(event domain.PaymentProcessedEvent) string
}

func (m *mockFormatter) Format(event domain.PaymentProcessedEvent) string {
	return m.formatFunc(event)
}

type mockSender struct {
	sendFunc func(ctx context.Context, userID string, content string) error
}

func (m *mockSender) Send(ctx context.Context, userID string, content string) error {
	return m.sendFunc(ctx, userID, content)
}

// --- Mock processed message store ---

type mockNotifProcessedStore struct {
	isProcessedFunc  func(ctx context.Context, messageID, consumerGroup string) (bool, error)
	markProcessedFunc func(ctx context.Context, msg domain.ProcessedMessage) error
}

func (m *mockNotifProcessedStore) IsProcessed(ctx context.Context, messageID, consumerGroup string) (bool, error) {
	return m.isProcessedFunc(ctx, messageID, consumerGroup)
}

func (m *mockNotifProcessedStore) MarkProcessed(ctx context.Context, msg domain.ProcessedMessage) error {
	return m.markProcessedFunc(ctx, msg)
}

// --- Tests ---

func TestNotificationService_HandlePaymentProcessed_Success(t *testing.T) {
	formatterCalled := false
	senderCalled := false
	markProcessedCalled := false

	factory := &mockNotificationFactory{
		createFormatterFunc: func() notification.MessageFormatter {
			return &mockFormatter{
				formatFunc: func(event domain.PaymentProcessedEvent) string {
					formatterCalled = true
					return "test content"
				},
			}
		},
		createSenderFunc: func() notification.MessageSender {
			return &mockSender{
				sendFunc: func(ctx context.Context, userID string, content string) error {
					senderCalled = true
					return nil
				},
			}
		},
	}

	processedStore := &mockNotifProcessedStore{
		isProcessedFunc: func(ctx context.Context, messageID, consumerGroup string) (bool, error) {
			return false, nil
		},
		markProcessedFunc: func(ctx context.Context, msg domain.ProcessedMessage) error {
			markProcessedCalled = true
			if msg.MessageID != "evt-001" {
				t.Errorf("expected messageID 'evt-001', got '%s'", msg.MessageID)
			}
			if msg.ConsumerGroup != "test-group" {
				t.Errorf("expected consumerGroup 'test-group', got '%s'", msg.ConsumerGroup)
			}
			return nil
		},
	}

	svc := NewNotificationService(processedStore, factory, "test-group")

	event := domain.PaymentProcessedEvent{
		EventID: "evt-001",
		OrderID: "order-123",
		UserID:  "user-456",
		Success: true,
	}
	payload, _ := json.Marshal(event)

	msg := kafkago.Message{
		Value: payload,
	}

	err := svc.HandlePaymentProcessed(context.Background(), msg)
	if err != nil {
		t.Fatalf("HandlePaymentProcessed returned error: %v", err)
	}

	if !formatterCalled {
		t.Error("expected CreateFormatter to be called")
	}
	if !senderCalled {
		t.Error("expected CreateSender to be called")
	}
	if !markProcessedCalled {
		t.Error("expected MarkProcessed to be called")
	}
}

func TestNotificationService_HandlePaymentProcessed_Idempotency(t *testing.T) {
	formatterCalled := false
	senderCalled := false
	markProcessedCalled := false

	factory := &mockNotificationFactory{
		createFormatterFunc: func() notification.MessageFormatter {
			formatterCalled = true
			return &mockFormatter{formatFunc: func(e domain.PaymentProcessedEvent) string { return "" }}
		},
		createSenderFunc: func() notification.MessageSender {
			return &mockSender{
				sendFunc: func(ctx context.Context, userID string, content string) error {
					senderCalled = true
					return nil
				},
			}
		},
	}

	processedStore := &mockNotifProcessedStore{
		isProcessedFunc: func(ctx context.Context, messageID, consumerGroup string) (bool, error) {
			return true, nil // already processed
		},
		markProcessedFunc: func(ctx context.Context, msg domain.ProcessedMessage) error {
			markProcessedCalled = true
			return nil
		},
	}

	svc := NewNotificationService(processedStore, factory, "test-group")

	event := domain.PaymentProcessedEvent{
		EventID: "evt-duplicate",
		OrderID: "order-001",
	}
	payload, _ := json.Marshal(event)

	msg := kafkago.Message{Value: payload}

	err := svc.HandlePaymentProcessed(context.Background(), msg)
	if err != nil {
		t.Fatalf("HandlePaymentProcessed returned error: %v", err)
	}

	if formatterCalled {
		t.Error("expected factory NOT to be called for duplicate message")
	}
	if senderCalled {
		t.Error("expected sender NOT to be called for duplicate message")
	}
	if markProcessedCalled {
		t.Error("expected MarkProcessed NOT to be called for duplicate message")
	}
}

func TestNotificationService_HandlePaymentProcessed_SenderError_StillMarksProcessed(t *testing.T) {
	markProcessedCalled := false

	factory := &mockNotificationFactory{
		createFormatterFunc: func() notification.MessageFormatter {
			return &mockFormatter{formatFunc: func(e domain.PaymentProcessedEvent) string { return "test" }}
		},
		createSenderFunc: func() notification.MessageSender {
			return &mockSender{
				sendFunc: func(ctx context.Context, userID string, content string) error {
					return errors.New("send failed")
				},
			}
		},
	}

	processedStore := &mockNotifProcessedStore{
		isProcessedFunc: func(ctx context.Context, messageID, consumerGroup string) (bool, error) {
			return false, nil
		},
		markProcessedFunc: func(ctx context.Context, msg domain.ProcessedMessage) error {
			markProcessedCalled = true
			return nil
		},
	}

	svc := NewNotificationService(processedStore, factory, "test-group")

	event := domain.PaymentProcessedEvent{
		EventID: "evt-sender-fail",
		OrderID: "order-999",
		UserID:  "user-001",
	}
	payload, _ := json.Marshal(event)

	msg := kafkago.Message{Value: payload}

	err := svc.HandlePaymentProcessed(context.Background(), msg)
	if err != nil {
		t.Fatalf("HandlePaymentProcessed should NOT return error on sender failure, got: %v", err)
	}

	if !markProcessedCalled {
		t.Error("expected MarkProcessed to be called even when sender fails (avoid infinite retry)")
	}
}

func TestNotificationService_HandlePaymentProcessed_EmptyEventID_UsesFallback(t *testing.T) {
	markProcessedCalled := false

	factory := &mockNotificationFactory{
		createFormatterFunc: func() notification.MessageFormatter {
			return &mockFormatter{formatFunc: func(e domain.PaymentProcessedEvent) string { return "ok" }}
		},
		createSenderFunc: func() notification.MessageSender {
			return &mockSender{sendFunc: func(ctx context.Context, userID string, content string) error { return nil }}
		},
	}

	processedStore := &mockNotifProcessedStore{
		isProcessedFunc: func(ctx context.Context, messageID, consumerGroup string) (bool, error) {
			return false, nil
		},
		markProcessedFunc: func(ctx context.Context, msg domain.ProcessedMessage) error {
			markProcessedCalled = true
			// Verify fallback message ID format
			if msg.MessageID == "" {
				t.Error("expected non-empty messageID from fallback")
			}
			return nil
		},
	}

	svc := NewNotificationService(processedStore, factory, "test-group")

	// Empty EventID triggers fallbackMessageID
	event := domain.PaymentProcessedEvent{
		EventID: "", // empty → fallback
		OrderID: "order-001",
	}
	payload, _ := json.Marshal(event)

	msg := kafkago.Message{
		Value:     payload,
		Topic:     "test.topic",
		Partition: 2,
		Offset:    42,
	}

	err := svc.HandlePaymentProcessed(context.Background(), msg)
	if err != nil {
		t.Fatalf("HandlePaymentProcessed returned error: %v", err)
	}

	if !markProcessedCalled {
		t.Error("expected MarkProcessed to be called")
	}
}
