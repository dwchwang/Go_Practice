package service

import (
	"context"
	"encoding/json"
	"testing"

	"order-processing/internal/domain"

	"github.com/google/uuid"
	kafkago "github.com/segmentio/kafka-go"
)

// Mock implementations for DI testing

type mockOrderStore struct {
	createWithOutboxFunc func(ctx context.Context, order *domain.Order) error
	getByIDFunc          func(ctx context.Context, id uuid.UUID) (*domain.Order, error)
	updateStatusFunc     func(ctx context.Context, id uuid.UUID, status domain.OrderStatus) error
}

func (m *mockOrderStore) CreateWithOutbox(ctx context.Context, order *domain.Order) error {
	return m.createWithOutboxFunc(ctx, order)
}

func (m *mockOrderStore) GetByID(ctx context.Context, id uuid.UUID) (*domain.Order, error) {
	return m.getByIDFunc(ctx, id)
}

func (m *mockOrderStore) UpdateStatus(ctx context.Context, id uuid.UUID, status domain.OrderStatus) error {
	return m.updateStatusFunc(ctx, id, status)
}

type mockProcessedMessageStore struct {
	isProcessedFunc   func(ctx context.Context, messageID, consumerGroup string) (bool, error)
	markProcessedFunc func(ctx context.Context, msg domain.ProcessedMessage) error
}

func (m *mockProcessedMessageStore) IsProcessed(ctx context.Context, messageID, consumerGroup string) (bool, error) {
	return m.isProcessedFunc(ctx, messageID, consumerGroup)
}

func (m *mockProcessedMessageStore) MarkProcessed(ctx context.Context, msg domain.ProcessedMessage) error {
	return m.markProcessedFunc(ctx, msg)
}

type mockOrderCache struct {
	setOrderFunc func(ctx context.Context, order *domain.Order) error
	getOrderFunc func(ctx context.Context, id string) (*domain.Order, error)
}

func (m *mockOrderCache) SetOrder(ctx context.Context, order *domain.Order) error {
	return m.setOrderFunc(ctx, order)
}

func (m *mockOrderCache) GetOrder(ctx context.Context, id string) (*domain.Order, error) {
	return m.getOrderFunc(ctx, id)
}

type mockEventPublisher struct {
	publishEventFunc func(ctx context.Context, topic string, key string, payload any) error
}

func (m *mockEventPublisher) PublishEvent(ctx context.Context, topic string, key string, payload any) error {
	return m.publishEventFunc(ctx, topic, key, payload)
}

func TestPaymentService_HandleOrderCreated_Success(t *testing.T) {
	orderID := uuid.New()
	eventID := "evt-001"

	updateCalled := false
	publishCalled := false
	markProcessedCalled := false

	orderStore := &mockOrderStore{
		updateStatusFunc: func(ctx context.Context, id uuid.UUID, status domain.OrderStatus) error {
			updateCalled = true
			if status != domain.StatusPaid {
				t.Errorf("expected status Paid, got %s", status)
			}
			return nil
		},
		getByIDFunc: func(ctx context.Context, id uuid.UUID) (*domain.Order, error) {
			return &domain.Order{
				ID:     orderID,
				Status: domain.StatusPaid,
			}, nil
		},
	}

	processedStore := &mockProcessedMessageStore{
		isProcessedFunc: func(ctx context.Context, messageID, consumerGroup string) (bool, error) {
			return false, nil
		},
		markProcessedFunc: func(ctx context.Context, msg domain.ProcessedMessage) error {
			markProcessedCalled = true
			return nil
		},
	}
	eventPublisher := &mockEventPublisher{
		publishEventFunc: func(ctx context.Context, topic string, key string, payload any) error {
			publishCalled = true
			return nil
		},
	}

	svc := NewPaymentService(orderStore, processedStore, eventPublisher, "test-group")

	event := domain.OrderCreatedEvent{
		EventID: eventID,
		OrderID: orderID.String(),
		UserID:  "user-001",
		Amount:  99.99,
	}
	payload, _ := json.Marshal(event)

	msg := kafkago.Message{
		Value: payload,
	}

	err := svc.HandleOrderCreated(context.Background(), msg)
	if err != nil {
		t.Fatalf("HandleOrderCreated returned error: %v", err)
	}

	if !updateCalled {
		t.Error("expected UpdateStatus to be called")
	}
	if !publishCalled {
		t.Error("expected PublishEvent to be called")
	}
	if !markProcessedCalled {
		t.Error("expected MarkProcessed to be called")
	}
}

func TestPaymentService_HandleOrderCreated_Idempotency(t *testing.T) {
	updateCalled := false
	publishCalled := false

	orderStore := &mockOrderStore{
		updateStatusFunc: func(ctx context.Context, id uuid.UUID, status domain.OrderStatus) error {
			updateCalled = true
			return nil
		},
	}

	processedStore := &mockProcessedMessageStore{
		isProcessedFunc: func(ctx context.Context, messageID, consumerGroup string) (bool, error) {
			return true, nil // already processed
		},
	}

	eventPublisher := &mockEventPublisher{
		publishEventFunc: func(ctx context.Context, topic string, key string, payload any) error {
			publishCalled = true
			return nil
		},
	}

	svc := NewPaymentService(orderStore, processedStore, eventPublisher, "test-group")

	event := domain.OrderCreatedEvent{
		EventID: "evt-duplicate",
		OrderID: uuid.New().String(),
		UserID:  "user-001",
	}
	payload, _ := json.Marshal(event)

	msg := kafkago.Message{
		Value: payload,
	}

	err := svc.HandleOrderCreated(context.Background(), msg)
	if err != nil {
		t.Fatalf("HandleOrderCreated returned error: %v", err)
	}

	if updateCalled {
		t.Error("expected UpdateStatus NOT to be called for duplicate message")
	}
	if publishCalled {
		t.Error("expected PublishEvent NOT to be called for duplicate message")
	}
}
