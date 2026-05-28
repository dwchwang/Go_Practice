package service

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"time"

	"order-processing/internal/cache"
	"order-processing/internal/domain"
	appkafka "order-processing/internal/kafka"
	"order-processing/internal/repository"

	"github.com/google/uuid"
	kafkago "github.com/segmentio/kafka-go"
)

type PaymentService struct {
	orderRepo  *repository.OrderRepository
	redisCache *cache.RedisCache
	producer   *appkafka.Producer
	rng        *rand.Rand
}

func NewPaymentService(
	orderRepo *repository.OrderRepository,
	redisCache *cache.RedisCache,
	producer *appkafka.Producer,
) *PaymentService {
	return &PaymentService{
		orderRepo:  orderRepo,
		redisCache: redisCache,
		producer:   producer,
		rng:        rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (s *PaymentService) HandleOrderCreated(ctx context.Context, msg kafkago.Message) error {
	var event domain.OrderCreatedEvent

	if err := json.Unmarshal(msg.Value, &event); err != nil {
		return fmt.Errorf("unmarshal OrderCreatedEvent: %w", err)
	}

	log.Printf(
		"[PaymentService] processing payment order_id=%s user_id=%s amount=%.2f",
		event.OrderID,
		event.UserID,
		event.Amount,
	)

	orderID, err := uuid.Parse(event.OrderID)
	if err != nil {
		return fmt.Errorf("parse order id: %w", err)
	}

	// Giả lập payment:
	// 90% success, 10% failed.
	success := s.rng.Float32() > 0.1

	newStatus := domain.StatusPaid
	if !success {
		newStatus = domain.StatusCancelled
	}

	if err := s.orderRepo.UpdateStatus(ctx, orderID, newStatus); err != nil {
		return fmt.Errorf("update order status: %w", err)
	}

	// Sau khi update DB, đọc lại order mới nhất để update Redis cache.
	updatedOrder, err := s.orderRepo.GetByID(ctx, orderID)
	if err != nil {
		return fmt.Errorf("get updated order: %w", err)
	}

	if err := s.redisCache.SetOrder(ctx, updatedOrder); err != nil {
		log.Printf("[PaymentService] update redis cache error: %v", err)
	}

	paymentEvent := domain.PaymentProcessedEvent{
		OrderID: event.OrderID,
		UserID:  event.UserID,
		Success: success,
	}

	if err := s.producer.PublishEvent(
		ctx,
		appkafka.TopicOrderPaymentProcessed,
		event.OrderID,
		paymentEvent,
	); err != nil {
		return fmt.Errorf("publish PaymentProcessedEvent: %w", err)
	}

	if success {
		log.Printf("[PaymentService] payment SUCCESS order_id=%s", event.OrderID)
	} else {
		log.Printf("[PaymentService] payment FAILED order_id=%s", event.OrderID)
	}

	return nil
}
