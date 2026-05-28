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
	orderRepo       *repository.OrderRepository
	processedRepo   *repository.ProcessedMessageRepository
	redisCache      *cache.RedisCache
	producer        *appkafka.Producer
	consumerGroup   string
	rng             *rand.Rand
}

func NewPaymentService(
	orderRepo *repository.OrderRepository,
	processedRepo *repository.ProcessedMessageRepository,
	redisCache *cache.RedisCache,
	producer *appkafka.Producer,
	consumerGroup string,
) *PaymentService {
	return &PaymentService{
		orderRepo:     orderRepo,
		processedRepo: processedRepo,
		redisCache:    redisCache,
		producer:      producer,
		consumerGroup: consumerGroup,
		rng:           rand.New(rand.NewSource(time.Now().UnixNano())),
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

	processed, err := s.processedRepo.IsProcessed(ctx, messageID, s.consumerGroup)
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

	if err := s.orderRepo.UpdateStatus(ctx, orderID, newStatus); err != nil {
		return fmt.Errorf("update order status: %w", err)
	}

	updatedOrder, err := s.orderRepo.GetByID(ctx, orderID)
	if err != nil {
		return fmt.Errorf("get updated order: %w", err)
	}

	if err := s.redisCache.SetOrder(ctx, updatedOrder); err != nil {
		log.Printf("[PaymentService] update redis cache error: %v", err)
	}

	paymentEvent := domain.PaymentProcessedEvent{
		EventID: messageID + ":payment-processed",
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

	if err := s.processedRepo.MarkProcessed(ctx, domain.ProcessedMessage{
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

func fallbackMessageID(msg kafkago.Message) string {
	return fmt.Sprintf("%s:%d:%d", msg.Topic, msg.Partition, msg.Offset)
}