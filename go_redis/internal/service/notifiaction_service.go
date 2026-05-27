package service

import (
	"context"
	"log"

	"github.com/redis/go-redis/v9"
)

type NotificationService struct {
	rdb *redis.Client
}

func NewNotificationService(rdb *redis.Client) *NotificationService {
	return &NotificationService{rdb: rdb}
}

func (s *NotificationService) PublishOrderCreated(ctx context.Context, userID string) error {
	message := "new order created by user: " + userID

	return s.rdb.Publish(ctx, "notif:orders", message).Err()
}

func (s *NotificationService) SubscribeOrderNotifications(ctx context.Context) {
	pubsub := s.rdb.Subscribe(ctx, "notif:orders")
	defer pubsub.Close()

	ch := pubsub.Channel()

	log.Println("Subscribed to notif:orders")

	for msg := range ch {
		log.Println("Order notification:", msg.Payload)
	}
}
