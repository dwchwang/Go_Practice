package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"order-processing/internal/config"
	appkafka "order-processing/internal/kafka"
	"order-processing/internal/service"
)

func main() {
	cfg := config.Load()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	notificationService := service.NewNotificationService()

	consumer := appkafka.NewConsumer(
		cfg.KafkaBrokers,
		appkafka.TopicOrderPaymentProcessed,
		"notification-service-group",
	)
	defer func() {
		if err := consumer.Close(); err != nil {
			log.Printf("[NotificationService] close consumer error: %v", err)
		}
	}()

	log.Println("[NotificationService] started")

	if err := consumer.Consume(ctx, notificationService.HandlePaymentProcessed); err != nil {
		log.Printf("[NotificationService] consumer stopped: %v", err)
	}

	log.Println("[NotificationService] shutdown complete")
}
