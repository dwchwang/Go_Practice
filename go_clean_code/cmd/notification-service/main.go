package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"order-processing/internal/config"
	"order-processing/internal/database"
	"order-processing/internal/factory/notification"
	appkafka "order-processing/internal/kafka"
	"order-processing/internal/repository"
	"order-processing/internal/service"
)

const notificationConsumerGroup = "notification-service-group"

func main() {
	cfg := config.Get()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	db, err := database.NewPostgres(cfg.PostgresDSN)
	if err != nil {
		log.Fatal("[NotificationService] connect postgres error:", err)
	}

	sqlDB, err := db.DB()
	if err != nil {
		log.Fatal("[NotificationService] get sql db error:", err)
	}
	defer sqlDB.Close()

	processedRepo := repository.NewProcessedMessageRepository(db)

	// Abstract Factory: chọn kênh thông báo (Email hoặc Console)
	notifFactory := &notification.EmailNotificationFactory{}

	notificationService := service.NewNotificationService(
		processedRepo,
		notifFactory,
		notificationConsumerGroup,
	)

	consumer := appkafka.NewConsumer(
		cfg.KafkaBrokers,
		appkafka.TopicOrderPaymentProcessed,
		notificationConsumerGroup,
		appkafka.TopicOrderPaymentProcessedDLQ,
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
