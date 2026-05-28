package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"order-processing/internal/config"
	"order-processing/internal/database"
	appkafka "order-processing/internal/kafka"
	"order-processing/internal/repository"
	"order-processing/internal/service"
)

const notificationConsumerGroup = "notification-service-group"

func main() {
	cfg := config.Load()

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

	notificationService := service.NewNotificationService(
		processedRepo,
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
