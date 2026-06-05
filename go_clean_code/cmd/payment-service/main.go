package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"order-processing/internal/cache"
	"order-processing/internal/config"
	"order-processing/internal/database"
	appkafka "order-processing/internal/kafka"
	"order-processing/internal/repository"
	"order-processing/internal/service"
)

const paymentConsumerGroup = "payment-service-group"

func main() {
	cfg := config.Get()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	db, err := database.NewPostgres(cfg.PostgresDSN)
	if err != nil {
		log.Fatal("[PaymentService] connect postgres error:", err)
	}

	sqlDB, err := db.DB()
	if err != nil {
		log.Fatal("[PaymentService] get sql db error:", err)
	}
	defer sqlDB.Close()

	redisCache, err := cache.NewRedisCache(ctx, cfg.RedisAddr)
	if err != nil {
		log.Fatal("[PaymentService] connect redis error:", err)
	}
	defer redisCache.Close()

	dbRepo := repository.NewOrderRepository(db)
	orderRepo := repository.NewCachedOrderRepository(dbRepo, redisCache)
	processedRepo := repository.NewProcessedMessageRepository(db)

	producer := appkafka.NewProducer(cfg.KafkaBrokers)
	defer func() {
		if err := producer.Close(); err != nil {
			log.Printf("[PaymentService] close producer error: %v", err)
		}
	}()

	paymentService := service.NewPaymentService(
		orderRepo,
		processedRepo,
		producer,
		paymentConsumerGroup,
	)

	consumer := appkafka.NewConsumer(
		cfg.KafkaBrokers,
		appkafka.TopicOrderCreated,
		paymentConsumerGroup,
		appkafka.TopicOrderCreatedDLQ,
	)
	defer func() {
		if err := consumer.Close(); err != nil {
			log.Printf("[PaymentService] close consumer error: %v", err)
		}
	}()

	log.Println("[PaymentService] started")

	if err := consumer.Consume(ctx, paymentService.HandleOrderCreated); err != nil {
		log.Printf("[PaymentService] consumer stopped: %v", err)
	}

	log.Println("[PaymentService] shutdown complete")
}
