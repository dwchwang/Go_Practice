package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"order-processing/internal/config"
	"order-processing/internal/database"
	appkafka "order-processing/internal/kafka"
	"order-processing/internal/outbox"
	"order-processing/internal/repository"
)

func main() {
	cfg := config.Get()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	db, err := database.NewPostgres(cfg.PostgresDSN)
	if err != nil {
		log.Fatal("[OutboxRelay] connect postgres error:", err)
	}

	sqlDB, err := db.DB()
	if err != nil {
		log.Fatal("[OutboxRelay] get sql db error:", err)
	}
	defer sqlDB.Close()

	producer := appkafka.NewProducer(cfg.KafkaBrokers)
	defer func() {
		if err := producer.Close(); err != nil {
			log.Printf("[OutboxRelay] close producer error: %v", err)
		}
	}()

	outboxRepo := repository.NewOutboxRepository(db)

	relay := outbox.NewRelay(
		outboxRepo,
		producer,
		2*time.Second,
		100,
	)

	relay.Start(ctx)

	log.Println("[OutboxRelay] shutdown complete")
}