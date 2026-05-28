package main

import (
	"context"
	"log"
	"time"

	"order-processing/internal/config"
	appkafka "order-processing/internal/kafka"
)

func main() {
	cfg := config.Load()

	producer := appkafka.NewProducer(cfg.KafkaBrokers)
	defer func() {
		if err := producer.Close(); err != nil {
			log.Printf("[BadMessageTest] close producer error: %v", err)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	key := "bad-order-001"

	// Cố tình gửi JSON sai format để Payment Service unmarshal fail.
	badPayload := []byte(`{"order_id":`)

	if err := producer.PublishEvent(
		ctx,
		appkafka.TopicOrderCreated,
		key,
		badPayload,
	); err != nil {
		log.Fatal("[BadMessageTest] publish bad message error:", err)
	}

	log.Printf(
		"[BadMessageTest] published bad message topic=%s key=%s",
		appkafka.TopicOrderCreated,
		key,
	)
}
