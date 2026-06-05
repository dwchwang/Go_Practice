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

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	admin := appkafka.NewTopicAdmin(cfg.KafkaBrokers)

	if err := admin.EnsureTopics(ctx, appkafka.DefaultTopics()); err != nil {
		log.Fatal("[KafkaTopicInit] ensure topics error:", err)
	}

	for _, topic := range appkafka.DefaultTopics() {
		log.Printf(
			"[KafkaTopicInit] topic ensured name=%s partitions=%d replication_factor=%d",
			topic.Name,
			topic.Partitions,
			topic.ReplicationFactor,
		)
	}
}