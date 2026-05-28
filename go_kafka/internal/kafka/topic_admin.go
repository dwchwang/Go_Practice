package kafka

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

type TopicAdmin struct {
	brokers []string
}

func NewTopicAdmin(brokers []string) *TopicAdmin {
	return &TopicAdmin{
		brokers: brokers,
	}
}

func (a *TopicAdmin) EnsureTopics(ctx context.Context, topics []TopicDefinition) error {
	if len(a.brokers) == 0 {
		return fmt.Errorf("kafka brokers is empty")
	}

	conn, err := kafkago.DialContext(ctx, "tcp", a.brokers[0])
	if err != nil {
		return fmt.Errorf("dial kafka broker: %w", err)
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		return fmt.Errorf("get kafka controller: %w", err)
	}

	controllerAddr := net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port))

	controllerConn, err := kafkago.DialContext(ctx, "tcp", controllerAddr)
	if err != nil {
		return fmt.Errorf("dial kafka controller: %w", err)
	}
	defer controllerConn.Close()

	configs := make([]kafkago.TopicConfig, 0, len(topics))

	for _, topic := range topics {
		configs = append(configs, kafkago.TopicConfig{
			Topic:             topic.Name,
			NumPartitions:     topic.Partitions,
			ReplicationFactor: topic.ReplicationFactor,
			ConfigEntries: []kafkago.ConfigEntry{
				{
					ConfigName:  "cleanup.policy",
					ConfigValue: "delete",
				},
				{
					ConfigName:  "retention.ms",
					ConfigValue: strconv.FormatInt((7 * 24 * time.Hour).Milliseconds(), 10),
				},
			},
		})
	}

	if err := controllerConn.CreateTopics(configs...); err != nil {
		return fmt.Errorf("create kafka topics: %w", err)
	}

	return nil
}
