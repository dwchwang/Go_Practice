package kafka

const (
	TopicOrderCreated = "order.created"
)

type TopicDefinition struct {
	Name              string
	Partitions        int
	ReplicationFactor int
}

func DefaultTopics() []TopicDefinition {
	return []TopicDefinition{
		{
			Name:              TopicOrderCreated,
			Partitions:        3,
			ReplicationFactor: 1,
		},
	}
}