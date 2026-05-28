package kafka

const (
	TopicOrderCreated          = "order.created"
	TopicOrderPaymentProcessed = "order.payment.processed"

	TopicOrderCreatedDLQ          = "order.created.DLQ"
	TopicOrderPaymentProcessedDLQ = "order.payment.processed.DLQ"
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
		{
			Name:              TopicOrderPaymentProcessed,
			Partitions:        3,
			ReplicationFactor: 1,
		},
		{
			Name:              TopicOrderCreatedDLQ,
			Partitions:        3,
			ReplicationFactor: 1,
		},
		{
			Name:              TopicOrderPaymentProcessedDLQ,
			Partitions:        3,
			ReplicationFactor: 1,
		},
	}
}
