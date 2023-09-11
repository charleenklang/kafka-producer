package producer

import (
	"math/rand"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func GenerateMessages(topic string, numMessages int) []kafka.Message {
	var messages []kafka.Message

	// Seed the random number generator.
	rand.Seed(time.Now().UnixNano())

	for i := 0; i < numMessages; i++ {
		// Generate random key and value.
		key := randomString(3)
		value := randomString(6)

		// Create a Kafka message.
		message := &kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Key:            []byte(key),
			Value:          []byte(value),
		}

		messages = append(messages, *message)
	}

	return messages
}

func randomString(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	result := make([]byte, length)
	for i := range result {
		result[i] = charset[rand.Intn(len(charset))]
	}
	return string(result)
}
