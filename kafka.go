package main

import (
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/compress"
)

// NewKafkaProducer returns a new writer for writing messages to a given topic.
func NewKafkaProducer(kafkaAddress, topic string) (*kafka.Writer, error) {
	conn, err := kafka.Dial("tcp", kafkaAddress)
	if err != nil {
		return nil, err
	}

	err = conn.CreateTopics(kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     6,
		ReplicationFactor: 1,
	})
	if err != nil {
		return nil, err
	}

	return kafka.NewWriter(kafka.WriterConfig{
		Brokers:          []string{kafkaAddress},
		Topic:            topic,
		Balancer:         &kafka.LeastBytes{},
		RequiredAcks:     1,
		CompressionCodec: &compress.SnappyCodec,
	}), nil
}
