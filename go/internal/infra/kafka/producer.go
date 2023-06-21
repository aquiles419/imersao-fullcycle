package kafka

import cKafka "github.com/confluentinc/confluent-kafka-go/kafka"

type Producer struct {
	ConfigMap *cKafka.ConfigMap
}

func NewKafkaProducer(configMap *cKafka.ConfigMap) *Producer {
	return &Producer{
		ConfigMap: configMap,
	}
}

func (p *Producer) Publish(msg interface{}, key []byte, topic string) error {
	producer, err := cKafka.NewProducer(p.ConfigMap)
	if err != nil {
		return err
	}
	message := &cKafka.Message{
		TopicPartition: cKafka.TopicPartition{Topic: &topic, Partition: cKafka.PartitionAny},
		Key:            key,
		Value:          msg.([]byte),
	}

	err = producer.Produce(message, nil)
	if err != nil {
		return err
	}
	return nil
}
