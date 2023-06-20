package kafka

import cKafka "github.com/confluentinc/confluent-kafka-go/kafka"

type Consumer struct {
	ConfigMap *cKafka.ConfigMap
	Topics    []string
}

func NewConsumer(configMap *cKafka.ConfigMap, topics []string) *Consumer {
	return &Consumer{
		ConfigMap: configMap,
		Topics:    topics,
	}
}

func (c *Consumer) Consume(msgChan chan *cKafka.Message) error {
	consumer, err := cKafka.NewConsumer(c.ConfigMap)
	if err != nil {
		panic(err)
	}
	err = consumer.SubscribeTopics(c.Topics, nil)
	if err != nil {
		panic(err)
	}
	for {
		msg, err := consumer.ReadMessage(-1)
		if err == nil {
			msgChan <- msg
		}
	}
}
