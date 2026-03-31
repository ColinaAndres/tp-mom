package factory

import (
	"fmt"

	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

func CreateQueueMiddleware(queueName string, connectionSettings m.ConnSettings) (m.Middleware, error) {
	url := fmt.Sprintf("amqp://guest:guest@%s:%d/", connectionSettings.Hostname, connectionSettings.Port)
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, err
	}

	consumerChannel, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	publisherChannel, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	q, err := consumerChannel.QueueDeclare(
		queueName, // name
		false,     // durability
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,
	)

	if err != nil {
		return nil, err
	}

	return &QueueMiddleware{
		conn:             conn,
		consumerChannel:  consumerChannel,
		publisherChannel: publisherChannel,
		queue:            q,
		consumerTag:      q.Name,
		done:             make(chan struct{}),
	}, nil
}

func CreateExchangeMiddleware(exchange string, keys []string, connectionSettings m.ConnSettings) (m.Middleware, error) {
	return nil, nil
}
