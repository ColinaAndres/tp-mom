package factory

import (
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

type baseMiddleware struct {
	conn             *amqp.Connection
	consumerChannel  *amqp.Channel
	publisherChannel *amqp.Channel
	consumerTag      string
	consumingWaiting sync.WaitGroup
}
