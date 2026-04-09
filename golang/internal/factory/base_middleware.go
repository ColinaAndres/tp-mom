package factory

import (
	"sync"

	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

type baseMiddleware struct {
	conn             *amqp.Connection
	consumerChannel  *amqp.Channel
	publisherChannel *amqp.Channel
	consumerTag      string
	consumingWaiting sync.WaitGroup
}

func (b *baseMiddleware) runConsumerLoop(msgs <-chan amqp.Delivery, callbackFunc func(msg m.Message, ack func(), nack func())) error {
	b.consumingWaiting.Add(1)
	defer b.consumingWaiting.Done()

	var ackError error
	for msg := range msgs {
		callbackFunc(
			m.Message{Body: string(msg.Body)},
			func() {
				if err := msg.Ack(false); err != nil {
					ackError = m.ErrMessageMiddlewareMessage
				}
			},
			func() {
				if err := msg.Nack(false, true); err != nil {
					ackError = m.ErrMessageMiddlewareMessage
				}
			},
		)
		if ackError != nil {
			return ackError
		}
	}

	if b.consumerChannel.IsClosed() {
		return m.ErrMessageMiddlewareDisconnected
	}
	return nil
}
