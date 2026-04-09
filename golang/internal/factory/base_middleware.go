package factory

import (
	"context"
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

func (b *baseMiddleware) stopConsuming() error {
	err := b.consumerChannel.Cancel(b.consumerTag, false)
	if err != nil && b.consumerChannel.IsClosed() {
		return m.ErrMessageMiddlewareDisconnected
	}
	b.consumingWaiting.Wait()
	return nil
}

func (b *baseMiddleware) publish(ctx context.Context, exchange, routingKey string, msg m.Message) error {
	err := b.publisherChannel.PublishWithContext(ctx,
		exchange,   // exchange
		routingKey, // routing key
		false,      // mandatory
		false,      // immediate
		amqp.Publishing{
			ContentType: contentType,
			Body:        []byte(msg.Body),
		},
	)
	if b.publisherChannel.IsClosed() {
		return m.ErrMessageMiddlewareDisconnected
	}

	return err
}

func (b *baseMiddleware) close() error {
	b.stopConsuming()
	if err := b.publisherChannel.Close(); err != nil {
		return m.ErrMessageMiddlewareClose
	}
	if err := b.consumerChannel.Close(); err != nil {
		return m.ErrMessageMiddlewareClose
	}
	if err := b.conn.Close(); err != nil {
		return m.ErrMessageMiddlewareClose
	}
	return nil
}
