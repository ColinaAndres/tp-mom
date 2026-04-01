package factory

import (
	"context"
	"time"

	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

type ExchangeMiddleware struct {
	conn             *amqp.Connection
	publisherChannel *amqp.Channel
	consumerChannel  *amqp.Channel
	channel          *amqp.Channel
	exchange         string
	keys             []string
}

func (em *ExchangeMiddleware) StartConsuming(callbackFunc func(msg m.Message, ack func(), nack func())) (err error) {
	queue, err := em.consumerChannel.QueueDeclare(
		"",    // name
		false, // durability
		true,  // delete when unused
		true,  // exclusive
		false, // no-wait
		nil,
	)

	if err != nil {
		return m.ErrMessageMiddlewareMessage
	}

	for _, key := range em.keys {
		err = em.consumerChannel.QueueBind(
			queue.Name,  // queue name
			key,         // routing key
			em.exchange, // exchange
			false,       // no-wait
			nil,
		)
		if err != nil {
			return m.ErrMessageMiddlewareMessage
		}
	}

	msgs, err := em.consumerChannel.Consume(
		queue.Name, // queue
		"",         // consumer
		false,      // auto-ack
		false,      // exclusive
		false,      // no-local
		false,      // no-wait
		nil,        // args
	)
	if err != nil {
		return m.ErrMessageMiddlewareMessage
	}

	for msg := range msgs {
		callbackFunc(m.Message{Body: string(msg.Body)},
			func() {
				if err := msg.Ack(false); err != nil {
					err = m.ErrMessageMiddlewareMessage
				}
			},
			func() {
				if err := msg.Nack(false, true); err != nil {
					err = m.ErrMessageMiddlewareMessage
				}
			})
	}
	return err
}

func (em *ExchangeMiddleware) StopConsuming() {
}

func (em *ExchangeMiddleware) Send(msg m.Message) (err error) {
	// se opta por usar un ctx para mantenernos en un tipo limite
	// y seguir la propuesta de RabbitMQ
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for _, key := range em.keys {
		err = em.channel.PublishWithContext(ctx,
			em.exchange, // exchange
			key,         // routing key
			false,       // mandatory
			false,       // immediate
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        []byte(msg.Body),
			},
		)
		if err != nil {
			return m.ErrMessageMiddlewareMessage
		}
	}

	return nil
}

func (em *ExchangeMiddleware) Close() error {
	err := em.channel.Close()
	if err != nil {
		return m.ErrMessageMiddlewareClose
	}
	err = em.conn.Close()
	if err != nil {
		return m.ErrMessageMiddlewareClose
	}
	return nil
}
