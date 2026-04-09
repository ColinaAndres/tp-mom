package factory

import (
	"context"
	"sync"

	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

type ExchangeMiddleware struct {
	conn             *amqp.Connection
	publisherChannel *amqp.Channel
	consumerChannel  *amqp.Channel
	channel          *amqp.Channel
	exchange         string
	consumerTag      string
	keys             []string
	consumingWaiting sync.WaitGroup
}

func (em *ExchangeMiddleware) StartConsuming(callbackFunc func(msg m.Message, ack func(), nack func())) error {
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

	em.consumerTag = queue.Name
	msgs, err := em.consumerChannel.Consume(
		queue.Name,     // queue
		em.consumerTag, // consumer
		false,          // auto-ack
		false,          // exclusive
		false,          // no-local
		false,          // no-wait
		nil,            // args
	)
	if err != nil {
		return m.ErrMessageMiddlewareMessage
	}

	em.consumingWaiting.Add(1)
	defer em.consumingWaiting.Done()

	var ackError error
	for msg := range msgs {
		callbackFunc(m.Message{Body: string(msg.Body)},
			func() {
				if err := msg.Ack(false); err != nil {
					ackError = m.ErrMessageMiddlewareMessage
				}
			},
			func() {
				if err := msg.Nack(false, true); err != nil {
					ackError = m.ErrMessageMiddlewareMessage
				}
			})
		if ackError != nil {
			return ackError
		}
	}

	if em.consumerChannel.IsClosed() {
		return m.ErrMessageMiddlewareDisconnected
	}

	return nil
}

func (em *ExchangeMiddleware) StopConsuming() error {
	err := em.consumerChannel.Cancel(em.consumerTag, false)
	if err != nil && em.consumerChannel.IsClosed() {
		// Si el canal no esta cerrado significa que nunca
		// se inicio el consumo, caso contrario se devuelve el error
		return m.ErrMessageMiddlewareDisconnected
	}

	em.consumingWaiting.Wait()
	return nil
}

func (em *ExchangeMiddleware) Send(msg m.Message) error {
	// se opta por usar un ctx para mantenernos en un tipo limite
	// y seguir la propuesta de RabbitMQ
	ctx, cancel := context.WithTimeout(context.Background(), publishTimeout)
	defer cancel()

	for _, key := range em.keys {
		err := em.publisherChannel.PublishWithContext(ctx,
			em.exchange, // exchange
			key,         // routing key
			false,       // mandatory
			false,       // immediate
			amqp.Publishing{
				ContentType: contentType,
				Body:        []byte(msg.Body),
			},
		)

		if err = mapMiddlewareError(err); err != nil {
			return err
		}
	}

	return nil
}

func (em *ExchangeMiddleware) Close() error {
	// si ocurre un error al parar el consumo,
	// se opta por seguir intentando cerrar los canales y la conexión
	em.StopConsuming()
	err := em.publisherChannel.Close()
	if err != nil {
		return m.ErrMessageMiddlewareClose
	}

	err = em.consumerChannel.Close()
	if err != nil {
		return m.ErrMessageMiddlewareClose
	}

	err = em.conn.Close()
	if err != nil {
		return m.ErrMessageMiddlewareClose
	}
	return nil
}
