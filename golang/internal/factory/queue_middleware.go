package factory

import (
	"context"
	"sync"

	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

type QueueMiddleware struct {
	conn             *amqp.Connection
	consumerChannel  *amqp.Channel
	publisherChannel *amqp.Channel
	queue            string
	consumerTag      string
	consumingWaiting sync.WaitGroup
}

func (qm *QueueMiddleware) StartConsuming(callbackFunc func(msg m.Message, ack func(), nack func())) error {
	msgs, err := qm.consumerChannel.Consume(
		qm.queue,       // queue
		qm.consumerTag, // consumer
		false,          // auto-ack
		false,          // exclusive
		false,          // no-local
		false,          // no-wait
		nil,            // args
	)
	if err != nil {
		return m.ErrMessageMiddlewareMessage
	}

	qm.consumingWaiting.Add(1)
	defer qm.consumingWaiting.Done()

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

	if qm.consumerChannel.IsClosed() {
		return m.ErrMessageMiddlewareDisconnected
	}

	return nil
}

func (qm *QueueMiddleware) StopConsuming() error {
	err := qm.consumerChannel.Cancel(qm.consumerTag, false)
	if err != nil && qm.consumerChannel.IsClosed() {
		// Si el canal no esta cerrado significa que nunca
		// se inicio el consumo, caso contrario se devuelve el error
		return m.ErrMessageMiddlewareDisconnected
	}

	qm.consumingWaiting.Wait()
	return nil
}

func (qm *QueueMiddleware) Send(msg m.Message) error {
	// se opta por usar un ctx para mantenernos en un tipo limite
	// y seguir la propuesta de RabbitMQ
	ctx, cancel := context.WithTimeout(context.Background(), publishTimeout)
	defer cancel()

	err := qm.publisherChannel.PublishWithContext(ctx,
		"",       // exchange
		qm.queue, // routing key
		false,    // mandatory
		false,    // immediate
		amqp.Publishing{
			ContentType: contentType,
			Body:        []byte(msg.Body),
		})
	if err = mapMiddlewareError(err); err != nil {
		return err
	}
	return nil
}

func (qm *QueueMiddleware) Close() error {
	// si ocurre un error al parar el consumo,
	// se opta por seguir intentando cerrar los canales y la conexión
	qm.StopConsuming()
	err := qm.publisherChannel.Close()
	if err != nil {
		return m.ErrMessageMiddlewareClose
	}
	err = qm.consumerChannel.Close()
	if err != nil {
		return m.ErrMessageMiddlewareClose
	}
	err = qm.conn.Close()
	if err != nil {
		return m.ErrMessageMiddlewareClose
	}
	return nil
}
