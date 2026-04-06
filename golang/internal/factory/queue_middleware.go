package factory

import (
	"context"
	"time"

	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

type QueueMiddleware struct {
	conn             *amqp.Connection
	consumerChannel  *amqp.Channel
	publisherChannel *amqp.Channel
	queue            amqp.Queue
	consumerTag      string
	done             chan struct{}
	consuming        bool
}

func (qm *QueueMiddleware) StartConsuming(callbackFunc func(msg m.Message, ack func(), nack func())) error {
	qm.consuming = true
	msgs, err := qm.consumerChannel.Consume(
		qm.queue.Name,  // queue
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

	// Canal que al cerrarse indica que se dejo de consumir y procesar mensajes
	defer close(qm.done)

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

	qm.consuming = false
	return err
}

func (qm *QueueMiddleware) StopConsuming() error {
	if !qm.consuming {
		return nil
	}

	err := qm.consumerChannel.Cancel(qm.consumerTag, false)
	if err != nil {
		return m.ErrMessageMiddlewareDisconnected
	}

	// Se espera a que se deje de consumir y procesar mensajes antes de retornar
	<-qm.done
	qm.consuming = false
	return nil
}

func (qm *QueueMiddleware) Send(msg m.Message) error {
	// se opta por usar un ctx para mantenernos en un tipo limite
	// y seguir la propuesta de RabbitMQ
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := qm.publisherChannel.PublishWithContext(ctx,
		"",            // exchange
		qm.queue.Name, // routing key
		false,         // mandatory
		false,         // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(msg.Body),
		})
	if err != nil {
		return m.ErrMessageMiddlewareMessage
	}
	return nil
}

func (qm *QueueMiddleware) Close() error {
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
