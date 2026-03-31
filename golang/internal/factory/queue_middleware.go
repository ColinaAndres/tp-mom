package factory

import (
	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

type QueueMiddleware struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	queue   amqp.Queue
}

func (qm *QueueMiddleware) StartConsuming(callbackFunc func(msg m.Message, ack func(), nack func())) (err error) {
	msgs, err := qm.channel.Consume(
		qm.queue.Name, // queue
		"",            // consumer
		false,         // auto-ack
		false,         // exclusive
		false,         // no-local
		false,         // no-wait
		nil,           // args
	)
	if err != nil {
		return m.ErrMessageMiddlewareMessage
	}

	go func() {
		for msg := range msgs {
			callbackFunc(m.Message{Body: string(msg.Body)}, ack(msg), nack(msg))
		}
	}()
	return nil
}

// creo que hay un error aca deberia devolver algun error pero no aparece en la interfaz
func (qm *QueueMiddleware) StopConsuming() {
	// TODO: Revisar si necesio un tag o si asi basta
	qm.channel.Cancel("", false)
}

func (qm *QueueMiddleware) Send(msg m.Message) (err error) {
	//TODO: preguntar si se usa un context o no
	err = qm.channel.Publish(
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
	err := qm.channel.Close()
	if err != nil {
		return m.ErrMessageMiddlewareClose
	}
	err = qm.conn.Close()
	if err != nil {
		return m.ErrMessageMiddlewareClose
	}
	return nil
}

func ack(msg amqp.Delivery) func() {
	// TODO: preguntar como manejar los errorres
	return func() {
		msg.Ack(false)
	}
}

func nack(msg amqp.Delivery) func() {
	// TODO: preguntar como manejar los errorres
	return func() {
		msg.Nack(false, true)
	}
}
