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
	return nil
}

// creo que hay un error aca deberia devolver algun error pero no aparece en la interfaz
func (qm *QueueMiddleware) StopConsuming() {}

func (qm *QueueMiddleware) Send(msg m.Message) (err error) {
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
