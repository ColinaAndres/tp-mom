package factory

import (
	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

func mapMiddlewareError(err error) error {
	switch err {
	case nil:
		return nil
	case amqp.ErrClosed:
		return m.ErrMessageMiddlewareDisconnected
	default:
		return m.ErrMessageMiddlewareMessage
	}
}
