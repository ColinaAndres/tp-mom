package factory

import (
	"time"

	"crypto/rand"
	"encoding/hex"

	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	publishTimeout = 5 * time.Second
	contentType    = "text/plain"
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

func SimpleCryptoID(tamano int) string {
	b := make([]byte, tamano)
	rand.Read(b)
	return hex.EncodeToString(b)
}
