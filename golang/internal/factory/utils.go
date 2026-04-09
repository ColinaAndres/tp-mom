package factory

import (
	"time"

	"crypto/rand"
	"encoding/hex"

	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	publishTimeout  = 5 * time.Second
	contentType     = "text/plain"
	defaultExchange = ""
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
	//Alternativa para no usar uuid sin permiso de la catedra, no es
	//criptográficamente seguro pero es suficiente para generar un consumerTag único en el ejercicio.
	//Recibe el tamaño del ID a generar en bytes, y devuelve un string hexadecimal de ese tamaño.
	b := make([]byte, tamano)
	rand.Read(b)
	return hex.EncodeToString(b)
}
