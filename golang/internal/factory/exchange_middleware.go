package factory

import (
	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
)

type ExchangeMiddleware struct{}

func (em *ExchangeMiddleware) StartConsuming(callbackFunc func(msg m.Message, ack func(), nack func())) (err error) {
	return nil
}

func (em *ExchangeMiddleware) StopConsuming() {

}

func (em *ExchangeMiddleware) Send(msg m.Message) (err error) {
	return nil
}

func (em *ExchangeMiddleware) Close() error {
	return nil
}
