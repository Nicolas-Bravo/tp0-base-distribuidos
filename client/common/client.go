package common

import (
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

// ClientConfig Configuration used by the client
type ClientConfig struct {
	ID            string
	ServerAddress string
	LoopAmount    int
	LoopPeriod    time.Duration
	BatchMax      int
}

// Client Entity that encapsulates how
type Client struct {
	config  ClientConfig
	conn    net.Conn
	stopped chan struct{}
}

// NewClient Initializes a new client receiving the configuration
// as a parameter
func NewClient(config ClientConfig) *Client {
	client := &Client{
		config:  config,
		stopped: make(chan struct{}),
	}

	// Manejo básico de señales para terminar el loop de forma graceful
	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, syscall.SIGTERM)
		<-c
		log.Infof("action: shutdown | result: in_progress | client_id: %v", client.config.ID)
		close(client.stopped)
		if client.conn != nil {
			client.conn.Close()
		}
	}()

	return client
}

// CreateClientSocket Initializes client socket. In case of
// failure, error is printed in stdout/stderr and returns the error
func (c *Client) createClientSocket() error {
	conn, err := net.Dial("tcp", c.config.ServerAddress)
	if err != nil {
		log.Criticalf(
			"action: connect | result: fail | client_id: %v | error: %v",
			c.config.ID,
			err,
		)
		return err
	}
	c.conn = conn
	return nil
}

// StartClientLoop ahora envía apuestas en batches leídas desde el CSV de la agencia
// reutilizando una única conexión TCP durante todo el proceso.
func (c *Client) StartClientLoop() {
	csvPath := filepath.Join("/data", "agency-"+c.config.ID+".csv")
	bets, err := LoadBetsFromCSV(csvPath, c.config.ID)
	if err != nil {
		log.Criticalf("action: load_bets | result: fail | client_id: %v | error: %v", c.config.ID, err)
		return
	}
	log.Debugf("action: load_bets | result: success | client_id: %v | total: %d", c.config.ID, len(bets))

	batchSize := c.config.BatchMax
	log.Debugf("action: config_batch | client_id: %v | batch_max: %d", c.config.ID, batchSize)

	// 1. Abrimos la conexión una única vez al principio
	if err := c.createClientSocket(); err != nil {
		return
	}
	// 2. Nos aseguramos de cerrar la conexión siempre al salir de la función
	defer func() {
		if c.conn != nil {
			c.conn.Close()
			c.conn = nil
		}
	}()

	for offset := 0; offset < len(bets); offset += batchSize {
		select {
		case <-c.stopped:
			log.Infof("action: exit | result: success | client_id: %v", c.config.ID)
			return
		default:
		}

		end := offset + batchSize
		if end > len(bets) {
			end = len(bets)
		}
		batch := bets[offset:end]

		log.Debugf("action: batch_loop | client_id: %v | offset: %d | end: %d | size: %d", c.config.ID, offset, end, len(batch))

		// Enviamos el lote por la conexión ya establecida
		if err := sendBatch(c.conn, batch); err != nil {
			log.Errorf("action: receive_message | result: fail | client_id: %v | error: %v", c.config.ID, err)
			return
		}

		log.Infof("action: apuesta_enviada | result: success | size: %d", len(batch))

		time.Sleep(c.config.LoopPeriod)
	}

	// Notificar fin de envíos al servidor por la misma conexión
	if err := SendEnd(c.conn, c.config.ID); err != nil {
		log.Errorf("action: send_end | result: fail | client_id: %v | error: %v", c.config.ID, err)
		return
	}

	// Consultar ganadores con reintentos hasta que el servidor tenga el sorteo listo
	for {
		count, done, err := SendWinnersRequest(c.conn, c.config.ID)
		if err != nil {
			log.Errorf("action: consulta_ganadores | result: fail | client_id: %v | error: %v", c.config.ID, err)
			return
		}
		if !done {
			// El servidor aún no realizó el sorteo; esperamos un poco y reintentamos
			time.Sleep(100 * time.Millisecond)
			continue
		}

		log.Infof("action: consulta_ganadores | result: success | cant_ganadores: %d", count)
		break
	}

	log.Infof("action: loop_finished | result: success | client_id: %v", c.config.ID)
}
