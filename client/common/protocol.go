package common

import (
	"bufio"
	"fmt"
	"net"
)

// encodeBet serializa una apuesta en una sola línea de texto.
func encodeBet(b Bet) string {
	// Protocolo: agency|firstName|lastName|dni|birthdate|number\n
	return fmt.Sprintf("%s|%s|%s|%s|%s|%s\n",
		b.AgencyID, b.FirstName, b.LastName, b.DNI, b.Birthdate, b.Number)
}

// writeAll escribe todos los bytes del slice en el socket, evitando short write.
func writeAll(conn net.Conn, data []byte) error {
	written := 0
	for written < len(data) {
		n, err := conn.Write(data[written:])
		if err != nil {
			return err
		}
		written += n
	}
	return nil
}

// sendBet envía una apuesta por el socket asegurando escribir toda la línea.
func sendBet(conn net.Conn, b Bet) error {
	payload := encodeBet(b)
	return writeAll(conn, []byte(payload))
}

// readLine lee una línea completa terminada en \n evitando short-read.
func readLine(conn net.Conn) (string, error) {
	reader := bufio.NewReader(conn)
	line, err := reader.ReadString('\n')
	return line, err
}

// sendBetAndWaitAck envía la apuesta y espera un ACK fijo del servidor.
func sendBetAndWaitAck(conn net.Conn, b Bet) error {
	if err := sendBet(conn, b); err != nil {
		return err
	}
	line, err := readLine(conn)
	if err != nil {
		return err
	}
	// Protocolo de ACK: "OK\n"
	if line != "OK\n" {
		return fmt.Errorf("unexpected ACK from server: %q", line)
	}
	return nil
}

// sendBatch envía un conjunto de apuestas usando el mismo socket.
// Cada apuesta viaja como una línea según el protocolo definido y se espera un ACK único.
func sendBatch(conn net.Conn, bets []Bet) error {
	// 1) cabecera
	header := fmt.Sprintf("BATCH|%d\n", len(bets))
	if err := writeAll(conn, []byte(header)); err != nil {
		return err
	}

	// 2) apuestas
	for _, b := range bets {
		line := encodeBet(b)
		if err := writeAll(conn, []byte(line)); err != nil {
			return err
		}
	}

	// 3) ACK único
	ack, err := readLine(conn)
	if err != nil {
		return err
	}
	if ack != "OK\n" {
		return fmt.Errorf("unexpected batch ACK: %q", ack)
	}

	return nil
}
