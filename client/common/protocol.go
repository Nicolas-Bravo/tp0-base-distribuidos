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

// sendBet envía una apuesta por el socket asegurando escribir toda la línea.
func sendBet(conn net.Conn, b Bet) error {
	payload := encodeBet(b)
	written := 0
	data := []byte(payload)
	for written < len(data) {
		n, err := conn.Write(data[written:])
		if err != nil {
			return err
		}
		written += n
	}
	return nil
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
