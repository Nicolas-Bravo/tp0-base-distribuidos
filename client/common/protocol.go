package common

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"net"
	"strings"
)

// encodeBet serializa una apuesta en una sola línea de texto simple.
func encodeBet(b Bet) string {
	return fmt.Sprintf("%s|%s|%s|%s|%s|%s",
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

// sendFrame envía un mensaje con el formato tipo|largo|payload, precedido por un uint32 big endian con el largo total del frame.
func sendFrame(conn net.Conn, msgType string, payload []byte) error {
	header := fmt.Sprintf("%s|%d|", msgType, len(payload))
	frame := append([]byte(header), payload...)
	// Prefijo de longitud del frame completo
	var lenPrefix [4]byte
	binary.BigEndian.PutUint32(lenPrefix[:], uint32(len(frame)))
	if err := writeAll(conn, lenPrefix[:]); err != nil {
		return err
	}
	return writeAll(conn, frame)
}

// readFrame lee un frame completo usando un lector bufferizado, compartido por conexión.
func readFrame(conn net.Conn) (string, []byte, error) {
	reader := bufio.NewReader(conn)

	var lenPrefix [4]byte
	if _, err := reader.Read(lenPrefix[:]); err != nil {
		return "", nil, err
	}
	frameLen := binary.BigEndian.Uint32(lenPrefix[:])
	buf := make([]byte, frameLen)
	if _, err := reader.Read(buf); err != nil {
		return "", nil, err
	}

	// buf = tipo|largo|payload
	sep1 := -1
	sep2 := -1
	for i, b := range buf {
		if b == '|' {
			if sep1 == -1 {
				sep1 = i
			} else {
				sep2 = i
				break
			}
		}
	}
	if sep1 == -1 || sep2 == -1 {
		return "", nil, fmt.Errorf("invalid frame format")
	}
	typeStr := string(buf[:sep1])
	// lengthStr := string(buf[sep1+1 : sep2]) // se puede validar si se quiere
	payload := buf[sep2+1:]
	return typeStr, payload, nil
}

// sendBatch envía todas las apuestas de un batch en un solo mensaje tipo BATCH y espera un ACK tipo BATCH_ACK.
func sendBatch(conn net.Conn, bets []Bet) error {
	// payload del batch: una apuesta por línea
	builder := make([]byte, 0)
	for idx, b := range bets {
		line := encodeBet(b)
		builder = append(builder, []byte(line)...)
		if idx < len(bets)-1 {
			builder = append(builder, '\n')
		}
	}

	if err := sendFrame(conn, "BATCH", builder); err != nil {
		return err
	}

	typeStr, _, err := readFrame(conn)
	if err != nil {
		return err
	}
	if typeStr != "BATCH_ACK" {
		return fmt.Errorf("unexpected batch ACK type: %q", typeStr)
	}
	return nil
}

// SendEnd notifica al servidor que una agencia terminó de enviar sus apuestas.
func SendEnd(conn net.Conn, agencyID string) error {
	return sendFrame(conn, "END", []byte(agencyID))
}

// SendWinnersRequest consulta la cantidad de ganadores para una agencia.
// Si el servidor aún no realizó el sorteo, se puede recibir una respuesta vacía.
// En ese caso, el cliente interpretará que todavía no hay datos y podrá reintentar.
func SendWinnersRequest(conn net.Conn, agencyID string) (int, bool, error) {
	if err := sendFrame(conn, "WINNERS_REQ", []byte(agencyID)); err != nil {
		return 0, false, err
	}
	msgType, payload, err := readFrame(conn)
	if err != nil {
		return 0, false, err
	}
	if msgType != "WINNERS_RESP" {
		return 0, false, fmt.Errorf("unexpected response type: %s", msgType)
	}

	// Payload vacío: el servidor aún no puede responder (sorteo no realizado)
	if len(payload) == 0 {
		return 0, false, nil
	}

	resp := string(payload)
	if resp == "ERR" {
		return 0, true, fmt.Errorf("server reported error on winners request")
	}

	parts := strings.Split(resp, ",")
	if len(parts) == 1 && parts[0] == "" {
		return 0, true, nil
	}
	return len(parts), true, nil
}
