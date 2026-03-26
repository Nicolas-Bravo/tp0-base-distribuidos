package common

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"net"
	"strconv"
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
	if sep1 == -1 {
		return "", nil, fmt.Errorf("invalid frame format: missing type separator")
	}

	typeStr := string(buf[:sep1])
	// Para WINNERS_RESP usamos tipo|cantidad|payload.
	if typeStr == "WINNERS_RESP" {
		if sep2 == -1 {
			return typeStr, nil, fmt.Errorf("invalid WINNERS_RESP frame: missing count separator")
		}
		return typeStr, buf[sep1+1:], nil
	}

	if sep2 == -1 {
		return "", nil, fmt.Errorf("invalid frame format")
	}
	payload := buf[sep2+1:]
	return typeStr, payload, nil
}

// sendBatch envía todas las apuestas de un batch en un solo mensaje tipo BATCH y espera un ACK tipo BATCH_ACK.
func sendBatch(conn net.Conn, bets []Bet) error {
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
// SendEnd notifica al servidor que una agencia terminó de enviar sus apuestas y espera el ACK.
func SendEnd(conn net.Conn, agencyID string) error {
	if err := sendFrame(conn, "END", []byte(agencyID)); err != nil {
		return err
	}

	// Consumir el END_ACK para mantener la sincronización de la conexión
	msgType, _, err := readFrame(conn)
	if err != nil {
		return err
	}

	if msgType != "END_ACK" {
		return fmt.Errorf("unexpected end ACK type: %q", msgType)
	}

	return nil
}

// SendWinnersRequest consulta la cantidad de ganadores para una agencia.
// Protocolo del servidor:
// - Si el sorteo aún no se realizó: WINNERS_RESP_WAIT (sin payload) => retry = false
// - Si hubo error: WINNERS_RESP_ERROR (sin payload) => error
// - Si el sorteo ya se realizó:
//   - Si hay ganadores: WINNERS_RESP|{cantidad}|{dni1,dni2,...}
//   - Si no hay ganadores: WINNERS_RESP|0|
//     En ambos casos (con o sin ganadores) retry = true, para que el cliente tome la respuesta como definitiva.
func SendWinnersRequest(conn net.Conn, agencyID string) (int, bool, error) {
	if err := sendFrame(conn, "WINNERS_REQ", []byte(agencyID)); err != nil {
		return 0, false, err
	}

	msgType, rest, err := readFrame(conn)
	if err != nil {
		return 0, false, err
	}

	switch msgType {
	case "WINNERS_RESP_WAIT":
		return 0, false, nil
	case "WINNERS_RESP_ERROR":
		return 0, true, fmt.Errorf("server reported error on winners request")
	case "WINNERS_RESP":
		parts := strings.SplitN(string(rest), "|", 3)
		if len(parts) < 2 {
			return 0, true, fmt.Errorf("invalid WINNERS_RESP frame")
		}
		count, err := strconv.Atoi(parts[1])
		if err != nil {
			return 0, true, fmt.Errorf("invalid winners count: %v", err)
		}
		if count == 0 {
			return 0, true, nil
		}
		if len(parts) < 3 || parts[2] == "" {
			return 0, true, fmt.Errorf("winners payload missing with non-zero count")
		}
		dnis := strings.Split(parts[2], ",")
		return len(dnis), true, nil
	default:
		return 0, false, fmt.Errorf("unexpected response type: %s", msgType)
	}
}
