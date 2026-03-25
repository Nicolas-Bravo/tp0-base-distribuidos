import logging
import struct

from .utils import Bet, store_bets


def _recv_all(sock, length: int) -> bytes:
    """Lee exactamente length bytes del socket o lanza excepción."""
    data = bytearray()
    while len(data) < length:
        chunk = sock.recv(length - len(data))
        if not chunk:
            raise ValueError("connection closed while reading frame")
        data.extend(chunk)
    return bytes(data)


def _read_frame(sock):
    """Lee un frame: uint32 big endian (largo) + contenido tipo|largo|payload."""
    len_prefix = _recv_all(sock, 4)
    (frame_len,) = struct.unpack("!I", len_prefix)
    frame = _recv_all(sock, frame_len)

    # Separar tipo y payload
    try:
        first_sep = frame.index(b"|")
        second_sep = frame.index(b"|", first_sep + 1)
    except ValueError:
        raise ValueError("invalid frame: missing separators")

    msg_type = frame[:first_sep].decode("utf-8")
    length_str = frame[first_sep + 1 : second_sep].decode("utf-8")
    try:
        payload_len = int(length_str)
    except ValueError:
        raise ValueError("invalid payload length in frame")

    payload = frame[second_sep + 1 :]
    if len(payload) != payload_len:
        raise ValueError("payload length mismatch")

    return msg_type, payload


def _send_frame(sock, msg_type: str, payload: bytes) -> None:
    header = f"{msg_type}|{len(payload)}|".encode("utf-8")
    frame = header + payload
    len_prefix = struct.pack("!I", len(frame))
    sock.sendall(len_prefix + frame)


# Manejo de mensajes de apuesta en batch
def handle_bet_connection(client_sock) -> None:
    try:
        msg_type, payload = _read_frame(client_sock)
        if msg_type != "BATCH":
            raise ValueError(f"unexpected message type {msg_type}")

        # payload: apuestas separadas por \n
        lines = [l for l in payload.decode("utf-8").split("\n") if l]

        bets = []
        for idx, raw in enumerate(lines):
            fields = raw.split("|")
            if len(fields) != 6:
                raise ValueError("invalid bet format in batch")
            agency, first_name, last_name, document, birthdate, number = fields
            bets.append(Bet(agency, first_name, last_name, document, birthdate, number))

        store_bets(bets)
        logging.info(
            f"action: apuesta_recibida | result: success | cantidad: {len(bets)}"
        )

        _send_frame(client_sock, "BATCH_ACK", b"")

    except Exception as e:
        logging.error(f"action: apuesta_recibida | result: fail | error: {e}")
        try:
            _send_frame(client_sock, "BATCH_ACK", b"ERR")
        except Exception:
            pass
    finally:
        logging.info("action: close_client | result: success")
        client_sock.close()
