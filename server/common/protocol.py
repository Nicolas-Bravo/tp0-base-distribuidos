import logging
import struct

from .utils import Bet, store_bets, load_bets, has_won

_end_notifications = set()
_bets_cache = None
_active_agencies = set()


# Lee exactamente 'length' bytes del socket o lanza una excepción si la conexión se cierra antes.
def _recv_all(sock, length: int) -> bytes:
    data = bytearray()
    while len(data) < length:
        chunk = sock.recv(length - len(data))
        if not chunk:
            raise ValueError("connection closed while reading frame")
        data.extend(chunk)
    return bytes(data)


# Lee un frame completo desde el socket.
# El formato del frame es: [uint32 big endian con largo] + b"{tipo}|{tamaño}|{mensaje}".
# Devuelve una tupla (msg_type, payload) donde payload son los bytes del mensaje.
def _read_frame(sock):
    len_prefix = _recv_all(sock, 4)
    (frame_len,) = struct.unpack("!I", len_prefix)
    frame = _recv_all(sock, frame_len)

    # Ubiar los separadores '|' que delimitan tipo y tamaño
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


# Envía un frame completo al socket con el formato: [uint32 largo] + "tipo|tamaño|mensaje".
def _send_frame(sock, msg_type: str, payload: bytes) -> None:
    header = f"{msg_type}|{len(payload)}|".encode("utf-8")
    frame = header + payload
    len_prefix = struct.pack("!I", len(frame))
    sock.sendall(len_prefix + frame)


# Procesa un mensaje de tipo BATCH, que contiene varias apuestas serializadas en texto.
# Cada línea del payload representa una apuesta con el formato:
#   agency|nombre|apellido|dni|nacimiento|numero
# Todas las apuestas se almacenan con store_bets y se responde con un BATCH_ACK.
def _handle_batch(payload, client_sock):
    lines = [l for l in payload.decode("utf-8").split("\n") if l]

    bets = []
    for idx, raw in enumerate(lines):
        fields = raw.split("|")
        if len(fields) != 6:
            raise ValueError("invalid bet format in batch")
        agency, first_name, last_name, document, birthdate, number = fields
        bets.append(Bet(agency, first_name, last_name, document, birthdate, number))
        _active_agencies.add(agency)

    store_bets(bets)
    logging.info(
        f"action: apuesta_recibida | result: success | cantidad: {len(bets)}"
    )

    _send_frame(client_sock, "BATCH_ACK", b"")


# Procesa un mensaje de tipo END, que indica el fin de envío de apuestas por parte de una agencia.
# Si todas las agencias activas han enviado sus apuestas, se realiza el sorteo.
def _handle_end(payload: bytes, sock):
    global _bets_cache
    agency_id = payload.decode("utf-8").strip()
    _end_notifications.add(agency_id)
    logging.info(f"action: fin_envio | result: success | agency: {agency_id} | total_agencias: {len(_end_notifications)}")
    if _bets_cache is None and _active_agencies and _end_notifications.issuperset(_active_agencies):
        # “sorteo”
        bets = list(load_bets())
        _bets_cache = bets
        logging.info("action: sorteo | result: success")
    _send_frame(sock, "END_ACK", b"")


# Procesa un mensaje de tipo WINNERS_REQ, que solicita los ganadores de una agencia específica.
# Responde con los documentos de los ganadores separados por comas.
def _handle_winners_req(payload: bytes, sock):
    global _bets_cache
    agency_id = int(payload.decode("utf-8").strip())

    if _bets_cache is None:
        # Antes del sorteo no se pueden responder consultas
        _send_frame(sock, "WINNERS_RESP", b"")
        return

    winners = []
    for bet in _bets_cache:
        if bet.agency == agency_id and has_won(bet):
            winners.append(str(bet.document))

    resp_payload = ",".join(winners).encode("utf-8")
    _send_frame(sock, "WINNERS_RESP", resp_payload)


# Manejo de mensajes de apuesta en batch
# Procesa conexiones de clientes y delega el manejo según el tipo de mensaje recibido.
def handle_bet_connection(client_sock) -> None:
    try:
        msg_type, payload = _read_frame(client_sock)
        if msg_type == "BATCH":
            _handle_batch(payload, client_sock)
        elif msg_type == "END":
            _handle_end(payload, client_sock)
        elif msg_type == "WINNERS_REQ":
            _handle_winners_req(payload, client_sock)
        else:
            raise ValueError(f"unexpected message type {msg_type}")

    except Exception as e:
        # Ante cualquier error en el procesamiento del mensaje se loguea el fallo
        # y se envía un BATCH_ACK con un payload de error simple.
        logging.error(f"action: apuesta_recibida | result: fail | error: {e}")
        try:
            _send_frame(client_sock, "BATCH_ACK", b"ERR")
        except Exception:
            pass
    finally:
        logging.info("action: close_client | result: success")
        client_sock.close()
