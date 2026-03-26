import logging
import struct
import threading

from .utils import Bet, store_bets, load_bets, has_won

_end_notifications = set()
_bets_cache = None
_active_agencies = set()

# Lock para proteger el acceso a las variables globales compartidas
_state_lock = threading.Lock()

# Lee exactamente 'length' bytes del socket.
# Lanza EOFError si la conexión se cierra limpiamente al inicio de la lectura.
# Lanza ValueError si la conexión se corta a mitad de un frame.
def _recv_all(sock, length: int) -> bytes:
    data = bytearray()
    while len(data) < length:
        chunk = sock.recv(length - len(data))
        if not chunk:
            if len(data) == 0:
                raise EOFError("Client closed connection gracefully")
            raise ValueError("connection closed while reading frame")
        data.extend(chunk)
    return bytes(data)


# Lee un frame completo desde el socket.
# El formato del frame es: [uint32 big endian con largo] + b"{tipo}|{tamaño}|{mensaje}".
# Devuelve una tupla (msg_type, payload) donde payload son los bytes del mensaje.
def _read_frame(sock):
    # Esto bloqueará hasta que llegue un mensaje o el socket se cierre
    len_prefix = _recv_all(sock, 4)
    (frame_len,) = struct.unpack("!I", len_prefix)
    frame = _recv_all(sock, frame_len)

    # Ubicar los separadores '|' que delimitan tipo y tamaño
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
def _handle_batch(payload, client_sock):
    lines = [l for l in payload.decode("utf-8").split("\n") if l]

    if not lines:
        _send_frame(client_sock, "BATCH_ACK", b"")
        return

    # Extraemos el ID de la agencia de la primera apuesta del batch
    first_row_fields = lines[0].split("|")
    if len(first_row_fields) != 6:
        raise ValueError("invalid bet format in batch")
    
    agency_id = int(first_row_fields[0])
    bets = []
    
    for raw in lines:
        fields = raw.split("|")
        if len(fields) != 6:
            raise ValueError("invalid bet format in batch")
        agency, first_name, last_name, document, birthdate, number = fields
        bets.append(Bet(agency, first_name, last_name, document, birthdate, number))

    # store_bets es thread-safe según la cátedra
    store_bets(bets)
    logging.info(
        f"action: apuesta_recibida | result: success | cantidad: {len(bets)}"
    )

    # Actualizamos el estado global usando el lock solo con la agencia identificada
    with _state_lock:
        _active_agencies.add(agency_id)

    _send_frame(client_sock, "BATCH_ACK", b"")


# Procesa un mensaje de tipo END, que indica el fin de envío de apuestas por parte de una agencia.
def _handle_end(payload: bytes, sock):
    global _bets_cache
    agency_id = int(payload.decode("utf-8").strip())
    
    with _state_lock:
        _end_notifications.add(agency_id)
        logging.info(
            f"action: fin_envio | result: success | agency: {agency_id} | total_agencias: {len(_end_notifications)}"
        )

        # Si todas las agencias reportaron END y aún no se sorteó, cargamos las apuestas
        if _bets_cache is None and _active_agencies and _end_notifications.issuperset(_active_agencies):
            bets = list(load_bets())
            _bets_cache = bets
            logging.info("action: sorteo | result: success")

    _send_frame(sock, "END_ACK", b"")


# Procesa un mensaje de tipo WINNERS_REQ, que solicita los ganadores de una agencia específica.
def _handle_winners_req(payload: bytes, sock):
    global _bets_cache
    agency_id = int(payload.decode("utf-8").strip())

    # Leemos la caché protegida por el lock para saber si ya se hizo el sorteo
    with _state_lock:
        cache_ready = _bets_cache is not None
        if cache_ready:
            # Una vez que _bets_cache está poblada, es solo de lectura, 
            # podemos referenciarla localmente para procesarla fuera del lock si fuera necesario.
            local_bets_cache = _bets_cache

    # Sorteo aún no realizado
    if not cache_ready:
        _send_frame(sock, "WINNERS_RESP_WAIT", b"")
        return

    try:
        winners = []
        for bet in local_bets_cache:
            if bet.agency == agency_id:
                if has_won(bet):
                    winners.append(str(bet.document))

        count = len(winners)
        if count > 0:
            payload_str = ",".join(winners)
            resp_payload = payload_str.encode("utf-8")
            _send_frame(sock, f"WINNERS_RESP|{count}", resp_payload)
        else:
            # Sin ganadores para esta agencia, se indica cantidad 0 y payload vacío
            _send_frame(sock, "WINNERS_RESP|0", b"")
    except Exception as e:
        logging.error(f"action: winners | result: fail | error: {e}")
        _send_frame(sock, "WINNERS_RESP_ERROR", b"")


# Manejo de mensajes de forma continua en la misma conexión
def handle_bet_connection(client_sock) -> None:
    try:
        # Bucle que procesa múltiples mensajes en el mismo socket
        while True:
            msg_type, payload = _read_frame(client_sock)
            
            if msg_type == "BATCH":
                _handle_batch(payload, client_sock)
            elif msg_type == "END":
                _handle_end(payload, client_sock)
            elif msg_type == "WINNERS_REQ":
                _handle_winners_req(payload, client_sock)
            else:
                raise ValueError(f"unexpected message type {msg_type}")

    except EOFError:
        # El cliente finalizó su flujo y cerró la conexión ordenadamente.
        logging.info("action: client_finished | result: success")
    except Exception as e:
        logging.error(f"action: handle_client_loop | result: fail | error: {e}")
        try:
            _send_frame(client_sock, "WINNERS_RESP_ERROR", b"")
        except Exception:
            pass
    finally:
        logging.info("action: close_client | result: success")
        client_sock.close()