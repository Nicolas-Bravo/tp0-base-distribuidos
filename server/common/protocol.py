import logging

from .utils import Bet, store_bets

# Lee una línea completa terminada en \n, manejando short-read
def _recv_line(sock) -> str:
    chunks = []
    while True:
        data = sock.recv(1024)
        if not data:
            break
        chunks.append(data)
        if b"\n" in data:
            break
    if not chunks:
        raise ValueError("empty message from client")
    raw = b"".join(chunks).decode("utf-8").rstrip("\r\n")
    return raw


# Envía todos los bytes de data, evitando short-write
def _send_all(sock, data: bytes) -> None:
    total_sent = 0
    while total_sent < len(data):
        sent = sock.send(data[total_sent:])
        if sent == 0:
            raise RuntimeError("socket connection broken")
        total_sent += sent


# Recibe una apuesta, la almacena y responde un ACK simple
def handle_bet_connection(client_sock) -> None:
    try:
        header = _recv_line(client_sock)
        parts = header.split("|")
        if len(parts) != 2 or parts[0] != "BATCH":
            raise ValueError("invalid batch header")
        try:
            count = int(parts[1])
        except ValueError:
            raise ValueError("invalid batch size")

        bets = []
        for _ in range(count):
            raw = _recv_line(client_sock)
            fields = raw.split("|")
            if len(fields) != 6:
                raise ValueError("invalid bet format in batch")

            agency, first_name, last_name, document, birthdate, number = fields
            bets.append(Bet(agency, first_name, last_name, document, birthdate, number))

        store_bets(bets)

        # log con cantidad de apuestas del batch
        logging.info(
            f"action: apuesta_recibida | result: success | cantidad: {len(bets)}"
        )

        _send_all(client_sock, b"OK\n")

        # logs individuales de apuesta_almacenada
        # for bet in bets:
        #    logging.info(
        #        f"action: apuesta_almacenada | result: success | dni: {bet.document} | numero: {bet.number}"
        #    )

    except Exception as e:
        logging.error(f"action: apuesta_recibida | result: fail | error: {e}")
        try:
            _send_all(client_sock, b"ERR\n")
        except Exception:
            pass
    finally:
        logging.info("action: close_client | result: success")
        client_sock.close()
