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
        raw = _recv_line(client_sock)
        parts = raw.split("|")
        if len(parts) != 6:
            raise ValueError("invalid bet format")

        agency, first_name, last_name, document, birthdate, number = parts

        bet = Bet(agency, first_name, last_name, document, birthdate, number)
        store_bets([bet])

        logging.info(
            f"action: apuesta_almacenada | result: success | dni: {document} | numero: {number}"
        )

        _send_all(client_sock, b"OK\n")
    finally:
        logging.info("action: close_client | result: success")
        client_sock.close()
