import socket
import logging
import signal
import threading
from .protocol import handle_bet_connection


class Server:
    def __init__(self, port, listen_backlog):
        self._shutdown = threading.Event()
        # Initialize server socket
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(('', port))
        self._server_socket.listen(listen_backlog)
        
        # Lista para mantener el control de los hilos de los clientes
        self._client_threads = []
        
        signal.signal(signal.SIGTERM, self._handle_signal)

    def _handle_signal(self, signum, frame):
        logging.info(f'action: shutdown | result: in_progress | signal: {signum}')
        self._shutdown.set()
        try:
            self._server_socket.close()
        except OSError:
            pass

    def run(self):
        """
        Server loop

        Server that accept a new connections and establishes a
        communication with a client using multithreading.
        """

        while not self._shutdown.is_set():
            try:
                client_sock = self.__accept_new_connection()
            except OSError:
                # Socket cerrado durante el apagado
                break
            
            # Limpiar hilos muertos periódicamente para no acumular referencias
            self._client_threads = [t for t in self._client_threads if t.is_alive()]

            # Lanzar un nuevo hilo para manejar la conexión del cliente
            client_thread = threading.Thread(
                target=self.__handle_client_connection,
                args=(client_sock,)
            )
            client_thread.start()
            self._client_threads.append(client_thread)

        # Esperar a que todos los hilos en curso finalicen antes de apagar completamente
        for t in self._client_threads:
            t.join()

        logging.info('action: shutdown | result: success')

    def __handle_client_connection(self, client_sock):
        """Delegar manejo de la conexión al módulo de protocolo."""
        try:
            handle_bet_connection(client_sock)
        except Exception as e:
            logging.error(f'action: receive_message | result: fail | error: {e}')

    def __accept_new_connection(self):
        """
        Accept new connections

        Function blocks until a connection to a client is made.
        Then connection created is printed and returned
        """

        # Connection arrived
        logging.info('action: accept_connections | result: in_progress')
        c, addr = self._server_socket.accept()
        logging.info(f'action: accept_connections | result: success | ip: {addr[0]}')
        return c