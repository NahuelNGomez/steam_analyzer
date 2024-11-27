import socket
import logging
import threading

HEALTH_CHECK_PORT = 7777

class HealthCheckServer:
    def __init__(self, threads_to_check: list[threading.Thread]):
        self.threads_to_check = threads_to_check
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.bind(('0.0.0.0', HEALTH_CHECK_PORT))
        self.socket.listen(1)

    def start(self):
        logging.info(f"Health check server started on port {HEALTH_CHECK_PORT}")
        while True:
            try:
                client_socket, addr = self.socket.accept()
                logging.info(f"Health check request from {addr}.")
                client_socket.recv(1)

                are_alive = [t.is_alive() for t in self.threads_to_check]
                print(are_alive)
                if all(are_alive):
                    client_socket.send(b'1')
                else:
                    logging.error("Thread is not alive. Sending OK=0.")
                    client_socket.send(b'0')

                client_socket.close()
            except Exception as e:
                logging.error(f"Error handling health check request: {e}")

    def start_in_thread(self):
        thread = threading.Thread(target=self.start)
        thread.start()
