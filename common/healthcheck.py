import socket
import logging
import threading

class HealthCheckServer:
    HEALTH_CHECK_PORT = 7777

    def __init__(self):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.bind(('0.0.0.0', self.HEALTH_CHECK_PORT))
        self.socket.listen(1)

    def start(self):
        while True:
            try:
                client_socket, addr = self.socket.accept()
                logging.info(f"Health check request from {addr}. Responding with OK=1.")
                client_socket.recv(1)
                client_socket.send(b'1')
                client_socket.close()
            except Exception as e:
                logging.error(f"Error handling health check request: {e}")

    def start_in_thread(self):
        thread = threading.Thread(target=self.start)
        thread.start()
        logging.info(f"Health check server started on port {self.HEALTH_CHECK_PORT}")
