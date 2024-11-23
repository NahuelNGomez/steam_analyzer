import socket
import logging
import threading

HEALTH_CHECK_PORT = 7777

class HealthCheckServer:
    def __init__(self, thread_to_check: threading.Thread = None):
        self.thread_to_check = thread_to_check
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
                
                if self.thread_to_check:
                    is_alive = self.thread_to_check.is_alive()
                else:
                    is_alive = True

                logging.error("Thread is not alive. Sending OK=0.")
                client_socket.send(b'1' if is_alive else b'0')
                client_socket.close()
            except Exception as e:
                logging.error(f"Error handling health check request: {e}")

    def start_in_thread(self):
        thread = threading.Thread(target=self.start)
        thread.start()
