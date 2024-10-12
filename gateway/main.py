# gateway/main.py

import os
from queue import Queue
import socket
import logging
import configparser
from common.middleware import Middleware
from connectionHandler import ConnectionHandler, shutdown_event
import signal
import threading

def load_config(config_file='config.ini'):
    config = configparser.ConfigParser()
    config.read(config_file)
    return config['DEFAULT']

def handle_sigterm(signum, frame):
    logging.info("SIGTERM recibido en Gateway. Iniciando cierre ordenado...")
    shutdown_event.set()

def start_server(config):
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)  # Permitir reutilización del puerto   
    server.bind(("0.0.0.0", int(config['gateway_PORT'])))
    server.listen(int(config.get('gateway_LISTEN_BACKLOG', 5)))

    logging.info(f"Gateway escuchando en {config['gateway_IP']}:{config['gateway_PORT']}")
    amount_of_review_instances = int(os.getenv("AMOUNT_OF_REVIEW_INSTANCE", 1))

    connection_handlers = []

    # Establecer un timeout para poder verificar el estado de shutdown_event
    server.settimeout(1.0)  # Timeout de 1 segundo

    try:
        while not shutdown_event.is_set():
            try:
                #print("Esperando conexión...", flush=True)
                client_sock, address = server.accept()
                logging.info(f"Conexión aceptada de {address[0]}:{address[1]}")
                handler = ConnectionHandler(client_sock, address, amount_of_review_instances)
                connection_handlers.append(handler)
            except socket.timeout:
                continue  # Volver a verificar el shutdown_event
            except Exception as e:
                logging.error(f"Error al aceptar conexión: {e}")
    except KeyboardInterrupt:
        logging.info("Interrupción por teclado recibida. Cerrando servidor...")
        shutdown_event.set()
    finally:
        logging.info("Cerrando servidor...")
        server.close()
        # Cerrar todas las conexiones activas
        for handler in connection_handlers:
            handler.shutdown()
        logging.info("Servidor cerrado correctamente.")

def main():
    config = load_config()
    logging.basicConfig(level=getattr(logging, config.get('LOGGING_LEVEL', 'INFO').upper()),
                        format='%(asctime)s - %(levelname)s - %(message)s')
    logging.info("Gateway iniciado.")

    # Registro del manejador de señales
    signal.signal(signal.SIGTERM, handle_sigterm)

    # Iniciar el servidor
    start_server(config)

if __name__ == '__main__':
    main()
