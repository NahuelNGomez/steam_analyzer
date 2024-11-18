# gateway/main.py

import os
import signal
import sys
import logging
import configparser
import socket
import threading
from connectionHandler import ConnectionHandler
from common.healthcheck import HealthCheckServer

def load_config(config_file='config.ini'):
    """
    Carga la configuración desde un archivo INI.
    """
    config = configparser.ConfigParser()
    config.read(config_file)
    return config['DEFAULT']

def setup_logging(level, log_file):
    """
    Configura el logging para que registre en un archivo y en la consola.
    """
    numeric_level = getattr(logging, level.upper(), None)
    if not isinstance(numeric_level, int):
        numeric_level = logging.INFO
    logging.basicConfig(
        level=numeric_level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout)
        ]
    )
    logging.info("Logging configurado.")

def start_server(config, shutdown_event, active_connections):
    """
    Inicia el servidor que escucha por conexiones entrantes y maneja cada conexión.
    """
    HealthCheckServer().start_in_thread()
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)  # Permite reutilizar la dirección
    server.bind((config['gateway_IP'], int(config['gateway_PORT'])))
    server.listen(int(config.get('gateway_LISTEN_BACKLOG', 5)))
    
    logging.info(f"Gateway escuchando en {config['gateway_IP']}:{config['gateway_PORT']}")
    amount_of_review_instances = int(os.getenv("AMOUNT_OF_REVIEW_INSTANCE", 1))
    
    server.settimeout(1.0)  # Timeout para permitir verificar el shutdown_event
    
    while not shutdown_event.is_set():
        try:
            logging.debug("Esperando conexión...")
            client_sock, address = server.accept()
            logging.info(f"Conexión aceptada de {address[0]}:{address[1]}")
            handler = ConnectionHandler(client_sock, address, amount_of_review_instances)
            active_connections.append(handler)
        except socket.timeout:
            continue  # Verificar nuevamente el shutdown_event
        except Exception as e:
            logging.error(f"Error al aceptar conexiones: {e}")


def main():
    """
    Función principal que carga la configuración, configura el logging, maneja señales y
    arranca el servidor.
    """
    # Cargar configuración
    config = load_config()
    
    # Configuración de logging
    LOGGING_LEVEL = config.get('LOGGING_LEVEL', 'INFO')
    LOG_FILE = config.get('LOG_FILE', 'gateway.log')
    setup_logging(LOGGING_LEVEL, LOG_FILE)
    
    # Configuración del servidor
    GATEWAY_IP = config.get('gateway_IP', '0.0.0.0')  
    GATEWAY_PORT = config.getint('gateway_PORT', 12345)
    LISTEN_BACKLOG = config.getint('gateway_LISTEN_BACKLOG', 5)
    
    # Evento para señal de cierre
    shutdown_event = threading.Event()
    active_connections = []
    
    # Manejar señales para un cierre graceful
    def handle_signal(signum, frame):
        logging.info(f"Señal {signum} recibida. Procediendo al cierre ordenado.")
        shutdown_event.set()
    
    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)  # Opcional: manejar Ctrl+C también
    
    # Iniciar el servidor en un hilo separado
    server_thread = threading.Thread(target=start_server, args=(config, shutdown_event, active_connections), name="ServerThread")
    server_thread.start()
    
    logging.info("Gateway iniciado.")
    
    try:
        while not shutdown_event.is_set():
            # Mantener el hilo principal activo
            shutdown_event.wait(timeout=1.0)
    except Exception as e:
        logging.error(f"Excepción en el main: {e}")
    finally:
        logging.info("Iniciando cierre ordenado de todas las conexiones.")
        for handler in active_connections:
            handler.shutdown()
        server_thread.join()
        logging.info("Gateway cerrado exitosamente.")

if __name__ == '__main__':
    main()
