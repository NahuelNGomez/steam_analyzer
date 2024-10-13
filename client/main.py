import configparser
import logging
import signal
import sys
from client import Client

def load_config(config_file='config.ini'):
    config = configparser.ConfigParser()
    config.read(config_file)
    return config['DEFAULT']

def setup_logging(level):
    numeric_level = getattr(logging, level.upper(), None)
    if not isinstance(numeric_level, int):
        numeric_level = logging.INFO
    logging.basicConfig(
        level=numeric_level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )
    logging.info("Logging configurado.")

def main():
    # Cargar configuración
    config = load_config()
    
    # Configuración de logging
    LOGGING_LEVEL = config.get('LOGGING_LEVEL', 'INFO')
    setup_logging(LOGGING_LEVEL)
    
    # Configuración del cliente
    BOUNDARY_IP = config.get('BOUNDARY_IP', '127.0.0.1')  
    BOUNDARY_PORT = config.get('BOUNDARY_PORT', 8000)
    RETRIES = config.getint('RETRIES', 5)
    DELAY = config.getint('DELAY', 5)
    
    client = Client(BOUNDARY_IP, BOUNDARY_PORT, retries=RETRIES, delay=DELAY)
    
    # Manejar señales para un cierre graceful
    def handle_signal(signum, frame):
        logging.info(f"Señal {signum} recibida. Procediendo al cierre ordenado.")
        client.shutdown()
        sys.exit(0)
    
    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)  # Opcional: manejar Ctrl+C también
    
    try:
        client.start()
    except Exception as e:
        logging.error(f"Excepción no manejada en main: {e}")
        client.shutdown()
        sys.exit(1)

if __name__ == "__main__":
    main()
