import configparser
import logging
import os
import signal
import sys
from src.client import Client

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
    
    # Configuración de logging
    LOGGING_LEVEL = os.getenv("LOGGING_LEVEL") or "INFO"
    setup_logging(LOGGING_LEVEL)
    
    # Configuración del cliente
    BOUNDARY_IP = os.getenv("BOUNDARY_IP") or '127.0.0.1'
    BOUNDARY_PORT = int(os.getenv("BOUNDARY_PORT") or 8000)
    RETRIES = int(os.getenv("RETRIES") or 5)
    DELAY = int(os.getenv("DELAY") or 5)
    CLIENT_ID = int(os.getenv("CLIENT_ID") or 1)
    GAME_FILE = os.getenv("GAME_FILE") or "sample_1_por_ciento_games.csv"
    REVIEW_FILE = os.getenv("REVIEW_FILE") or "sample_1_por_ciento_review.csv"
    
    client = Client(BOUNDARY_IP, BOUNDARY_PORT, retries=RETRIES, delay=DELAY,client_id=CLIENT_ID, game_file=GAME_FILE, review_file=REVIEW_FILE)
    
    # Manejar señales para un cierre graceful
    def handle_signal(signum, frame):
        logging.info(f"Señal {signum} recibida. Procediendo al cierre ordenado.")
        client.shutdown()
        sys.exit(0)
    
    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)
    
    try:
        client.start()
    except Exception as e:
        logging.error(f"Excepción no manejada en main: {e}")
        client.shutdown()
        sys.exit(1)

if __name__ == "__main__":
    main()
