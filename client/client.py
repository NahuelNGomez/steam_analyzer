import time
import socket
import configparser
import logging

def load_config(config_file='config.ini'):
    config = configparser.ConfigParser()
    config.read(config_file)
    return config['DEFAULT']

def setup_logging(level):
    numeric_level = getattr(logging, level.upper(), None)
    if not isinstance(numeric_level, int):
        numeric_level = logging.INFO
    logging.basicConfig(level=numeric_level,
                        format='%(asctime)s - %(levelname)s - %(message)s')
def start_client(boundary_ip, boundary_port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.connect((boundary_ip, int(boundary_port)))
            logging.info(f"Conectado al servidor en {boundary_ip}:{boundary_port}")
            
            message = "Mensaje automático desde el cliente"
            while True:
                logging.debug(f"Enviado: {message}")
                s.sendall(message.encode())
                
                data = s.recv(1024)
                logging.debug(f"Recibido: {data.decode()}")
                print(f"Respuesta del servidor: {data.decode()}")
                time.sleep(1)
            
        except ConnectionRefusedError:
            logging.error(f"No se pudo conectar al servidor en {boundary_ip}:{boundary_port}")

if __name__ == "__main__":
    config = load_config()
    setup_logging(config.get('LOGGING_LEVEL', 'INFO'))
    BOUNDARY_IP = config.get('BOUNDARY_IP', 'localhost')
    BOUNDARY_PORT = config.get('BOUNDARY_PORT', 12345)
    start_client(BOUNDARY_IP, BOUNDARY_PORT)
