# client/client.py

import time
import socket
import configparser
import logging
from protocol import Protocol

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
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

def send_data(protocol, file_path, data_type):
    try:
        with open(file_path, "r", encoding='utf-8') as file:
            data = file.read()
            # Crear el mensaje con tipo y datos separados por doble salto de línea
            message = f"{data_type}\n\n{data}"
            protocol.send_message(message)
            logging.debug(f"Enviado ({data_type}): {data[:50]}...")  # Mostrar solo los primeros 50 caracteres
            # Esperar y recibir la respuesta del servidor
            response = protocol.receive_message()
            if response:
                logging.debug(f"Recibido: {response}")
                print(f"Respuesta del servidor: {response}")
            else:
                logging.warning("No se recibió respuesta del servidor.")
            time.sleep(1)
    except FileNotFoundError:
        logging.error(f"Archivo no encontrado: {file_path}")
    except Exception as e:
        logging.error(f"Error al enviar datos: {e}")

def start_client(boundary_ip, boundary_port, retries=5, delay=5):
    attempt = 0
    while attempt < retries:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(10)  # Establecer timeout para recv
                s.connect((boundary_ip, int(boundary_port)))
                protocol = Protocol(s)
                logging.info(f"Conectado al servidor en {boundary_ip}:{boundary_port}")

                # Enviar datasets una vez
                send_data(protocol, "data/samplegames.csv", "games")
                send_data(protocol, "data/samplereviews.csv", "reviews")
                
                # Salir del bucle después de enviar los datos
                logging.info("Datos enviados. Cerrando conexión.")
                break  # Terminar después de enviar los datos
        except socket.gaierror:
            attempt += 1
            logging.error(f"Error al resolver el hostname {boundary_ip}. Intento {attempt} de {retries}")
            time.sleep(delay)
        except ConnectionRefusedError:
            attempt += 1
            logging.error(f"Conexión rechazada por el servidor en {boundary_ip}:{boundary_port}. Intento {attempt} de {retries}")
            time.sleep(delay)
        except socket.timeout:
            attempt +=1
            logging.error(f"Tiempo de espera agotado al conectar al servidor en {boundary_ip}:{boundary_port}. Intento {attempt} de {retries}")
            time.sleep(delay)
        except Exception as e:
            attempt +=1
            logging.error(f"Error en el cliente: {e}. Intento {attempt} de {retries}")
            time.sleep(delay)
    logging.critical(f"No se pudo conectar al servidor en {boundary_ip}:{boundary_port} después de {retries} intentos.")

if __name__ == "__main__":
    config = load_config()
    setup_logging(config.get('LOGGING_LEVEL', 'INFO'))
    BOUNDARY_IP = config.get('BOUNDARY_IP', 'gateway')  
    BOUNDARY_PORT = config.get('BOUNDARY_PORT', 12345)
    start_client(BOUNDARY_IP, BOUNDARY_PORT)
