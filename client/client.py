import time
import socket
import logging
import json
from common.protocol import Protocol
from common.constants import MAX_BATCH_SIZE
import threading

class Client:
    def __init__(self, boundary_ip, boundary_port, retries=5, delay=5):
        self.boundary_ip = boundary_ip
        self.boundary_port = boundary_port
        self.retries = retries
        self.delay = delay
        self.responses = []  
        self.lock = threading.Lock()  
        
    def send_data(self, protocol, file_path, data_type):
        try:
            with open(file_path, "r", encoding="utf-8") as file:
                batch = []
                first = True
                protocol.send_message(data_type)
                for line in file:
                    if first:
                        first = False
                        message = f"{data_type}\n\n{line}"
                        protocol.send_message(message)
                        continue
                    batch.append(line)
                    if len(batch) == MAX_BATCH_SIZE:  # Si alcanzamos el tamaño del batch
                        data = ''.join(batch)
                        message = f"{data_type}\n\n{data}"
                        protocol.send_message(message)
                        logging.debug(f"Enviado ({data_type}): {data[:50]}...")
                        batch.clear()  # Vaciar el batch para el siguiente conjunto de líneas
                        protocol.receive_message()

                # Enviar cualquier resto de líneas que no formó un batch completo
                if batch:
                    data = ''.join(batch)
                    message = f"{data_type}\n\n{data}"
                    protocol.send_message(message)
                    logging.debug(f"Enviado ({data_type}): {data[:50]}...")
                    protocol.receive_message()

        except FileNotFoundError:
            logging.error(f"Archivo no encontrado: {file_path}")
        except Exception as e:
            logging.error(f"Error al enviar datos: {e}")

    def send_fin(self, protocol):
        try:
            message = "fin\n\n"
            protocol.send_message(message)
            print("Fin de la transmisión de datos", flush=True)
            logging.debug(f"Enviado ({message})")
        except FileNotFoundError:
            logging.error(f"Error al enviar fin")
        except Exception as e:
            logging.error(f"Error al enviar datos: {e}")

    def start(self):
        attempt = 0
        while attempt < self.retries:
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.connect((self.boundary_ip, int(self.boundary_port)))
                    protocol = Protocol(s)
                    logging.info(
                        f"Conectado al servidor en {self.boundary_ip}:{self.boundary_port}"
                    )
                    
                    # Enviar datasets una vez
                    self.send_data(protocol, "data/sample_1_por_ciento_games.csv", "games")
                    self.send_data(protocol, "data/sample_1_por_ciento_review.csv", "reviews")
                    self.send_fin(protocol)

                    # Iniciar un hilo para guardar respuestas periódicamente
                    save_thread = threading.Thread(target=self.periodic_save_responses)
                    save_thread.daemon = True
                    save_thread.start()

                    # Escuchar respuestas del servidor después de enviar los datasets
                    logging.info("Esperando resultado del servidor...")
                    self.wait_for_result(protocol)

                    logging.info("Cerrando conexión.")
                    break  # Terminar después de recibir respuestas
            except socket.gaierror:
                attempt += 1
                logging.error(
                    f"Error al resolver el hostname {self.boundary_ip}. Intento {attempt} de {self.retries}"
                )
                time.sleep(self.delay)
            except ConnectionRefusedError:
                attempt += 1
                logging.error(
                    f"Conexión rechazada por el servidor en {self.boundary_ip}:{self.boundary_port}. Intento {attempt} de {self.retries}"
                )
                time.sleep(self.delay)
            except Exception as e:
                attempt += 1
                logging.error(
                    f"Error en el cliente: {e}. Intento {attempt} de {self.retries}"
                )
                time.sleep(self.delay)
        logging.critical(
            f"No se pudo conectar al servidor en {self.boundary_ip}:{self.boundary_port} después de {self.retries} intentos."
        )

    def wait_for_result(self, protocol):
        """
        Método para seguir escuchando respuestas del servidor después de enviar los datasets.
        """
        try:
            while True:
                response = protocol.receive_message()
                if response:
                    logging.info(f"Respuesta recibida: {response}")
                    with self.lock:
                        self.responses.append(response)  # Almacenar la respuesta
                    if "close" in response:
                        break
                else:
                    logging.warning(
                        "No se recibió respuesta, el servidor podría haber cerrado la conexión."
                    )
                    break
        except Exception as e:
            logging.error(f"Error al recibir datos del servidor: {e}")

    def periodic_save_responses(self):
        """
        Guardar las respuestas en un archivo JSON periódicamente.
        """
        while True:
            time.sleep(10)  # Guardar cada 10 segundos
            self.save_responses_to_json()

    def save_responses_to_json(self):
        """
        Guardar las respuestas en un archivo JSON.
        """
        try:
            with self.lock:
                with open("/results/responses.json", "w", encoding="utf-8") as json_file:
                    json.dump(self.responses, json_file, indent=4, ensure_ascii=False)
                    logging.info("Respuestas guardadas en /results/responses.json")
        except Exception as e:
            logging.error(f"Error al guardar respuestas en JSON: {e}")