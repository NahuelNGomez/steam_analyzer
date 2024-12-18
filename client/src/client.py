import time
import socket
import logging
import json
import threading
from common.packet_fin import Fin
from common.protocol import Protocol
from common.constants import MAX_BATCH_SIZE


class Client:
    def __init__(self, boundary_ip, boundary_port, retries=5, delay=5, client_id=0, game_file="sample_1_por_ciento_games.csv", review_file="sample_1_por_ciento_review.csv"):
        self.boundary_ip = boundary_ip
        self.boundary_port = boundary_port
        self.retries = retries
        self.delay = delay
        self.responses = []
        self.lock = threading.Lock()
        self.shutdown_event = threading.Event()
        self.client_id = client_id  # ID del cliente
        self.game_file = game_file  # Ruta del archivo de juegos
        self.review_file = review_file  # Ruta del archivo de reviews

    def send_data(self, protocol, file_path, data_type):
        try:
            with open(file_path, "r", encoding="utf-8") as file:
                batch = []
                first = True
                protocol.send_message(data_type)
                for line in file:
                    if self.shutdown_event.is_set():
                        logging.info(
                            "Señal de cierre recibida. Deteniendo envío de datos."
                        )
                        return
                    if first:
                        first = False
                        message = f"{data_type}\n\n{self.client_id}\n\n{line}"
                        protocol.send_message(message)
                        logging.debug(f"Enviado (inicio {data_type}): {line[:50]}...")
                        continue
                    batch.append(line)
                    if (
                        len(batch) == MAX_BATCH_SIZE
                    ):  # Si alcanzamos el tamaño del batch
                        data = "".join(batch)
                        message = f"{data_type}\n\n{self.client_id}\n\n{data}"
                        protocol.send_message(message)
                        logging.debug(f"Enviado ({data_type}): {data[:50]}...")
                        batch.clear()  # Vaciar el batch para el siguiente conjunto de líneas
                        protocol.receive_message()

                # Enviar cualquier resto de líneas que no formó un batch completo
                if batch:
                    data = "".join(batch)
                    message = f"{data_type}\n\n{self.client_id}\n\n{data}"
                    protocol.send_message(message)
                    logging.debug(f"Enviado ({data_type}): {data[:50]}...")
                    protocol.receive_message()

        except FileNotFoundError:
            logging.error(f"Archivo no encontrado: {file_path}")
        except Exception as e:
            logging.error(f"Error al enviar datos: {e}")

    def send_fin(self, protocol):
        try:
            fin_msg = Fin(0, self.client_id)
            protocol.send_message(fin_msg.encode())
            logging.info("Fin de la transmisión de datos")
        except Exception as e:
            logging.error(f"Error al enviar fin: {e}")

    def start(self):
        attempt = 0
        while attempt < self.retries and not self.shutdown_event.is_set():
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.connect((self.boundary_ip, int(self.boundary_port)))
                    protocol = Protocol(s)
                    logging.info(
                        f"Conectado al servidor en {self.boundary_ip}:{self.boundary_port}"
                    )

                    # Enviar datasets una vez
                    # self.send_data(protocol, "data/games.csv", "games")
                    # self.send_data(protocol, "data/dataset.csv", "reviews")
                    
                    response = self.send_handshake(protocol)
                    if not response:
                        game_path = "../data/" + self.game_file
                        self.send_data(
                            protocol, game_path, "games"
                        )
                        review_path = "../data/" + self.review_file
                        self.send_data(
                            protocol, review_path, "reviews"
                        )
                        self.send_fin(protocol)

                        save_thread = threading.Thread(
                            target=self.periodic_save_responses, name="SaveThread"
                        )
                        save_thread.daemon = True
                        save_thread.start()

                        logging.info("Esperando resultado del servidor...")

                        logging.info("Cerrando conexión.")
                    self.wait_for_result(protocol)
                    break

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
        else:
            if not self.shutdown_event.is_set():
                logging.critical(
                    f"No se pudo conectar al servidor en {self.boundary_ip}:{self.boundary_port} después de {self.retries} intentos."
                )

    def send_handshake(self, protocol):
        
        message = f"handshake\n\n{self.client_id}\n\n"
        protocol.send_message(message)
        response = protocol.receive_message()
        print("Handshake enviado", flush=True)
        print("Respuesta del servidor: ", response, flush=True)
        
        if ("False\n\n" in response):
            print("handshake exitoso", flush=True)
            return False
        else:
            return True
    
    def wait_for_result(self, protocol):
        """
        Método para seguir escuchando respuestas del servidor después de enviar los datasets.
        """
        try:
            while not self.shutdown_event.is_set():
                response = protocol.receive_message()
                if response:
                    logging.info(f"Respuesta recibida: {response}")
                    with self.lock:
                        if ("ACK de fin" in response) or ("close" in response) or ("OK\n\n" in response) :
                            logging.info("Fin de la transmisión de resultados")
                        try:
                            json_response = json.loads(response)
                            processed_response = self.restructure_json(json_response)
                            self.responses.append(processed_response)
                        except json.JSONDecodeError:
                            logging.warning(f"Respuesta no es JSON válida: {response}")
                        
                    if "close" in response:
                        break
                else:
                    logging.warning(
                        "No se recibió respuesta, el servidor podría haber cerrado la conexión."
                    )
                    break
            self.save_responses_to_json()
        except Exception as e:
            logging.error(f"Error al recibir datos del servidor: {e}")

    def periodic_save_responses(self):
        """
        Guardar las respuestas en un archivo JSON periódicamente.
        """
        while not self.shutdown_event.is_set():
            time.sleep(10)  # Guardar cada 10 segundos
            self.save_responses_to_json()

    def save_responses_to_json(self):
        """
        Guardar las respuestas en un archivo JSON.
        """
        try:
            with self.lock:
                path = "/results/dist_results_" + str(self.client_id)+ ".json"
                with open(
                    path, "w", encoding="utf-8"
                ) as json_file:
                    json.dump(self.responses, json_file, indent=4, ensure_ascii=False)
                    logging.info("Respuestas guardadas en /results/dist_results.json")
        except Exception as e:
            logging.error(f"Error al guardar respuestas en JSON: {e}")

    def shutdown(self):
        """
        Método para manejar el cierre del cliente de manera ordenada.
        """
        logging.info("Iniciando cierre ordenado del cliente.")
        self.shutdown_event.set()
        # Esperar a que los hilos secundarios finalicen si es necesario
        self.save_responses_to_json()
        logging.info("Cliente cerrado exitosamente.")

    def restructure_json(self, json_data):
        """
        Elimina la capa `client_id` y reestructura el JSON para cada categoría.
        Acepta tanto un diccionario único como una lista de diccionarios.
        
        Args:
            json_data: Puede ser un diccionario o una lista de diccionarios
            
        Returns:
            dict: JSON reestructurado sin la capa de client_id
        """
        restructured_data = {}
        
        # Si recibimos un solo diccionario, lo convertimos en una lista
        if isinstance(json_data, dict):
            json_data = [json_data]
        elif not isinstance(json_data, list):
            raise ValueError("Los datos JSON deben ser un diccionario o una lista de diccionarios.")
        
        for category in json_data:
            if not isinstance(category, dict):
                raise ValueError("Cada categoría debe ser un diccionario.")
            
            for category_key, category_value in category.items():
                if not isinstance(category_value, dict):
                    raise ValueError(f"Se esperaba un diccionario para la categoría {category_key}.")
                
                # Inicializa la lista si la categoría no existe
                if category_key not in restructured_data:
                    restructured_data[category_key] = []
                
                # Procesa cada item bajo el client_id
                for client_id, items in category_value.items():
                    if not isinstance(items, list):
                        raise ValueError(f"Se esperaba una lista de items bajo {client_id}.")
                    
                    # Añade cada entrada sin la capa de client_id
                    restructured_data[category_key].extend(items)
        
        return restructured_data
