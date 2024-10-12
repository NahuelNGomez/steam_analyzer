import time
import socket
import logging
import json
from common.protocol import Protocol
from common.constants import MAX_BATCH_SIZE
import threading
import signal
import sys

class Client:
    def __init__(self, boundary_ip, boundary_port, retries=5, delay=5):
        self.boundary_ip = boundary_ip
        self.boundary_port = boundary_port
        self.retries = retries
        self.delay = delay
        self.responses = []
        self.lock = threading.Lock()

        # Inicializa el socket y el estado del cliente
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.protocol = None
        self.stop_event = False  # Booleano para detener el cliente

        # Configura el manejador de señales
        signal.signal(signal.SIGTERM, self.handle_sigterm)

        self.save_thread = threading.Thread(target=self.periodic_save_responses)
        self.save_thread.daemon = True

    def shutdown(self):
        """Realiza el cierre del cliente de forma segura."""
        if self.stop_event:
            logging.info("El cliente ya está detenido.")
            return

        logging.info("Cerrando cliente...")
        self.stop_event = True

        # Cerrar socket de forma segura
        try:
            self.socket.shutdown(socket.SHUT_RDWR)
            self.socket.close()
            logging.info("Socket cerrado correctamente.")
        except Exception as e:
            logging.error(f"Error al cerrar el socket: {e}")

        # Unir hilo de guardado para asegurar cierre limpio
        if self.save_thread.is_alive():
            self.save_thread.join()
            logging.info("Hilo de guardado terminado.")

    def handle_sigterm(self, signum, frame):
        """Maneja la señal SIGTERM."""
        logging.info("SIGTERM recibido. Cerrando cliente...")
        self.shutdown()
        sys.exit(0)

    def start(self):
        """Inicia la conexión con el servidor y maneja los intentos."""
        self.save_thread.start()  # Inicia el hilo de guardado
        attempt = 0

        while attempt < self.retries and not self.stop_event:
            try:
                logging.info(f"Intentando conectar a {self.boundary_ip}:{self.boundary_port}")
                self.socket.connect((self.boundary_ip, int(self.boundary_port)))
                logging.info(f"Conectado al servidor en {self.boundary_ip}:{self.boundary_port}")
                self.protocol = Protocol(self.socket)

                # Enviar los datasets al servidor
                self.send_data(self.protocol, "data/sample_1111_por_ciento.csv", "games")
                self.send_data(self.protocol, "data/sample_3333_por_ciento_review.csv", "reviews")
                self.send_fin(self.protocol)

                logging.info("Esperando resultado del servidor...")
                self.wait_for_result(self.protocol)

                logging.info("Cerrando conexión.")
                break  # Termina después de enviar y recibir datos

            except (socket.gaierror, ConnectionRefusedError) as e:
                attempt += 1
                logging.error(f"Error en la conexión: {e}. Intento {attempt} de {self.retries}")
                time.sleep(self.delay)
            except ConnectionResetError:
                logging.error("Conexión cerrada por el servidor.")
                self.shutdown()
            except Exception as e:
                attempt += 1
                logging.error(f"Error en el cliente: {e}. Intento {attempt} de {self.retries}")
                time.sleep(self.delay)

        if not self.stop_event:
            logging.critical("No se pudo conectar al servidor después de múltiples intentos.")
        self.shutdown()

    def send_data(self, protocol, file_path, data_type):
        """Envía datos desde un archivo al servidor."""
        try:
            with open(file_path, "r", encoding="utf-8") as file:
                batch = []
                first = True
                protocol.send_message(data_type)

                for line in file:
                    if self.stop_event:
                        logging.info("Detenido durante el envío de datos.")
                        return

                    if first:
                        first = False
                        protocol.send_message(f"{data_type}\n\n{line}")
                        continue

                    batch.append(line)
                    if len(batch) == MAX_BATCH_SIZE:
                        data = ''.join(batch)
                        protocol.send_message(f"{data_type}\n\n{data}")
                        logging.debug(f"Enviado ({data_type}): {data[:50]}...")
                        batch.clear()
                        protocol.receive_message()

                if batch:
                    data = ''.join(batch)
                    protocol.send_message(f"{data_type}\n\n{data}")
                    logging.debug(f"Enviado ({data_type}): {data[:50]}...")
                    protocol.receive_message()

        except FileNotFoundError:
            logging.error(f"Archivo no encontrado: {file_path}")
        except Exception as e:
            logging.error(f"Error al enviar datos: {e}")

    def send_fin(self, protocol):
        """Envía el mensaje de fin de transmisión."""
        try:
            protocol.send_message("fin\n\n")
            logging.info("Mensaje de fin enviado.")
        except Exception as e:
            logging.error(f"Error al enviar fin: {e}")

    def wait_for_result(self, protocol):
        """Escucha respuestas del servidor después de enviar los datasets."""
        try:
            while not self.stop_event:
                response = protocol.receive_message()
                if response:
                    logging.info(f"Respuesta recibida: {response}")
                    with self.lock:
                        self.responses.append(response)
                    if "close" in response:
                        break
                else:
                    logging.warning("No se recibió respuesta, el servidor podría haber cerrado la conexión.")
                    break
        except Exception as e:
            logging.error(f"Error al recibir datos del servidor: {e}")

    def periodic_save_responses(self):
        """Guarda las respuestas periódicamente en un archivo JSON."""
        while not self.stop_event:
            time.sleep(10)
            self.save_responses_to_json()
        self.save_responses_to_json()

    def save_responses_to_json(self):
        """Guarda las respuestas en un archivo JSON."""
        try:
            with self.lock:
                with open("/results/responses.json", "w", encoding="utf-8") as json_file:
                    json.dump(self.responses, json_file, indent=4, ensure_ascii=False)
                    logging.info("Respuestas guardadas en /results/responses.json")
        except Exception as e:
            logging.error(f"Error al guardar respuestas en JSON: {e}")
# import time
# import socket
# import logging
# import json
# from common.protocol import Protocol
# from common.constants import MAX_BATCH_SIZE
# import threading

# class Client:
#     def __init__(self, boundary_ip, boundary_port, retries=5, delay=5):
#         self.boundary_ip = boundary_ip
#         self.boundary_port = boundary_port
#         self.retries = retries
#         self.delay = delay
#         self.responses = []  
#         self.lock = threading.Lock()  
        
#     def send_data(self, protocol, file_path, data_type):
#         try:
#             with open(file_path, "r", encoding="utf-8") as file:
#                 batch = []
#                 first = True
#                 protocol.send_message(data_type)
#                 for line in file:
#                     if first:
#                         first = False
#                         message = f"{data_type}\n\n{line}"
#                         protocol.send_message(message)
#                         continue
#                     batch.append(line)
#                     if len(batch) == MAX_BATCH_SIZE:  # Si alcanzamos el tamaño del batch
#                         data = ''.join(batch)
#                         message = f"{data_type}\n\n{data}"
#                         protocol.send_message(message)
#                         logging.debug(f"Enviado ({data_type}): {data[:50]}...")
#                         batch.clear()  # Vaciar el batch para el siguiente conjunto de líneas
#                         protocol.receive_message()

#                 # Enviar cualquier resto de líneas que no formó un batch completo
#                 if batch:
#                     data = ''.join(batch)
#                     message = f"{data_type}\n\n{data}"
#                     protocol.send_message(message)
#                     logging.debug(f"Enviado ({data_type}): {data[:50]}...")
#                     protocol.receive_message()

#         except FileNotFoundError:
#             logging.error(f"Archivo no encontrado: {file_path}")
#         except Exception as e:
#             logging.error(f"Error al enviar datos: {e}")

#     def send_fin(self, protocol):
#         try:
#             message = "fin\n\n"
#             protocol.send_message(message)
#             print("Fin de la transmisión de datos", flush=True)
#             logging.debug(f"Enviado ({message})")
#         except FileNotFoundError:
#             logging.error(f"Error al enviar fin")
#         except Exception as e:
#             logging.error(f"Error al enviar datos: {e}")

#     def start(self):
#         attempt = 0
#         while attempt < self.retries:
#             try:
#                 with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
#                     s.connect((self.boundary_ip, int(self.boundary_port)))
#                     protocol = Protocol(s)
#                     logging.info(
#                         f"Conectado al servidor en {self.boundary_ip}:{self.boundary_port}"
#                     )
                    
#                     # Enviar datasets una vez
#                     self.send_data(protocol, "data/sample_1111_por_ciento.csv", "games")
#                     self.send_data(protocol, "data/sample_3333_por_ciento_review.csv", "reviews")
#                     self.send_fin(protocol)

#                     # Iniciar un hilo para guardar respuestas periódicamente
#                     save_thread = threading.Thread(target=self.periodic_save_responses)
#                     save_thread.daemon = True
#                     save_thread.start()

#                     # Escuchar respuestas del servidor después de enviar los datasets
#                     logging.info("Esperando resultado del servidor...")
#                     self.wait_for_result(protocol)

#                     logging.info("Cerrando conexión.")
#                     break  # Terminar después de recibir respuestas
#             except socket.gaierror:
#                 attempt += 1
#                 logging.error(
#                     f"Error al resolver el hostname {self.boundary_ip}. Intento {attempt} de {self.retries}"
#                 )
#                 time.sleep(self.delay)
#             except ConnectionRefusedError:
#                 attempt += 1
#                 logging.error(
#                     f"Conexión rechazada por el servidor en {self.boundary_ip}:{self.boundary_port}. Intento {attempt} de {self.retries}"
#                 )
#                 time.sleep(self.delay)
#             except Exception as e:
#                 attempt += 1
#                 logging.error(
#                     f"Error en el cliente: {e}. Intento {attempt} de {self.retries}"
#                 )
#                 time.sleep(self.delay)
#         logging.critical(
#             f"No se pudo conectar al servidor en {self.boundary_ip}:{self.boundary_port} después de {self.retries} intentos."
#         )

#     def wait_for_result(self, protocol):
#         """
#         Método para seguir escuchando respuestas del servidor después de enviar los datasets.
#         """
#         try:
#             while True:
#                 response = protocol.receive_message()
#                 if response:
#                     logging.info(f"Respuesta recibida: {response}")
#                     with self.lock:
#                         self.responses.append(response)  # Almacenar la respuesta
#                     if "close" in response:
#                         break
#                 else:
#                     logging.warning(
#                         "No se recibió respuesta, el servidor podría haber cerrado la conexión."
#                     )
#                     break
#         except Exception as e:
#             logging.error(f"Error al recibir datos del servidor: {e}")

#     def periodic_save_responses(self):
#         """
#         Guardar las respuestas en un archivo JSON periódicamente.
#         """
#         while True:
#             time.sleep(10)  # Guardar cada 10 segundos
#             self.save_responses_to_json()

#     def save_responses_to_json(self):
#         """
#         Guardar las respuestas en un archivo JSON.
#         """
#         try:
#             with self.lock:
#                 with open("/results/responses.json", "w", encoding="utf-8") as json_file:
#                     json.dump(self.responses, json_file, indent=4, ensure_ascii=False)
#                     logging.info("Respuestas guardadas en /results/responses.json")
#         except Exception as e:
#             logging.error(f"Error al guardar respuestas en JSON: {e}")