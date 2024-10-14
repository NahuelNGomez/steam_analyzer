# gateway/connectionHandler.py

import json
import logging
import os
from queue import Queue, Empty
import sys
from common.game import Game
from common.middleware import Middleware
from common.protocol import Protocol
from common.constants import MAX_QUEUE_SIZE
from common.review import Review
from common.utils import split_complex_string
import csv
import io
import threading
import socket
import time

input_queues: dict = json.loads(os.getenv("INPUT_QUEUES", "{}"))
output_exchanges = json.loads(os.getenv("OUTPUT_EXCHANGES", "[]"))
instance_id = os.getenv("INSTANCE_ID", "0")

class ConnectionHandler:
    def __init__(self, client_socket, address, amount_of_review_instances):
        self.id_reviews = 0
        self.client_socket = client_socket
        self.address = address
        self.protocol = Protocol(self.client_socket)
        self.reviews_from_client_queue = Queue(maxsize=MAX_QUEUE_SIZE)
        self.reviews_from_client_queue_to_positive = Queue(maxsize=MAX_QUEUE_SIZE)
        self.reviews_to_process_queue = Queue(maxsize=MAX_QUEUE_SIZE)
        self.games_from_client_queue = Queue(maxsize=MAX_QUEUE_SIZE)
        self.result_to_client_queue = Queue(maxsize=MAX_QUEUE_SIZE)
        self.amount_of_review_instances = amount_of_review_instances
        self.completed_games = False
        self.next_instance = 0
        self.remaining_responses = 5
        self.filtrados = 0
        csv.field_size_limit(sys.maxsize)
            
        self.gamesHeader = []
        self.shutdown_event = threading.Event()
        
        # Thread para manejar la conexión
        self.thread = threading.Thread(target=self.handle_connection, name=f"ConnectionHandler-{self.address}")
        self.thread.daemon = True
        self.thread.start()

    def handle_connection(self):
        # Inicializar y arrancar hilos secundarios
        self.games_middleware_sender_thread = threading.Thread(
            target=self.__middleware_sender,
            args=(self.games_from_client_queue, "games", [], 1, 'fanout'),
            name="games_middleware_sender",
            daemon=True
        )
        self.games_middleware_receiver_thread = threading.Thread(
            target=self._middleware_receiver,
            args=(input_queues,),
            name="games_middleware_receiver",
            daemon=True
        )
        self.review_middleware_sender_thread = threading.Thread(
            target=self.__middleware_sender,
            args=(self.reviews_from_client_queue, "reviews", [], 1, 'direct'),
            name="reviews_middleware_sender",
            daemon=True
        )
        self.review_middleware_receiver_thread = threading.Thread(
            target=self._middleware_receiver,
            args=(input_queues,),
            name="reviews_middleware_receiver",
            daemon=True
        )
        self.review_middleware_sender_thread_positive = threading.Thread(
            target=self.__middleware_sender,
            args=(self.reviews_from_client_queue_to_positive, "to_positive_review", [], 1, 'direct'),
            name="reviews_middleware_sender_positive",
            daemon=True
        )
        self.review_process = threading.Thread(
            target=self.process_review,
            name="process_review",
            daemon=True
        )
        
        # Iniciar todos los hilos
        self.games_middleware_sender_thread.start()
        self.games_middleware_receiver_thread.start()
        self.review_middleware_sender_thread.start()
        self.review_middleware_receiver_thread.start()
        self.review_middleware_sender_thread_positive.start()
        self.review_process.start()

        logging.info(f"Conexión establecida desde {self.address}")
        first = True
        try:
            while not self.shutdown_event.is_set():
                data = self.protocol.receive_message()
                if not data:
                    logging.info(f"Cliente {self.address} desconectado")
                    break

                logging.debug(
                    f"Recibido de {self.address}: {data[:50]}..."  # Mostrar solo los primeros 50 caracteres
                )

                # Separar tipo de dataset y contenido usando doble salto de línea
                parts = data.split("\n\n", 1)
                if len(parts) < 2:
                    logging.warning("Datos recibidos en formato incorrecto.")
                    self.protocol.send_message("Error: Formato de datos incorrecto")
                    continue

                if first:
                    first = False
                    self.gamesHeader = parts[1].strip().split("\n")
                    logging.info(f"Header recibido: {self.gamesHeader}")
                    self.protocol.send_message("OK\n\n")
                    continue

                data_type = parts[0].strip().lower()
                if data_type not in ["games", "reviews", "fin"]: 
                    logging.warning(f"Tipo de dataset desconocido: {data_type}")
                    self.protocol.send_message(
                        f"Error: Tipo de dataset desconocido '{data_type}'"
                    )
                    continue

                try:
                    if data_type == "fin":
                        logging.info("Fin de la transmisión de datos")
                        self.protocol.send_message("OK - ACK de fin")
                        self.reviews_from_client_queue.put("fin\n\n")
                        self._fin_sender()
                        break

                    if data_type == "reviews":
                        self.reviews_to_process_queue.put(parts[1])
                        self.protocol.send_message("OK\n\n")
                        if not self.completed_games:
                            self.games_from_client_queue.put("fin\n\n")
                            self.completed_games = True
                        
                    if data_type == "games":
                        games_list = parts[1].strip().split("\n")
                        finalList = ''
                        for row in games_list:
                            try:
                                # Convertir a un objeto Game y procesar los datos
                                game = Game.from_csv_row(row)
                                if game.checkNanElements():
                                    continue
                                game_str = json.dumps(game.getData())
                                finalList += f"{game_str}\n"
                            except Exception as e:
                                logging.error(f"Error al procesar la fila: {row}, error: {e}")
                                continue  # Continuar con la siguiente fila si ocurre un error
                        if finalList:
                            logging.info(f"Enviando los siguientes datos a la cola: {finalList[:50]}...")
                        else:
                            logging.info("No hay datos para enviar después del filtrado.")
                        # Enviar los juegos procesados a la cola
                        self.games_from_client_queue.put(finalList)
                        self.protocol.send_message("OK\n\n")
                except Exception as e:
                    logging.error(f"Error al procesar el CSV: {e}")
                    self.protocol.send_message("Error processing data")

            logging.info(f"Fin del recibo de datos {self.address}")
            self._send_results()
        except Exception as e:
            logging.error(f"Error en la conexión con {self.address}: {e}")
        finally:
            #self.shutdown()
            logging.info("Conexión cerrada.")

    def _send_results(self):
        try:
            while not self.shutdown_event.is_set():
                try:
                    data = self.result_to_client_queue.get(block=True, timeout=1)
                    if data is None:
                        continue
                    if type(data) == str:
                        self.protocol.send_message(data)
                    else:
                        self.protocol.send_message(data.decode())
                except Empty:
                    continue
                except OSError:
                    logging.error("Middleware closed")
                    break
            self.protocol.send_message("close\n\n")
        except Exception as e:
            logging.error(f"Error al enviar resultados al cliente: {e}")

    def _fin_sender(self):
        middleware = Middleware(
            output_exchanges=['to_positive_review'],
            output_queues=['to_positive_review_1_0', 'to_positive_review_2_0', 'to_positive_review_3_0', 'to_positive_review_4_0'],
            amount_output_instances=1,
            exchange_output_type='direct'
        )
        for i in range(4):
            routing = f"to_positive_review_{i+1}_0"
            logging.info(f"Sending to FIN {routing}")
            middleware.send("fin\n\n", routing_key=f"to_positive_review_{i+1}_0")

    def __middleware_sender(self, packet_queue, output_exchange, output_queues, instances, output_type):
        logging.info("Middleware sender started")
        middleware = Middleware(
            output_exchanges=[output_exchange],
            output_queues=output_queues,
            amount_output_instances=instances,
            exchange_output_type=output_type
        )
        while not self.shutdown_event.is_set():
            try:
                packet = packet_queue.get(block=True, timeout=1)
                if packet is None:
                    break
                logging.debug(f"Enviando mensaje {packet[:50]}...")

                if output_exchange == 'reviews':
                    middleware.send(data=packet, routing_key='reviews_queue_1')
                elif output_exchange == 'to_positive_review':
                    routing = f"to_positive_review_{self.next_instance}_0"
                    middleware.send(data=packet, routing_key=routing)
                    self.next_instance = (self.next_instance % 4) + 1
                else:
                    middleware.send(packet)
                
                logging.debug(f"Dispatched message {packet[:50]}...")
            except Empty:
                continue
            except OSError:
                logging.error("Middleware closed")
                break
            except Exception as e:
                logging.error(f"Error en middleware_sender: {e}")
                break
        logging.info("Middleware sender stopped")

    
    def _middleware_receiver(self, input_queues):
        logging.info("Middleware receiver started")
        middleware = Middleware(
            input_queues, [], [], instance_id, self.get_data, self.get_data
        )
        middleware.start()
        logging.info("Middleware receiver stopped")

    def process_review(self):
        while not self.shutdown_event.is_set():
            try:
                packet = self.reviews_to_process_queue.get(block=True, timeout=1)
                review_list = packet.strip().split("\n")
                finalList = ''
                for row in review_list:
                    review = Review.from_csv_row(self.id_reviews, row)
                    if review.checkNanElements():
                        self.filtrados += 1

                        continue
                    review_str = json.dumps(review.getData())
                    finalList += f"{review_str}\n"
                    self.id_reviews += 1
                self.reviews_from_client_queue.put(finalList)
                self.reviews_from_client_queue_to_positive.put(finalList)
                logging.info("Review batch processed")
            except Empty:
                continue
            except Exception as e:
                logging.error(f"Error en process_review: {e}")
                
    def checkData(data):
        data = data

    def get_data(self, data):
        
        logging.info("Data sent to client")
        json_response = json.loads(data)
        if not 'final_check_low_limit' in json_response:
            self.result_to_client_queue.put(data)
        if 'supported_platforms' in json_response:
            logging.info("JSON contains 'supported_platforms'")
            self.remaining_responses -= 1
        if 'top_10_indie_games_2010s' in json_response:
            logging.info("JSON contains 'top_10_indie_games_2010s'")
            self.remaining_responses -= 1
        if 'top_5_indie_games_positive_reviews' in json_response:
            logging.info("JSON contains 'top_5_indie_games_positive_reviews'")
            self.remaining_responses -= 1
        if 'negative_count_percentile' in json_response:
            logging.info("JSON contains 'negative_count_percentile'")
            self.remaining_responses -= 1
        if 'final_check_low_limit' in json_response:
            logging.info("JSON contains 'final_check_low_limit'")
            self.remaining_responses -= 1
        if self.remaining_responses == 0:
            self.result_to_client_queue.put("close\n\n")

    def shutdown(self):
        if not self.shutdown_event.is_set():
            logging.info(f"Iniciando cierre ordenado de la conexión con {self.address}")
            self.shutdown_event.set()
            
            # Enviar FIN a middleware si es necesario
            self._fin_sender()
            
            # Esperar a que los hilos secundarios finalicen
            self.games_middleware_sender_thread.join(timeout=2)
            self.games_middleware_receiver_thread.join(timeout=2)
            self.review_middleware_sender_thread.join(timeout=2)
            self.review_middleware_receiver_thread.join(timeout=2)
            self.review_middleware_sender_thread_positive.join(timeout=2)
            self.review_process.join(timeout=2)
            
            # Cerrar el socket
            try:
                self.client_socket.shutdown(socket.SHUT_RDWR)
            except Exception as e:
                logging.error(f"Error al cerrar el socket: {e}")
            self.client_socket.close()
            logging.info(f"Conexión con {self.address} cerrada exitosamente.")

