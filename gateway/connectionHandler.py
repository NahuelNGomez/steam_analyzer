# gateway/connectionHandler.py

import json
import logging
import os
from queue import Queue
import re
from common.game import Game
from common.middleware import Middleware
from common.protocol import Protocol
from common.constants import MAX_QUEUE_SIZE
from common.utils import split_complex_string
import csv
import io
import threading

input_queues: dict = json.loads(os.getenv("INPUT_QUEUES")) or {}
output_exchanges = json.loads(os.getenv("OUTPUT_EXCHANGES")) or []
instance_id = os.getenv("INSTANCE_ID", 0)

class ConnectionHandler:
    def __init__(self, client_socket, address):
        self.client_socket = client_socket
        self.address = address
        self.protocol = Protocol(self.client_socket)
        self.reviews_from_client_queue = Queue(maxsize=MAX_QUEUE_SIZE)
        self.games_from_client_queue = Queue(maxsize=MAX_QUEUE_SIZE)
        self.result_to_client_queue = Queue(maxsize=MAX_QUEUE_SIZE)
        
        self.gamesHeader = []
        # Thread para manejar la conexión - No lo haría para entrega 1.
        self.thread = threading.Thread(target=self.handle_connection)
        self.thread.daemon = True
        self.thread.start()

    def handle_connection(self):
        self.games_middleware_sender_thread = threading.Thread(
            target=self.__middleware_sender,
            args=(self.games_from_client_queue, "games"),
            name="games_middleware_sender",
        )
        self.games_middleware_receiver_thread = threading.Thread(
            target=self._middleware_receiver,
            args=(input_queues,),
            name="games_middleware_receiver",
        )
        self.review_middleware_sender_thread = threading.Thread(
            target=self.__middleware_sender,
            args=(self.reviews_from_client_queue, "reviews"),
            name="reviews_middleware_sender",
        )
        self.review_middleware_receiver_thread = threading.Thread(
            target=self._middleware_receiver,
            args=(input_queues,),
            name="reviews_middleware_receiver",
        )
        
        self.games_middleware_sender_thread.start()
        self.games_middleware_receiver_thread.start()
        self.review_middleware_sender_thread.start()
        self.review_middleware_receiver_thread.start()

        logging.info(f"Conexión establecida desde {self.address}")
        first = True
        try:
            while True:
                data = self.protocol.receive_message()
                if not data:
                    logging.info(f"Cliente {self.address} desconectado")
                    break

                logging.debug(
                    f"Recibido de {self.address}: {data}..."
                )  # Mostrar solo los primeros 50 caracteres

                # Separar tipo de dataset y contenido usando doble salto de línea
                parts = data.split("\n\n", 1)
                if len(parts) < 2:
                    print("Error: Formato de datos incorrecto:", parts, flush=True)
                    logging.warning("Datos recibidos en formato incorrecto.")
                    self.protocol.send_message("Error: Formato de datos incorrecto")
                    continue

                if first:
                    first = False
                    self.gamesHeader = parts[1].strip().split("\n")
                    print("Header: ", self.gamesHeader, flush=True)
                    self.protocol.send_message("OK")
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
                        self.protocol.send_message("OK - ACK de fin")
                        self.games_from_client_queue.put("fin\n\n")
                        self.reviews_from_client_queue.put("fin\n\n")
                        break

                    if data_type == "reviews":
                        self.reviews_from_client_queue.put(parts[1].strip())
                        
                    if data_type == "games":
                        games_list = parts[1].strip().split("\n")
                        finalList = ''
                        print("Datos recibidos para 'games':", flush=True)
                        print(games_list, flush=True)
                        for row in games_list:
                            try:
                                # Convertir a un objeto Game y procesar los datos
                                game = Game.from_csv_row(row)
                                if game.checkNanElements():
                                    continue
                                game_str = json.dumps(game.getData())
                                finalList += f"{game_str}\n"
                                print(f"Fila procesada y agregada a la lista final: {game_str}", flush=True)
                            except Exception as e:
                                print(f"Error al procesar la fila: {row}, error: {e}", flush=True)
                                continue  # Continuar con la siguiente fila si ocurre un error
                        if finalList:
                            print("Enviando los siguientes datos a la cola:", flush=True)
                            print(finalList, flush=True)
                        else:
                            print("No hay datos para enviar después del filtrado.", flush=True)
                        # Enviar los juegos procesados a la cola
                        self.games_from_client_queue.put(finalList)
                        self.protocol.send_message("OK")

                    
                except Exception as e:
                    logging.error(f"Error al procesar el CSV: {e}")
                    self.protocol.send_message("Error processing data")

            logging.info(f"Fin del recibo de datos {self.address}")
            while True:
                try:
                    data = self.result_to_client_queue.get(block=True)
                    if data is None:
                        break
                    self.protocol.send_message(data)
                except OSError:
                    logging.error("Middleware closed")
                    break
            self.protocol.send_message("close\n\n")

        except Exception as e:
            logging.error(f"Error en la conexión con {self.address}: {e}")
        finally:
            self.client_socket.close()
            logging.info("Conexión cerrada.")

    def __middleware_sender(self, packet_queue, output_exchange):
        logging.info("Middleware sender started")
        print("Middleware sender started", flush=True)
        middleware = Middleware(output_exchanges=[output_exchange])
        while True:
            try:
                packet = packet_queue.get(block=True)
                if packet is None:
                    # middleware.shutdown()
                    break
                logging.debug(f"Enviando mensaje {packet}...")
                middleware.send(data=packet)
                logging.debug(f"Dispatched message {packet}")
                
            except OSError:
                logging.error("Middleware closed")
                break
        logging.info("Middleware sender stopped")

    def _middleware_receiver(self, input_queues):
        logging.info("Middleware receiver started")
        print("Middleware receiver started", flush=True)
        middleware = Middleware(
            input_queues, [], [], instance_id, self.get_data, self.get_data
        )
        middleware.start()
        logging.info("Middleware receiver stopped")

    def get_data(self, data):
        logging.info("Got data!")
        self.result_to_client_queue.put(data)
        logging.info("Data sent to client")
