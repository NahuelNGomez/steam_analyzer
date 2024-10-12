# gateway/connectionHandler.py

import json
import logging
import os
from queue import Queue, Empty
from common.game import Game
from common.middleware import Middleware  # Asegúrate de que esta línea esté presente
from common.protocol import Protocol
from common.constants import MAX_QUEUE_SIZE
from common.review import Review
import threading
import socket

# Configuración de variables de entorno
input_queues: dict = json.loads(os.getenv("INPUT_QUEUES", "{}")) or {}
instance_id = os.getenv("INSTANCE_ID", "0")
shutdown_event = threading.Event()

class MiddlewareSender(threading.Thread):
    def __init__(self, packet_queue, output_exchange, output_queues, instances, name):
        super().__init__(name=name, daemon=True)
        self.packet_queue = packet_queue
        self.output_exchange = output_exchange
        self.output_queues = output_queues
        self.instances = instances
        self.middleware = Middleware(
            output_exchanges=[self.output_exchange],
            output_queues=self.output_queues,
            amount_output_instances=self.instances
        )

    def run(self):
        logging.info(f"{self.name} iniciado")
        print(f"{self.name} iniciado", flush=True)
        while not shutdown_event.is_set():
            try:
                packet = self.packet_queue.get(block=True)
                if packet is None:
                    logging.info(f"{self.name} recibió señal de cierre")
                    break
                logging.debug(f"{self.name} enviando mensaje: {packet[:50]}...")
                self.middleware.send(data=packet)
                logging.debug(f"{self.name} mensaje despachado")
            except Empty:
                continue
            except Exception as e:
                logging.error(f"Error en {self.name}: {e}")
                break
        logging.info(f"{self.name} detenido")

class MiddlewareReceiver(threading.Thread):
    def __init__(self, input_queues, name):
        super().__init__(name=name, daemon=True)
        self.input_queues = input_queues
        self.middleware = Middleware(
            input_queues=self.input_queues,
            output_exchanges=[],
            output_queues=[]
        )

    def run(self):
        logging.info(f"{self.name} iniciado")
        print(f"{self.name} iniciado", flush=True)
        try:
            self.middleware.start()
        except Exception as e:
            logging.error(f"Error en {self.name}: {e}")
        logging.info(f"{self.name} detenido")

class ConnectionHandler:
    def __init__(self, client_socket, address, amount_of_review_instances):
        self.id_reviews = 0
        self.client_socket = client_socket
        self.address = address
        self.protocol = Protocol(self.client_socket)
        self.amount_of_review_instances = amount_of_review_instances
        self.completed_games = False
        self.gamesHeader = []
        self.result_to_client_queue = Queue(maxsize=MAX_QUEUE_SIZE)

        # Colas para middleware
        self.reviews_from_client_queue = Queue(maxsize=MAX_QUEUE_SIZE)
        self.reviews_to_process_queue = Queue(maxsize=MAX_QUEUE_SIZE)
        self.games_from_client_queue = Queue(maxsize=MAX_QUEUE_SIZE)

        # Hilos de middleware
        self.sender_games = MiddlewareSender(
            packet_queue=self.games_from_client_queue,
            output_exchange="games",
            output_queues=[],  # Agregar si es necesario
            instances=1,
            name=f"MiddlewareSender-Games-{self.address}"
        )
        self.receiver_games = MiddlewareReceiver(
            input_queues=input_queues,
            name=f"MiddlewareReceiver-Games-{self.address}"
        )
        self.sender_reviews = MiddlewareSender(
            packet_queue=self.reviews_from_client_queue,
            output_exchange="reviews",
            output_queues=["reviews_queue"],
            instances=self.amount_of_review_instances,
            name=f"MiddlewareSender-Reviews-{self.address}"
        )
        self.receiver_reviews = MiddlewareReceiver(
            input_queues=input_queues,
            name=f"MiddlewareReceiver-Reviews-{self.address}"
        )
        self.process_review_thread = threading.Thread(
            target=self.process_review,
            name=f"ProcessReview-{self.address}",
            daemon=True
        )

        # Lista de hilos para facilitar el shutdown
        self.threads = [
            self.sender_games,
            self.receiver_games,
            self.sender_reviews,
            self.receiver_reviews,
            self.process_review_thread
        ]

        self._start_threads()

    def _start_threads(self):
        for thread in self.threads:
            thread.start()

        # Hilo principal para manejar la conexión
        self.connection_thread = threading.Thread(
            target=self.handle_connection,
            name=f"ConnectionHandler-{self.address}",
            daemon=True
        )
        self.connection_thread.start()
        self.threads.append(self.connection_thread)

    def handle_connection(self):
        logging.info(f"Conexión establecida desde {self.address}")
        first = True
        try:
            while not shutdown_event.is_set():
                try:
                    data = self.protocol.receive_message() 
                except Exception as e:
                    logging.error(f"Error al recibir mensaje: {e}")
                    break

                if not data:
                    logging.info(f"Cliente {self.address} desconectado")
                    break

                logging.debug(f"Recibido de {self.address}: {data[:50]}...")

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
                    self.protocol.send_message("OK")
                    continue

                data_type = parts[0].strip().lower()
                if data_type not in ["games", "reviews", "fin"]: 
                    logging.warning(f"Tipo de dataset desconocido: {data_type}")
                    self.protocol.send_message(f"Error: Tipo de dataset desconocido '{data_type}'")
                    continue

                try:
                    if data_type == "fin":
                        logging.info("Fin de la transmisión de datos")
                        self.protocol.send_message("OK - ACK de fin")
                        self.reviews_from_client_queue.put("fin\n\n")
                        break

                    if data_type == "reviews":
                        processed_reviews = self.process_reviews(parts[1])
                        if processed_reviews:
                            self.reviews_from_client_queue.put(processed_reviews)
                            self.protocol.send_message("OK")
                            if not self.completed_games:
                                self.completed_games = True

                    if data_type == "games":
                        print("Llega un game batch", flush=True)
                        games_list = parts[1].strip().split("\n")
                        finalList = ''
                        print("Datos recibidos para 'games':", flush=True)
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
                        else:
                            print("No hay datos para enviar después del filtrado.", flush=True)
                        # Enviar los juegos procesados a la cola
                        self.games_from_client_queue.put(finalList)
                        self.protocol.send_message("OK")

                except Exception as e:
                    logging.error(f"Error al procesar los datos: {e}")
                    self.protocol.send_message("Error processing data")

            logging.info(f"Fin del recibo de datos {self.address}")
            while not shutdown_event.is_set():
                try:
                    data = self.result_to_client_queue.get(block=True)
                    if data is None:
                        continue
                    if isinstance(data, str):
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
            logging.error(f"Error en la conexión con {self.address}: {e}")
        finally:
            self.shutdown()

    def process_games(self, data):
        logging.info("Procesando batch de juegos")
        games_list = data.strip().split("\n")
        finalList = ''
        for row in games_list:
            try:
                game = Game.from_csv_row(row)
                if game.checkNanElements():
                    continue
                game_str = json.dumps(game.getData())
                finalList += f"{game_str}\n"
            except Exception as e:
                logging.error(f"Error al procesar la fila: {row}, error: {e}")
                continue
        return finalList if finalList else None

    def process_reviews(self, data):
        logging.info("Procesando batch de reviews")
        review_list = data.strip().split("\n")
        finalList = ''
        for row in review_list:
            try:
                review = Review.from_csv_row(self.id_reviews, row)
                if review.checkNanElements():
                    continue
                review_str = json.dumps(review.getData())
                finalList += f"{review_str}\n"
                self.id_reviews += 1
            except Exception as e:
                logging.error(f"Error al procesar review: {row}, error: {e}")
                continue
        return finalList if finalList else None

    def process_review(self):
        while not shutdown_event.is_set():
            try:
                packet = self.reviews_to_process_queue.get(block=True)
                if packet is None:
                    break
                review_list = packet.strip().split("\n")
                finalList = ''
                for row in review_list:
                    try:
                        review = Review.from_csv_row(self.id_reviews, row)
                        if review.checkNanElements():
                            continue
                        review_str = json.dumps(review.getData())
                        finalList += f"{review_str}\n"
                        self.id_reviews += 1
                    except Exception as e:
                        logging.error(f"Error al procesar review: {row}, error: {e}")
                        continue
                if finalList:
                    self.reviews_from_client_queue.put(finalList)
                    logging.info("Review batch procesado y enviado al middleware")
            except Empty:
                continue
            except Exception as e:
                logging.error(f"Error en process_review: {e}")
                continue

    def get_data(self, data):
        logging.info("Got data!")
        self.result_to_client_queue.put(data)
        logging.info("Data sent to client")

    def shutdown(self):
        logging.info(f"Cerrando conexión con {self.address}")

        # Señalar a las colas que no se enviarán más datos
        self.games_from_client_queue.put(None)
        self.reviews_from_client_queue.put(None)
        self.reviews_to_process_queue.put(None)
        self.result_to_client_queue.put(None)

        # Detener hilos de middleware
        for thread in self.threads:
            if isinstance(thread, MiddlewareSender) or isinstance(thread, MiddlewareReceiver):
                logging.info(f"Esperando que termine el hilo {thread.name}")
                thread.join()
            elif thread.name.startswith("ProcessReview"):
                logging.info(f"Esperando que termine el hilo {thread.name}")
                thread.join()
            elif thread.name.startswith("ConnectionHandler"):
                # El hilo de conexión ya está manejando el shutdown
                continue

        # Cerrar el socket de manera segura
        try:
            self.client_socket.shutdown(socket.SHUT_RDWR)
        except OSError:
            logging.warning("El socket ya estaba cerrado o no se pudo cerrar.")
        finally:
            self.client_socket.close()

        logging.info(f"Conexión con {self.address} cerrada.")
