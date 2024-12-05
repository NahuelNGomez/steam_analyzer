import json
import logging
import os
import threading
from common.game import Game
from common.game_review import GameReview
from common.middleware import Middleware
from common.packet_fin import Fin
from common.review import Review
from common.fault_manager import FaultManager
import uuid

class GameReviewFilter:
    def __init__(
        self,
        input_games_queue,
        input_reviews_queue,
        output_exchanges,
        output_queues,
        instance_id,
        previous_review_nodes,
        amount_of_language_filters,
    ):
        """
        :param input_queues: Lista de colas de entrada (e.g., ['action_games_queue', 'positive_reviews_queue']).
        :param output_queue: Cola de salida (e.g., 'action_games_positive_reviews_queue').
        :param join_key: Clave para realizar el join (por defecto, 'app_id').
        """
        self.join_key = "app_id"

        self.games_input_queue = input_games_queue
        self.reviews_input_queue = input_reviews_queue
        self.output_exchanges = output_exchanges
        self.output_queues = output_queues
        self.instance_id = instance_id
        self.completed_games: dict = {}
        self.completed_reviews: dict = {}
        self.sended_fin: dict = {}
        self.reviews_to_add: dict = {}  # {client_id: [reviews]}
        self.previous_review_nodes = previous_review_nodes
        self.nodes_completed: dict = {}  # {client_id: nodes_completed_count}
        self.review_file_size: dict = {}  # {client_id: file_size_count}
        self.batch_counter: dict = {} 
        self.total_batches: dict = {} 
        self.last_processed_packet: dict = {} 
        self.amount_of_language_filters = amount_of_language_filters
        self.fault_manager = FaultManager("../persistence/", self.reviews_input_queue[0])
        self.next_instance = 1
        if "action" in self.games_input_queue[1].lower():
            self.packet_id = 1
        else:
            self.packet_id = int(self.reviews_input_queue[0].split("_")[-1])

        self.action_packet_id = 1
        self.games: dict = {}
        self.file_lock = threading.Lock()
        self.init_state()
        self.games_receiver = threading.Thread(target=self._games_receiver)
        self.reviews_receiver = threading.Thread(target=self._reviews_receiver)
        

    def _games_receiver(self):
        self.books_middleware = Middleware(
            input_queues={self.games_input_queue[0]: self.games_input_queue[1]},
            callback=self._add_game,
            eofCallback=self.handle_game_eof,
            output_queues=self.output_queues,  ## ???
            output_exchanges=self.output_exchanges,  ## ???
            faultManager=self.fault_manager,
            intance_id=self.instance_id,
            exchange_output_type="direct"
        )
        self.books_middleware.start()
        
    def _reviews_receiver(self):
        self.reviews_middleware = Middleware(
            input_queues={self.reviews_input_queue[0]: self.reviews_input_queue[1]},
            callback=self._add_review,
            eofCallback=self.handle_review_eof,
            output_queues=[],  ## ???
            output_exchanges=self.output_exchanges,  ## ???
            faultManager=self.fault_manager,
            intance_id=self.instance_id,
            exchange_output_type="direct",
        )
        for key in self.fault_manager.get_keys(f'processed_packets_{self.reviews_input_queue[0]}'):
            data = self.fault_manager.get(key)
            data = json.loads(data)
            #client_id = int(key.split("_")[-1])
            self.action_packet_id = data["last_sended_packet"]
            self.packet_id = data["last_sended_packet"]
            last_init_process_packet = data["last_init_process_packet"]
            logging.info(f"last_sended_packet: {self.packet_id} - last_init_process_packet: {last_init_process_packet}")
            
            for key in self.fault_manager.get_keys(f'game_filter_{self.reviews_input_queue[0]}'):
                if self.fault_manager.get(key) is not None:
                    client_id = int(key.split("_")[-1])
                    if self.action_packet_id != data["last_init_process_packet"]: # se quedó a medio procesar
                        self.action_packet_id = data["last_init_process_packet"]
                        self.packet_id = data["last_init_process_packet"]
                        logging.info(f"[1] Estado PROCESSED para cliente {client_id}: {self.action_packet_id}")
                        self.process_reviews(f"review_filter_{self.reviews_input_queue[0]}_{client_id}", client_id)
                        
            
            for key in self.fault_manager.get_keys(f'review_filter_{self.reviews_input_queue[0]}'):
                    
                if self.fault_manager.get(key) is not None:
                    client_id = int(key.split("_")[-1])
                    if self.action_packet_id == data["last_sended_packet"]:
                        logging.info(f"[2] Estado PROCESSED para cliente {client_id}: {self.action_packet_id}") 
                        self.process_reviews(f"review_filter_{self.reviews_input_queue[0]}_{client_id}", client_id)
                logging.info(f"[3] Estado PROCESSED para cliente {client_id}: {self.action_packet_id}")
        self.reviews_middleware.start()
        
    def init_state(self):
        for key in self.fault_manager.get_keys(f'game_filter_{self.reviews_input_queue[0]}'):
            data = self.fault_manager.get(key)
            data = data.strip().split("\n")
            client_id = int(key.split("_")[-1])
            if client_id not in self.games:
                self.games[client_id] = {}
            
            for line in data:
                if not line:
                    continue
                try:
                    uuid_line = uuid.UUID(line)
                    last_packet_id = str(uuid_line)
                    continue
                except:
                    pass
                json_data_game = json.loads(line)
                game_id = json_data_game["id"]
                game_name = json_data_game["name"]
                
                self.games[client_id][str(game_id)] = game_name
            # logging.info(f"Estado JUEGOS para cliente {client_id}: {last_packet_id} - {self.games[client_id]}")
        for key in self.fault_manager.get_keys(f'review_filter_{self.reviews_input_queue[0]}'):
            data = self.fault_manager.get(key)
            data = data.strip().split("\n")
            client_id = int(key.split("_")[-1])
            if client_id not in self.total_batches:
                self.total_batches[client_id] = 0
            if client_id not in self.batch_counter:
                self.batch_counter[client_id] = 0
            if client_id not in self.last_processed_packet:
                self.last_processed_packet[client_id] = None
            for line in data:
                if not line:
                    continue
                json_data = json.loads(line)
                if isinstance(json_data, dict) and all(key in json_data for key in ["packet_id", "batch_counter", "total_batches"]):
                    self.batch_counter[client_id] = int(json_data["batch_counter"])
                    self.total_batches[client_id] = int(json_data["total_batches"])
                    self.last_processed_packet[client_id] = json_data["packet_id"]
            logging.info(f"Estado REVIEW para cliente {client_id}: {self.batch_counter[client_id]} - {self.total_batches[client_id]} - {self.last_processed_packet[client_id]}")
            
        

    def _add_game(self, game):
        """
        Agrega un juego al diccionario de juegos, organizados por client_id.
        """
        try:
            aux = game.strip().split("\n")
            packet_id = aux[0]
            batch = aux[1:]
            client_id_file = None
            
            
            final_list = str(packet_id) + "\n"
            for game in batch:
                
                game = Game.decode(json.loads(game))
                # logging.info(f"Recibido juego - {packet_id}")
                client_id = game.client_id
                client_id_file = client_id
                if client_id not in self.games:
                    self.games[client_id] = {}
                self.games[client_id][str(game.id)] = game.name
                game_data = {
                    "id": game.id,
                    "name": game.name,
                }
                final_list += f"{json.dumps(game_data)}\n"
            
            self.fault_manager.append(f"game_filter_{self.reviews_input_queue[0]}_{client_id_file}", final_list)

        except Exception as e:
            logging.error(f"Error al agregar juego para cliente {game.client_id}: {e}")

    def _add_review(self, message):
        """
        Agrega una review a la lista y escribe en el archivo cuando llega a 1000.
        """
        batch = message.split("\n")
        packet_id = batch[0]
        #logging.info(f"Recibiendo REVIEW - {packet_id}")
        batch = batch[1:]
        if packet_id == self.last_processed_packet:
            logging.info("Ignoring duplicate packet.")
            return
        #final_list = str(packet_id) + "\n"
       # print("Recibiendo REVIEW - batch:", len(batch), flush=True)
       # print("Recibiendo REVIEW - batch:", batch, flush=True)
        client_id = int(Review.decode(json.loads(batch[0])).client_id)
        # print("Recibiendo REVIEW - client_id:", client_id, flush=True)
        if client_id not in self.batch_counter:
            self.batch_counter[client_id] = 0
        if client_id not in self.reviews_to_add:
            self.reviews_to_add[client_id] = []
        if client_id not in self.review_file_size:
            self.review_file_size[client_id] = 0
        if client_id not in self.nodes_completed:
            self.nodes_completed[client_id] = 0
        if client_id not in self.sended_fin:
            self.sended_fin[client_id] = False
        if client_id not in self.total_batches:
            self.total_batches[client_id] = 0
        if client_id not in self.completed_reviews:
            self.completed_reviews[client_id] = False
        
        self.batch_counter[client_id] += 1
        logging.info(f"Recibiendo REVIEW - batch_counter: {self.batch_counter[client_id]}")
        with self.file_lock:
            for row in batch:
                if not row.strip():
                    continue
                self.reviews_to_add[client_id].append(row)
            meta_data = {
                "packet_id": packet_id,
                "batch_counter": self.batch_counter[client_id],
                "total_batches": self.total_batches[client_id],
            }
            data_to_send = json.dumps(meta_data) +'\n' + '\n'.join(self.reviews_to_add[client_id])
            self.fault_manager.append(f"review_filter_{self.reviews_input_queue[0]}_{client_id}", data_to_send)
            
            self.reviews_to_add[client_id] = []
            self.review_file_size[client_id] += 1

            if self.review_file_size[client_id] >= 2000:
                print("Procesando reviews para cliente {client_id}", flush=True)
                self.review_file_size[client_id] = 0
                self.process_reviews(
                    f"review_filter_{self.reviews_input_queue[0]}_{client_id}",
                    client_id,
                )

                    
        if (self.nodes_completed[client_id] == self.previous_review_nodes) and not self.sended_fin[client_id]:
            if self.batch_counter[client_id] >= self.total_batches[client_id] and self.total_batches[client_id] != 0:
                
                print("Fin de la transmisión de datos batches",self.batch_counter[client_id],flush=True)
                with self.file_lock:
                    self.save_last_reviews(client_id)
                    self.process_reviews(f"review_filter_{self.reviews_input_queue[0]}_{client_id}", client_id)
                    self.send_fin(client_id)
                self.sended_fin[client_id] = True
                self.fault_manager.delete_key(f"game_filter_{self.reviews_input_queue[0]}_{client_id}")
                self.fault_manager.delete_key(f"processed_packets_{self.reviews_input_queue[0]}")

        
    def send_fin(self, client_id):
        # print("envío fin", flush=True)
        self.reviews_middleware.send(Fin(0, client_id).encode(), routing_key="games_reviews_queue_0")
        self.reviews_middleware.send(Fin(0, client_id).encode(), routing_key="games_reviews_action_queue_3")
        for i in range(1, self.amount_of_language_filters + 1):
            routing = f"games_reviews_action_queue_{i}_0"
            print("Enviando FIN - ", routing, flush=True)
            self.reviews_middleware.send(data=Fin(0, client_id).encode(), routing_key=routing)
        
        
    def handle_game_eof(self, message):
        """
        Maneja el mensaje de fin de juegos.
        """
        logging.info("Fin de la transmisión de juegos")
        fin = Fin.decode(message)

        client_id = int(fin.client_id)
        # print("Recibiendo EOF - ", type(client_id), flush=True)

        self.completed_games[client_id] = True
        if client_id not in self.batch_counter:
            self.batch_counter[client_id] = 0
        if client_id not in self.reviews_to_add:
            self.reviews_to_add[client_id] = []
        if client_id not in self.review_file_size:
            self.review_file_size[client_id] = 0
        if client_id not in self.nodes_completed:
            self.nodes_completed[client_id] = 0
        if client_id not in self.sended_fin:
            self.sended_fin[client_id] = False
        if client_id not in self.total_batches:
            self.total_batches[client_id] = 0
        if client_id not in self.completed_reviews:
            self.completed_reviews[client_id] = False

        if (
            self.completed_games[client_id]
            and self.completed_reviews[client_id]
            and not self.sended_fin[client_id]
        ):
            self.sended_fin[client_id] = True
            logging.info("Fin de la transmisión de datos")

            self.process_reviews(f"review_filter_{self.reviews_input_queue[0]}_{client_id}", client_id)
            self.send_fin(client_id)

    def save_last_reviews(self,client_id):
        """
        Guarda las reviews restantes en el archivo.
        """
        # print(" EOF Recibiendo- ", flush=True)
        self.fault_manager.append(f"review_filter_{self.reviews_input_queue[0]}_{client_id}", '\n'.join(self.reviews_to_add[client_id]))

        self.reviews_to_add[client_id] = []

    def handle_review_eof(self, message):
        """
        Maneja el mensaje de fin de reviews y asegura que todas las reviews se escriban en el archivo.
        """

        message_fin = Fin.decode(message)
        client_id = int(message_fin.client_id)
        if client_id not in self.batch_counter:
            self.batch_counter[client_id] = 0
        if client_id not in self.reviews_to_add:
            self.reviews_to_add[client_id] = []
        if client_id not in self.review_file_size:
            self.review_file_size[client_id] = 0
        if client_id not in self.nodes_completed:
            self.nodes_completed[client_id] = 0
        if client_id not in self.sended_fin:
            self.sended_fin[client_id] = False
        if client_id not in self.total_batches:
            self.total_batches[client_id] = 0
        if client_id not in self.completed_reviews:
            self.completed_reviews[client_id] = False
        self.completed_reviews[client_id] = True
        self.nodes_completed[client_id] += 1
        self.total_batches[client_id] = int(message_fin.batch_id)
        # print("Recibiendo EOF - counter nodes_completed", self.nodes_completed[client_id] , flush=True)
        # print("Recibiendo EOF - Client_id", client_id, flush=True)
        # print("Recibiendo EOF - TypeBatch_id", type(message_fin.batch_id), flush=True)
        # print("Recibiendo EOF - Batch_id", message_fin.batch_id, flush=True)
        # print("Recibiendo EOF - TypeTotalBatch", type(self.total_batches[client_id]), flush=True)
        # print("Recibiendo EOF - TotalBatch", self.total_batches[client_id], flush=True)
        

        if (self.nodes_completed[client_id] == self.previous_review_nodes) and not self.sended_fin[client_id]:
            if self.batch_counter[client_id] >= self.total_batches[client_id]:
                print(
                    "Fin de la transmisión de datos batches",
                    self.batch_counter[client_id],
                    flush=True,
                    
                )
                with self.file_lock:
                    self.save_last_reviews(client_id)

                    self.process_reviews(
                        f"review_filter_{self.reviews_input_queue[0]}_{client_id}",
                        message_fin.client_id,
                    )

                    self.send_fin(client_id)
                    self.sended_fin[client_id] = True
                    self.fault_manager.delete_key(f"game_filter_{self.reviews_input_queue[0]}_{client_id}")
                    self.fault_manager.delete_key(f"processed_packets_{self.reviews_input_queue[0]}")
                    
    def process_reviews(self, key, client_id):
        """
        Procesa las reviews y realiza el join con los juegos específicos del client_id.
        """
        client_games = self.games.get(int(client_id), {})
        batch_size = 200
        final_list = str(self.packet_id) + "\n"
        final_list_action = str(self.action_packet_id) + "\n"
        batch_counter = 0
        data = self.fault_manager.get(key)
        if data is None:
            return
        self.next_instance = 1
        lines = data.strip().split("\n")
        lines = lines[1:]   
        logging.info(f"[PROCESS REVIEW] Procesando reviews para cliente {client_id} - {self.action_packet_id} - {self.packet_id}")
        if "action" in self.games_input_queue[1].lower():
            self.fault_manager.update(f"processed_packets_{self.reviews_input_queue[0]}", json.dumps({"last_sended_packet": self.action_packet_id, "last_init_process_packet": self.action_packet_id}))
        else:
            self.fault_manager.update(f"processed_packets_{self.reviews_input_queue[0]}", json.dumps({"last_sended_packet": self.packet_id, "last_init_process_packet": self.packet_id}))
        initial_packet = self.action_packet_id
        indie_initial_packet = self.packet_id
        for line in lines:
            try: 
                json_data = json.loads(line)
            
                if isinstance(json_data, dict) and all(key in json_data for key in ["packet_id", "batch_counter", "total_batches"]):
                    continue
                review = Review.decode(json_data)
                #logging.info(f"Procesando review - {type(review.client_id)}(STR) - {type(review.game_id) (STR)}")
                if review.game_id in client_games:
                    #logging.info(f"Procesando review - {self.action_packet_id} - {self.packet_id}")
                    game = client_games[review.game_id]
                    if "action" in self.games_input_queue[1].lower():
                        game_review = GameReview(review.game_id, game, review.review_text, review.client_id)
                        game_str = json.dumps(game_review.getData())
                        #data_to_send = f"{self.action_packet_id}\n{game_str}\n"
                        final_list_action += f"{game_str}\n"
                        batch_counter += 1
                        if (batch_counter >= batch_size):
                            routing = f"games_reviews_action_queue_{self.next_instance}_0"
                            self.reviews_middleware.send(data=final_list_action,routing_key=routing) # language filter
                            self.reviews_middleware.send(data=final_list_action,routing_key="games_reviews_action_queue_3") # Percentil directo
                            self.fault_manager.update(f"processed_packets_{self.reviews_input_queue[0]}", json.dumps({"last_sended_packet": self.action_packet_id, "last_init_process_packet": initial_packet}))
                            self.action_packet_id += 1
                            self.next_instance = (self.next_instance % self.amount_of_language_filters) + 1
                            final_list_action = str(self.action_packet_id) + "\n"
                            batch_counter = 0                    
                    else:
                        game_review = GameReview(review.game_id, game, None, review.client_id)
                        game_str = json.dumps(game_review.getData())
                        final_list += f"{game_str}\n"
                        batch_counter += 1
                        if (batch_counter >= batch_size):
                            logging.info(f"Enviando paquete {self.packet_id}")
                            self.reviews_middleware.send(final_list, routing_key="games_reviews_queue_0")
                            self.fault_manager.update(f"processed_packets_{self.reviews_input_queue[0]}", json.dumps({"last_sended_packet": self.packet_id, "last_init_process_packet": indie_initial_packet}))
                            self.packet_id += 4
                            final_list = str(self.packet_id) + "\n"
                            batch_counter = 0
                else:
                    pass
            except json.JSONDecodeError as e:
                logging.error(f"Error al procesar la línea '{line}': {e}")
        
        if final_list and "action" not in self.games_input_queue[1].lower():
            self.reviews_middleware.send(final_list, routing_key="games_reviews_queue_0")
            self.fault_manager.update(f"processed_packets_{self.reviews_input_queue[0]}", json.dumps({"last_sended_packet": self.packet_id, "last_init_process_packet": indie_initial_packet}))
            self.packet_id += 4
        if final_list_action and "action" in self.games_input_queue[1].lower():
            routing = f"games_reviews_action_queue_{self.next_instance}_0"
            self.reviews_middleware.send(data=final_list_action,routing_key=routing) # language filter
            self.reviews_middleware.send(data=final_list_action,routing_key="games_reviews_action_queue_3")
            self.fault_manager.update(f"processed_packets_{self.reviews_input_queue[0]}", json.dumps({"last_sended_packet": self.action_packet_id, "last_init_process_packet": initial_packet}))
            self.action_packet_id += 1
            
        self.fault_manager.delete_key(f"review_filter_{self.reviews_input_queue[0]}_{client_id}")

        if "action" in self.games_input_queue[1].lower():
            self.fault_manager.update(f"processed_packets_{self.reviews_input_queue[0]}", json.dumps({"last_sended_packet": self.action_packet_id, "last_init_process_packet": self.action_packet_id}))
        else:
            self.fault_manager.update(f"processed_packets_{self.reviews_input_queue[0]}", json.dumps({"last_sended_packet": self.packet_id, "last_init_process_packet": self.packet_id}))
        logging.info(f"[PROCESS REVIEW] Reviews procesadas para cliente {client_id} - {self.action_packet_id} - {self.packet_id}")
    def start(self):
        """
        Inicia el proceso de join.
        """
        logging.info("Iniciando GameReviewFilter...")
        self.games_receiver.start()
        self.reviews_receiver.start()
        logging.info("GameReviewFilter finalizado.")
