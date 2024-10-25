import json
import logging
import os
import threading
from common.game import Game
from common.game_review import GameReview
from common.middleware import Middleware
from common.packet_fin import Fin
from common.review import Review
from common.utils import split_complex_string


class GameReviewFilter:
    def __init__(self, input_games_queue, input_reviews_queue, output_exchanges, output_queues, instance_id, previous_review_nodes):
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
        self.completed_reviews = False
        self.sended_fin = False
        self.reviews_to_add = []
        self.previous_review_nodes = previous_review_nodes
        self.nodes_completed = 0
        self.review_file = 0
        self.review_file_size = 0
        self.batch_counter = 0
        self.total_batches = 0 

        self.games: dict = {}
        
        self.file_lock = threading.Lock()

        self.games_receiver = threading.Thread(target=self._games_receiver)
        self.reviews_receiver = threading.Thread(target=self._reviews_receiver)

    def _games_receiver(self):
        self.books_middleware = Middleware(
            input_queues={self.games_input_queue[0]: self.games_input_queue[1]},
            callback=self._add_game,
            eofCallback=self.handle_game_eof,
            output_queues=self.output_queues,  ## ???
            output_exchanges=self.output_exchanges,  ## ???
            intance_id=self.instance_id
        )
        self.books_middleware.start()

    def _reviews_receiver(self):
        self.reviews_middleware = Middleware(
            input_queues={self.reviews_input_queue[0]: self.reviews_input_queue[1]},
            callback=self._add_review,
            eofCallback=self.handle_review_eof,
            output_queues=self.output_queues,  ## ???
            output_exchanges=self.output_exchanges,  ## ???
            intance_id=self.instance_id
        )
        self.reviews_middleware.start()
        
        
    def _add_game(self, game):
        """
        Agrega un juego al diccionario de juegos, organizados por client_id.
        """
        try:
            game = Game.decode(json.loads(game))
            print("Recibiendo GAME - client_id:", type(game.client_id), "game_id:", type(game.id), flush=True)
            client_id = game.client_id
            if client_id not in self.games:
                self.games[client_id] = {}
            self.games[client_id][str(game.id)] = game.name
        except Exception as e:
            logging.error(f"Error al agregar juego para cliente {game.client_id}: {e}")
            
    def _add_review(self, message):
        """
        Agrega una review a la lista y escribe en el archivo cuando llega a 1000.
        """
        batch = message.split("\n")
        self.batch_counter += 1
        with self.file_lock:
            for row in batch:
                if not row.strip():
                    continue
                self.reviews_to_add.append(row)
                if len(self.reviews_to_add) >= 3000:
                    name = "../data/reviewsData" + self.reviews_input_queue[0] + ".txt"
                    with open(name, "a") as file:
                        for review_cleaned in self.reviews_to_add:
                            file.write(review_cleaned + "\n")
                    self.reviews_to_add = []
                    self.review_file_size += 1
                if self.review_file_size >= 70:
                    print("Procesando reviews", flush=True)
                    self.review_file_size = 0
                    self.process_reviews("../data/reviewsData" + self.reviews_input_queue[0] + ".txt", Review.decode(json.loads(row)).client_id)
        if (self.nodes_completed == self.previous_review_nodes) and not self.sended_fin:
            if self.batch_counter == self.total_batches and self.sended_fin == False:
                print("Fin de la transmisión de datos batches", self.batch_counter, flush=True)
                with self.file_lock:
                    self.save_last_reviews()
                    self.process_reviews("../data/reviewsData" + self.reviews_input_queue[0] + ".txt", Review.decode(json.loads(batch[0])).client_id)
                    self.reviews_middleware.send(Fin(0,Review.decode(json.loads(batch[0])).client_id)) # Cambiar cuando review tenga el id del client
        
                self.sended_fin = True
                



    def handle_game_eof(self, message):
        """
        Maneja el mensaje de fin de juegos.
        """
        logging.info("Fin de la transmisión de juegos")
        fin = Fin.decode(message)
        
        client_id = int(fin.client_id)
        print("Recibiendo EOF - ",type(client_id),flush=True)

        self.completed_games[client_id] = True
        
        if self.completed_games[client_id] and self.completed_reviews and not self.sended_fin:
            self.sended_fin = True
            logging.info("Fin de la transmisión de datos")
            self.process_reviews("../data/reviewsData" + self.reviews_input_queue[0] + ".txt", fin.client_id)
            self.reviews_middleware.send(message)
    
    def save_last_reviews(self):
        """
        Guarda las reviews restantes en el archivo.
        """
        print(" EOF Recibiendo- ",flush=True)
        name = "../data/reviewsData" + self.reviews_input_queue[0] + ".txt"
        with open(name, "a") as file:
            for review in self.reviews_to_add:
                file.write(review + "\n")
        self.reviews_to_add = []
    
    
    def handle_review_eof(self, message):
        """
        Maneja el mensaje de fin de reviews y asegura que todas las reviews se escriban en el archivo.
        """
        self.completed_reviews = True
        self.nodes_completed += 1
        
        message_fin = Fin.decode(message)
        self.total_batches = int(message_fin.batch_id)
        
        print("Recibiendo EOF - ",self.total_batches,flush=True)
        if (self.nodes_completed == self.previous_review_nodes) and not self.sended_fin:
            if self.batch_counter == self.total_batches:
                print("Fin de la transmisión de datos batches", self.batch_counter, flush=True)
                with self.file_lock:
                    self.save_last_reviews()
                    self.process_reviews("../data/reviewsData" + self.reviews_input_queue[0] + ".txt", message_fin.client_id)
                    self.reviews_middleware.send(Fin(self.total_batches, message_fin.client_id).encode())
        
                self.sended_fin = True
        
        
    def process_reviews(self, path, client_id): 
        """
        Procesa las reviews y realiza el join con los juegos específicos del client_id.
        """
        client_games = self.games.get(int(client_id), {})
        print("Juegos para el cliente", client_id, ":", client_games, flush=True)  # Verificar contenido

        name = path
        with open(name, "r") as file:
            for line in file:
                review = Review.decode(json.loads(line))
                print("Recibiendo REVIEW - review.game_id:", type(review.game_id), "valor:", review.game_id, flush=True)
                
                if review.game_id in client_games:
                    print("Procesando review para game_id:", review.game_id, flush=True)
                    game = client_games[review.game_id]
                    
                    if 'action' in self.games_input_queue[1].lower():
                        game_review = GameReview(review.game_id, game, review.review_text, review.client_id)
                    else:
                        game_review = GameReview(review.game_id, game, None, review.client_id)
                    
                    game_str = json.dumps(game_review.getData())
                    self.reviews_middleware.send(game_str)
                else:
                    print(f"Review no encontrada para game_id {review.game_id} en client {client_id}", flush=True)
                        
        logging.info("Fin de la ejecución de reviews para el cliente %s", client_id)
        os.remove(name)
        
    def start(self):
        """
        Inicia el proceso de join.
        """
        logging.info("Iniciando GameReviewFilter...")
        self.games_receiver.start()
        self.reviews_receiver.start()
        logging.info("GameReviewFilter finalizado.")