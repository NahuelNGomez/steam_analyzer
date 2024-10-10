import json
import logging
import os
import threading
from common.game import Game
from common.game_review import GameReview
from common.middleware import Middleware
from common.review import Review
from common.utils import split_complex_string


class GameReviewFilter:
    def __init__(self, input_games_queue, input_reviews_queue, output_exchanges, output_queues, instance_id):
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
        self.completed_games = False
        self.completed_reviews = False
        self.sended_fin = False
        self.reviews_to_add = []
        #self.requeued_reviews = []

        self.games: dict = {}

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
        
        
    def saveReviewInTxt(self, review):
        """
        Guarda el juego en la base de datos.
        """
        # name = "data/reviewsData" + str(os.getpid()) + ".txt"
        # with open("data/reviewsData.txt", "a") as file:
        
    def _add_game(self, game):
        """
        Agrega un juego al diccionario de juegos.
        """
        try:
            game = Game.decode(json.loads(game))
            self.games[game.id] = game.name
        except Exception as e:
            logging.error(f"Error al agregar juego: {e}")

    def handle_game_eof(self, message):
        """
        Maneja el mensaje de fin de juegos.
        """
        print("Fin de la transmisión de juegos", flush=True)
        self.completed_games = True
        
        if self.completed_games and self.completed_reviews and not self.sended_fin:
            self.sended_fin = True
            print("Fin de la transmisión de datos", flush=True)
            self.process_reviews()
            self.reviews_middleware.send("fin\n\n")
            
    def _add_review(self, review):
        review_cleaned = review.replace('\x00', '')
        self.reviews_to_add.append(review_cleaned)
        if len(self.reviews_to_add) >= 1000:
            name = "data/reviewsData" + self.reviews_input_queue[0] + ".txt"
            with open(name, "a") as file:
                for review_cleaned in self.reviews_to_add:
                    file.write(review_cleaned + "\n")

            self.reviews_to_add = []

    def handle_review_eof(self, message):
        self.completed_reviews = True
        
        name = "data/reviewsData" + self.reviews_input_queue[0] + ".txt"
        with open(name, "a") as file:
            for review in self.reviews_to_add:
                file.write(review + "\n")
        self.reviews_to_add = []
        
        if self.completed_games and self.completed_reviews and not self.sended_fin:
            self.sended_fin = True
            print("Fin de la transmisión de reviews", flush=True)
            print("Fin de la transmisión de datos", flush=True)
            self.process_reviews()
            self.reviews_middleware.send("fin\n\n")
        
    def process_reviews(self):
        name = "data/reviewsData" + self.reviews_input_queue[0] + ".txt"
        with open(name, "r") as file:
            for line in file:
                review = Review.decode(json.loads(line))
                if review.game_id in self.games:
                    game = self.games[review.game_id]
                    if ('action' in self.games_input_queue[1]):
                        game_review = GameReview(review.game_id, game, review.review_text)
                        game_str = json.dumps(game_review.getData())
                        self.reviews_middleware.send(game_str)
                    else:
                        game_review = GameReview(review.game_id, game, None)
                        game_str = json.dumps(game_review.getData())
                        self.reviews_middleware.send(game_str)
                else:
                    print(f"Juego no encontrado: {review.game_id}Descartado ", flush=True)
        print("Fin de la ejecución de reviews", flush=True)

    def start(self):
        """
        Inicia el proceso de join.
        """
        logging.info("Iniciando GameReviewFilter...")
        self.games_receiver.start()
        self.reviews_receiver.start()
        logging.info("GameReviewFilter finalizado.")