import json
import logging
import os
import threading
from common.middleware import Middleware
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
        self.requeued_reviews = []

        self.games: dict = {}

        self.games_receiver = threading.Thread(target=self._games_receiver)
        self.reviews_receiver = threading.Thread(target=self._reviews_receiver)

    def saveGameInTxt(self, game):
        """
        Guarda el juego en la base de datos.
        """
        with open("data/gamesData.txt", "a") as file:
            file.write(game + "\n")

    def saveReviewInTxt(self, review):
        """
        Guarda el juego en la base de datos.
        """
        
        with open("data/reviewsData.txt", "a") as file:
            file.write(review + "\n")

    def _games_receiver(self):
        self.books_middleware = Middleware(
            input_queues={self.games_input_queue[0]: self.games_input_queue[1]},
            callback=self._add_game,
            eofCallback=self.handle_game_eof,
            output_queues=self.output_queues,  ## ???
            output_exchanges=self.output_exchanges,  ## ???
            intance_id=self.instance_id,
        )
        self.books_middleware.start()

    def _reviews_receiver(self):
        self.reviews_middleware = Middleware(
            input_queues={self.reviews_input_queue[0]: self.reviews_input_queue[1]},
            callback=self._add_review,
            eofCallback=self.handle_review_eof,
            output_queues=self.output_queues,  ## ???
            output_exchanges=self.output_exchanges,  ## ???
            intance_id=self.instance_id,
        )
        self.reviews_middleware.start()

    def _add_game(self, game):
        """
        Agrega un juego al diccionario de juegos.
        """
        try:
            game = split_complex_string(game)
            self.games[game[0]] = game[1]
        except Exception as e:
            logging.error(f"Error al agregar juego: {e}")

    def handle_game_eof(self, message):
        """
        Maneja el mensaje de fin de juegos.
        """
        print("Fin de la transmisión de juegos", flush=True)
        self.completed_games = True
        
        if (self.requeued_reviews == []) and self.completed_games and self.completed_reviews:
            print("Fin de la transmisión de reviews", flush=True)
            self.reviews_middleware.send("fin\n\n")
        
    def _add_review(self, review):
        review_list = split_complex_string(review)
        if review_list[0] in self.games:
            self.saveReviewInTxt(review)
            game = self.games[review_list[0]]
            value = ','.join(i for i in self.games if self.games[i] == game)
            if ('action' in self.games_input_queue[1] ):
                self.reviews_middleware.send(value + "," + game + "," + review_list[2] + "," + review_list[3] + "," + review_list[4] + "," + review_list[4]) #+ "," + review_list[5])
            else:
                self.reviews_middleware.send(value + "," + game)
        
            #self.reviews_middleware.send(value + "," + game)
            if len(review_list) == 6 and review_list[5] in self.requeued_reviews:
                self.requeued_reviews.remove(review_list[5])
            
        else:
            if not self.completed_games:
                if len(review_list) == 6 and (review_list[5] not in self.requeued_reviews):
                    self.requeued_reviews.append(review_list[5])
                print(f"Juego no encontrado: {review_list[0]}Reencolando {self.requeued_reviews}", flush=True)
                if ('action' in self.games_input_queue[1] ):
                    return 3
                else:
                    return 2
            
            print(f"Juego no encontrado:- Se descarta {self.requeued_reviews}", flush=True)
            if len(review_list) == 6 and (review_list[5] in self.requeued_reviews):
                self.requeued_reviews.remove(review_list[5])
                
        if (self.requeued_reviews == []) and self.completed_games and self.completed_reviews:
            print("Fin de la transmisión de reviews", flush=True)
            self.reviews_middleware.send("fin\n\n")

    def handle_review_eof(self, message):
        self.completed_reviews = True
        if (self.requeued_reviews == []) and self.completed_games and self.completed_reviews:
            print("Fin de la transmisión de reviews", flush=True)
            self.reviews_middleware.send("fin\n\n")

    def start(self):
        """
        Inicia el proceso de join.
        """
        logging.info("Iniciando GameReviewFilter...")
        self.games_receiver.start()
        self.reviews_receiver.start()
        logging.info("GameReviewFilter finalizado.")