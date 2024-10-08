import json
import logging
import os
from common.middleware import Middleware
from common.utils import split_complex_string

class GameReviewFilter:
    def __init__(self, input_queues, output_queue, instance_id):
        """
        :param input_queues: Lista de colas de entrada (e.g., ['shooter_games_queue', 'positive_reviews_queue']).
        :param output_queue: Cola de salida (e.g., 'shooter_games_positive_reviews_queue').
        :param join_key: Clave para realizar el join (por defecto, 'app_id').
        """
        self.join_key='app_id'
        self.middleware = Middleware(input_queues, [], output_queue, instance_id, self._callback, self._finCallback)

    def saveGameInTxt(self, game):
        """
        Guarda el juego en la base de datos.
        """
        with open('data/gamesData.txt', 'a') as file:
            file.write(game + '\n')
            
    def start(self):
        """
        Inicia el proceso de join.
        """
        logging.info("Iniciando GameReviewFilter...")
        self.middleware.start()
        logging.info("GameReviewFilter finalizado.")

    def _callback(self, message):
        """
        Callback para procesar los mensajes.
        """
        try:
            result = split_complex_string(message)
            if len(result) > 5: 
                self.saveGameInTxt(message) 
      
        except Exception as e:
            logging.error(f"Error en GameReviewFilter callback: {e}")
    
    def _finCallback(self, message):
        """
        Callback para finalizar el proceso.
        """
        self.middleware.send(message)
        logging.info("GameReviewFilter finalizado.")