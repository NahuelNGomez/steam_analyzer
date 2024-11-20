import json
import logging
from collections import defaultdict
from common.game_review import GameReview
from common.middleware import Middleware
from common.healthcheck import HealthCheckServer

class GameNamesAccumulator:
    def __init__(self, input_queues, output_exchanges, instance_id, reviews_low_limit):
        """
        Inicializa el acumulador de nombres de juegos con los parámetros especificados.

        :param input_queues: Diccionario de colas de entrada.
        :param output_exchanges: Lista de exchanges de salida.
        :param instance_id: ID de instancia para identificar colas únicas.
        """
        self.games = defaultdict(int)
        self.sent_games = []
        self.reviews_low_limit = reviews_low_limit
        self.middleware = Middleware(input_queues, [], output_exchanges, instance_id, 
        self._callBack, self._finCallBack)
        self.datasent = False
        self.counter = 0
        self.review_batches = []
        

    def start(self):
        """
        Inicia el acumulador.
        """
        HealthCheckServer().start_in_thread()
        self.middleware.start()
        logging.info("GameNamesAccumulator started")
    
    def process_game(self, game):
        """
        Procesa cada mensaje (juego) recibido y acumula las reseñas positivas y negativas.
        Si el número de reseñas supera el límite definido, envía el nombre del juego.
        """
        try:
            self.counter += 1
            game_id = game.game_id
            if game_id in self.games:
                self.games[game_id]['count'] += 1
            else:
                if self.games[game_id] not in self.sent_games:
                    self.games[game_id] = {
                        'name': game.game_name,
                        'count': 1
                    }
                
            if self.games[game_id]['count'] > self.reviews_low_limit and game_id not in self.sent_games:
                self.middleware.send(json.dumps(message))
                self.sent_games.append(game_id)
                self.games.pop(game_id)
                self.datasent = True

        except Exception as e:
            logging.error(f"Error in process_game: {e}")
    
    def get_games(self):
        """
        Obtiene los juegos acumulados.

        :return: Diccionario de juegos acumulados.
        """
        return self.games
    
    def _finCallBack(self, data):
        """
        Callback para manejar el mensaje de fin.

        :param data: Datos recibidos.
        """
        logging.info("Fin de la transmisión, enviando data")
        self.middleware.send(json.dumps(data))
    
    def _callBack(self, data):
        """
        Callback para procesar los mensajes recibidos.

        :param data: Datos recibidos.
        """
        try:
            game = GameReview.decode(json.loads(data))
            logging.debug(f"Mensaje decodificado: {game}")
            self.process_game(game)
            
        except Exception as e:
            logging.error(f"Error en GameNamesAccumulator callback: {e}")
            
