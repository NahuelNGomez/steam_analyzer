import json
import logging
from collections import defaultdict
from common.middleware import Middleware
from common.utils import split_complex_string
from common.constants import REVIEWS_APP_ID_POS, REVIEWS_TEXT_POS

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

    def start(self):
        """
        Inicia el acumulador.
        """
        self.middleware.start()
        logging.info("GameNamesAccumulator started")
    
    def process_game(self, message):
        """
        # Processes each message (game) received and adds it to the top 10 list if applicable.
        # """
        try:
            print("Games dict: ", self.games, flush=True)
            game_id = message[REVIEWS_APP_ID_POS]
            print(f"Game ID: {game_id}", flush=True)
            text = message[REVIEWS_TEXT_POS][1:-1]
            print(f"Game text: {text}", flush=True)
            if game_id in self.games:
                self.games[game_id]['count'] += 1
            else:
                if self.games[game_id] not in self.sent_games:
                    self.games[game_id] = {
                        'name': message[1][1:-1],
                        'count': 1
                    }

            if self.games[game_id]['count'] >= self.reviews_low_limit and game_id not in self.sent_games:
                self.middleware.send(json.dumps(self.games[game_id]))
                self.sent_games.append(game_id)
                self.games.pop(game_id)
                print(f"Game sent: {game_id}", flush=True)
            
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
        self.middleware.send(data)
    
    def _callBack(self, data):
        """
        Callback para procesar los mensajes recibidos.

        :param data: Datos recibidos.
        """
        try:
            result = split_complex_string(data)
            logging.debug(f"Mensaje decodificado: {result}")
            self.process_game(result)
            
        except Exception as e:
            logging.error(f"Error en GameNamesAccumulator callback: {e}")
            