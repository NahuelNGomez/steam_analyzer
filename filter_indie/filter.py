# filter_indie/filter.py

import json
import logging
from collections import defaultdict
from common.middleware import Middleware

class FilterIndie:
    def __init__(self, input_queues, output_exchanges, instance_id):
            self.middleware = Middleware(input_queues, [], output_exchanges, instance_id, self._callBack, self._finCallBack)

    def filter_indie_games(self, message):
        """
        Filtra juegos que tienen el género 'Indie'.
        """
        try:
            genres_str = message.get('Genres', '')
            genres = [genre.strip() for genre in genres_str.split(',')]
            if 'Indie' in genres:
                return message
            return None
        except Exception as e:
            logging.error(f"Error en filter_indie_games: {e}")
            return None

    def _finCallBack(self, data):
        self.middleware.send(data = data)
        
    def _callBack(self, data):
        message = json.loads(data)
        logging.debug(f"Mensaje decodificado: {message}")

        filtered_game = self.filter_indie_games(message)
        if filtered_game:
            self.middleware.send(json.dumps(filtered_game))
            logging.info(f"Juego filtrado enviado:{filtered_game}")
        else:
            logging.info("Juego no cumple con el filtro Indie.")
            print("Juego no cumple con el filtro Indie.", flush=True)
            
    def start(self):
        self.middleware.start()