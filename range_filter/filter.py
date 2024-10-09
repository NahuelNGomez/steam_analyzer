import json
import logging
from datetime import datetime
from common.game import Game
from common.middleware import Middleware
from common.utils import split_complex_string
from common.constants import GAMES_RELEASE_DATE_POS

class RangeFilter:
    def __init__(self, start_year, end_year, input_queues, output_exchanges, instance_id):
        self.middleware = Middleware(input_queues, [], output_exchanges, instance_id, self._callBack, self._finCallBack)
        
        self.start_year = start_year
        self.end_year = end_year
        
    def _callBack(self, data):
        json_row = json.loads(data)
        game = Game.decode(json_row)
        logging.debug(f"Mensaje decodificado: {game}")

        filtered_game = self.filter_by_range(game)
        if filtered_game:
            self.middleware.send(','.join(filtered_game))
            logging.info(f"Juego filtrado enviado:{filtered_game}")
        else:
            logging.info("Juego no cumple con el filtro de rango.")
            print("Juego no cumple con el filtro de rango.", flush=True)

    def filter_by_range(self, game):
        """
        Filtra juegos publicados entre start_year y end_year.
        """
        try:
            release_date_str = game.release_date
            release_year = self.extract_year(release_date_str)
            if self.start_year <= release_year <= self.end_year:
                return game
            return None
        except Exception as e:
            logging.error(f"Error en filter_by_range: {e}")
            return None

    def extract_year(self, release_date_str):
        """
        Extrae el año de una cadena de fecha en formato "MMM DD, YYYY" o "MMMM DD, YYYY".
        """
        try:
            release_date = datetime.strptime(release_date_str, "%b %d, %Y")
            return release_date.year
        except ValueError:
            release_date = datetime.strptime(release_date_str, "%B %d, %Y")
            return release_date.year
        
    def _finCallBack(self, data):
        print("rangeFilter sending data: ", data, flush=True)
        self.middleware.send(data = data)
        

    def start(self):
        self.middleware.start()
        