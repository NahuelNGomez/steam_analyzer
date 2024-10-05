import json
import logging
from datetime import datetime
import re

from common.middleware import Middleware

RELEASE_DATE_POSITION = 2


def split_complex_string(s):
    pattern = r'''
        (?:\[.*?\])   # Captura arrays entre corchetes
        |             # O
        (?:".*?")     # Captura texto entre comillas dobles
        |             # O
        (?:'.*?')     # Captura texto entre comillas simples
        |             # O
        (?:[^,]+)     # Captura cualquier cosa que no sea una coma
    '''
    return re.findall(pattern, s, re.VERBOSE)

class RangeFilter:
    def __init__(self, start_year, end_year, input_queues, output_exchanges, instance_id):
        self.middleware = Middleware(input_queues, [], output_exchanges, instance_id, self._callBack, self._finCallBack)
        
        self.start_year = start_year
        self.end_year = end_year
        
        
    def _callBack(self, data):
        message =split_complex_string(data)
        logging.debug(f"Mensaje decodificado: {message}")

        filtered_game = self.filter_by_range(message)
        if filtered_game:
            self.middleware.send(','.join(filtered_game))
            logging.info(f"Juego filtrado enviado:{filtered_game}")
        else:
            logging.info("Juego no cumple con el filtro de rango.")
            print("Juego no cumple con el filtro de rango.", flush=True)

    def filter_by_range(self, message):
        """
        Filtra juegos publicados entre start_year y end_year.
        """
        try:
            release_date_str = message[RELEASE_DATE_POSITION]
            release_date_str = release_date_str[1:-1]
            release_year = self.extract_year(release_date_str)
            if self.start_year <= release_year <= self.end_year:
                return message
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
        