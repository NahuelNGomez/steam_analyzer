# games_filter/filter.py

import csv
import io
import logging
from collections import defaultdict
import re
from common.middleware import Middleware
from datetime import datetime
import json

# POSITIONS - HARDCODE? - NO DEBERIA PASARLE COLUMNAS QUE NO USA
NAME_POSITON = 1
WINDOWS_POSITION = 16
MAC_POSITION = 17
LINUX_POSITION = 18


def split_complex_string(s):
    # Usamos una expresión regular que captura comas, pero no dentro de arrays [] ni dentro de comillas
    # Esto identifica bloques entre comillas o corchetes como un solo token
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

class GamesCounter:
    def __init__(self, input_queues, output_exchanges, instance_id):
        """
        Inicializa el contador de juegos.
        
        :param input_queues: Diccionario de colas de entrada.
        :param output_exchanges: Lista de exchanges de salida.
        :param instance_id: ID de instancia para identificar colas únicas.
        """
        self.platform_counts = defaultdict(int)
        self.middleware = Middleware(input_queues, [], output_exchanges, instance_id, self._callBack, self._finCallBack)


    def counterGames(self, game):
        try:
            game_name = game[NAME_POSITON]
            windows = game[WINDOWS_POSITION]
            mac = game[MAC_POSITION]
            linux = game[LINUX_POSITION]
            
            print(f"Juego: {game_name}, Windows: {windows}, Mac: {mac}, Linux: {linux}", flush=True)

            if isinstance(windows, str):
                windows = windows.lower() == 'true'
            if isinstance(mac, str):
                mac = mac.lower() == 'true'
            if isinstance(linux, str):
                linux = linux.lower() == 'true'

            if windows:
                self.platform_counts['Windows'] += 1
                logging.info(f"Juego '{game_name}' soporta Windows.")
            if mac:
                self.platform_counts['Mac'] += 1
                logging.info(f"Juego '{game_name}' soporta Mac.")
            if linux:
                self.platform_counts['Linux'] += 1
                logging.info(f"Juego '{game_name}' soporta Linux.")

            logging.info(f"Conteo Actual: {dict(self.platform_counts)}")
        except Exception as e:
            logging.error(f"Error al filtrar el juego: {e}")
    
    def get_platform_counts(self):
        return str(dict(self.platform_counts))

    def _callBack(self, data):
        """
        Callback para procesar los mensajes recibidos.

        :param data: Datos recibidos.
        """
        try:
            result =split_complex_string(data)
            
            print("result: ", result, flush=True)
            logging.debug(f"Mensaje decodificado: {result}")

            self.counterGames(result)
        except Exception as e:
            logging.error(f"Error en _callBack al procesar el mensaje: {e}")
    
    def _finCallBack(self, data):
        """
        Callback para manejar el mensaje de fin.

        :param data: Datos recibidos.
        """
        print("gamesCounter sending data: ", self.platform_counts, flush=True)
        self.middleware.send(json.dumps(self.platform_counts))
    
    
    def start(self):
        """
        Inicia el middleware.
        """
        self.middleware.start()