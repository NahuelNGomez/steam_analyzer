# games_filter/filter.py

import csv
import io
import logging
from collections import defaultdict
from common.game import Game
from common.middleware import Middleware
from common.utils import split_complex_string
from common.constants import GAMES_NAME_POS, GAMES_WINDOWS_POS, GAMES_MAC_POS, GAMES_LINUX_POS
from datetime import datetime
import json

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
        self.total_games = 0

    def counterGames(self, game):
        try:
            game_name = game.name
            windows = game.windows
            mac = game.mac
            linux = game.linux
            self.total_games += 1
            print(f"Total de juegos: {self.total_games}", flush=True)
            
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
            batch = data.split('\n')
            for row in batch:
                json_row = json.loads(row)
                game = Game.decode(json_row)
                print("result: ", game, flush=True)
                logging.debug(f"Mensaje decodificado: {game}")
                self.counterGames(game)
                
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