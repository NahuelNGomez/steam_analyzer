# games_filter/filter.py

import csv
import io
import logging
from collections import defaultdict
from common.game import Game
from common.middleware import Middleware
from common.utils import split_complex_string
from common.packet_fin import Fin
from common.constants import GAMES_NAME_POS, GAMES_WINDOWS_POS, GAMES_MAC_POS, GAMES_LINUX_POS
from datetime import datetime
import json
class GamesCounter:
    def __init__(self, input_queues, output_exchanges, instance_id):
        #self.platform_counts = defaultdict(int)
        self.platform_counts = defaultdict(lambda: {'Windows': 0, 'Mac': 0, 'Linux': 0})
        self.middleware = Middleware(input_queues, [], output_exchanges, instance_id, self._callBack, self._finCallBack)

    def counterGames(self, game):
        try:
            game_name = game.name
            windows = game.windows
            mac = game.mac
            linux = game.linux
            client_id = game.client_id
            logging.info(f"Juego '{game_name}' recibido del cliente {client_id}")
            
            # Convertir a booleano de forma robusta
            windows = self._convert_to_boolean(windows)
            mac = self._convert_to_boolean(mac)
            linux = self._convert_to_boolean(linux)

            # Verificar si los valores ya eran booleanos
            if windows:
                self.platform_counts[client_id]['Windows'] += 1
            if mac:
                self.platform_counts[client_id]['Mac'] += 1
                #logging.info(f"Juego '{game_name}' soporta Mac.")
            if linux:
                self.platform_counts[client_id]['Linux'] += 1
                #logging.info(f"Juego '{game_name}' soporta Linux.")

        except Exception as e:
            logging.error(f"Error al filtrar el juego '{game_name}': {e}")
    
    def _convert_to_boolean(self, value):
        """
        Convierte un valor a booleano, considerando posibles valores de entrada como str o bool.
        """
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.strip().lower() in ['true', '1']
        return False

    def _callBack(self, data):
        """
        Callback para procesar los mensajes recibidos.

        :param data: Datos recibidos.
        """
        try:
            batch = data.split('\n')
            for row in batch:
                try:
                    json_row = json.loads(row)
                    game = Game.decode(json_row)
                    self.counterGames(game)
                except Exception as e:
                    logging.error(f"Error al procesar la fila '{row}': {e}")
                
        except Exception as e:
            logging.error(f"Error en _callBack al procesar el mensaje: {e}")
    
    def _finCallBack(self, data):
        """
        Callback para manejar el mensaje de fin.
        """
        try:
            fin = Fin.decode(data)
            client_id = int(fin.client_id)
            logging.info(f"Fin de la transmisión de juegos del cliente {client_id}")
            response = {
                "supported_platforms": {
                    "client id " + str(client_id): {
                        "Windows": self.platform_counts[client_id]['Windows'],
                        "Mac": self.platform_counts[client_id]['Mac'],
                        "Linux": (self.platform_counts[client_id]['Linux'])
                    }
                }
            }
            self.middleware.send(json.dumps(response, indent=4))
            #self.middleware.send("Respuesta del contador de juegos enviada.")
        except Exception as e:
            logging.error(f"Error al procesar el mensaje de fin: {e}")
    
    def start(self):
        """
        Inicia el middleware.
        """
        self.middleware.start()
