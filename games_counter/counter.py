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
            
            # Mostrar los valores decodificados para Windows, Mac y Linux
            print(f"Juego: {game_name}, Valores Originales -> Windows: {windows}, Mac: {mac}, Linux: {linux}", flush=True)
            logging.info(f"Juego: {game_name}, Valores Originales -> Windows: {windows}, Mac: {mac}, Linux: {linux}")

            # Convertir a booleano de forma robusta
            windows = self._convert_to_boolean(windows)
            mac = self._convert_to_boolean(mac)
            linux = self._convert_to_boolean(linux)

            # Mostrar los valores convertidos para Windows, Mac y Linux
            print(f"Juego: {game_name}, Valores Convertidos -> Windows: {windows}, Mac: {mac}, Linux: {linux}", flush=True)
            logging.info(f"Juego: {game_name}, Valores Convertidos -> Windows: {windows}, Mac: {mac}, Linux: {linux}")

            # Verificar si los valores ya eran booleanos
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
                json_row = json.loads(row)
                game = Game.decode(json_row)
                logging.debug(f"Mensaje decodificado: {game}")
                self.counterGames(game)
                
        except Exception as e:
            logging.error(f"Error en _callBack al procesar el mensaje: {e}")
    
    def _finCallBack(self, data):
        """
        Callback para manejar el mensaje de fin.
        """
        response = {
            "supported_platforms": [
                {"platform": "Windows", "game_count": self.platform_counts['Windows']},
                {"platform": "Mac", "game_count": self.platform_counts['Mac']},
                {"platform": "Linux", "game_count": self.platform_counts['Linux']}
            ],
            "generated_at": datetime.utcnow().isoformat() + "Z"
        }
        self.middleware.send(json.dumps(response, indent=4))
        #self.middleware.send("Respuesta del contador de juegos enviada.")
    
    def start(self):
        """
        Inicia el middleware.
        """
        self.middleware.start()
