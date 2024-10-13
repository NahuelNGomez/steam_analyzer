import json
import logging
from collections import defaultdict
from common.game import Game
from common.middleware import Middleware
from common.utils import split_complex_string
from common.constants import GAMES_GENRES_POS

class GenreFilter:
    def __init__(self, input_queues, output_exchanges, instance_id, genre):
        """
        Inicializa el filtro con el género especificado.

        :param input_queues: Diccionario de colas de entrada.
        :param output_exchanges: Lista de exchanges de salida.
        :param instance_id: ID de instancia para identificar colas únicas.
        :param genre: Género por el cual filtrar los juegos.
        """
        self.genre = genre
        self.middleware = Middleware(input_queues, [], output_exchanges, instance_id, self._callBack, self._finCallBack)

    def filter_games_by_genre(self, message):
        """
        Filtra juegos que tienen el género especificado.

        :param message: Diccionario con la información del juego.
        :return: Diccionario del juego si cumple con el género, de lo contrario None.
        """
        try: 
            genres_str = message.genres
            if self.genre in genres_str:
                return message
            return None
        except Exception as e:
            logging.error(f"Error en filter_games_by_genre: {e}")
            return None
    
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
            batch = data.split('\n')
            for row in batch:
                json_row = json.loads(row)
                game = Game.decode(json_row)
                
                logging.debug(f"Mensaje decodificado: {game}")

                filtered_game = self.filter_games_by_genre(game)
                if filtered_game:
                    game_str = json.dumps(filtered_game.getData())
                    self.middleware.send(game_str)
                    logging.info(f"Juego filtrado enviado: {filtered_game}")
                else:
                    logging.info("Juego no cumple con el filtro de género.")
        
        except Exception as e:
            logging.error(f"Error en _callback al procesar el mensaje: {e}")

    def start(self):
        """
        Inicia el middleware.
        """
        self.middleware.start()
