import json
import logging
from collections import defaultdict
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
            genres_str = message[GAMES_GENRES_POS]
            genres_str = genres_str[1:-1]
            print(f"Genres: {genres_str}", flush=True)
            genres = [genre.strip() for genre in genres_str.split(',')]
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
        self.middleware.send(data=data)
    
    def _callBack(self, data):
        """
        Callback para procesar los mensajes recibidos.

        :param data: Datos recibidos.
        """
        try:
            result = split_complex_string(data)
            logging.debug(f"Mensaje decodificado: {result}")

            filtered_game = self.filter_games_by_genre(result)
            if filtered_game:
                self.middleware.send(','.join(filtered_game))
                logging.info(f"Juego filtrado enviado: {filtered_game}")
            else:
                logging.info("Juego no cumple con el filtro de género.")
                print("Juego no cumple con el filtro de género.", flush=True)
        except Exception as e:
            logging.error(f"Error en _callback al procesar el mensaje: {e}")

    def start(self):
        """
        Inicia el middleware.
        """
        self.middleware.start()
