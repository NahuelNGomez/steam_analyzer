import json
import logging
from collections import defaultdict
from common.middleware import Middleware
from common.utils import split_complex_string

class PercentileAccumulator:
    def __init__(self, input_queues, output_exchanges, instance_id, percentile=90):
        """
        Inicializa el acumulador de nombres de juegos con los parámetros especificados.

        :param input_queues: Diccionario de colas de entrada.
        :param output_exchanges: Lista de exchanges de salida.
        :param instance_id: ID de instancia para identificar colas únicas.
        :param percentile: Percentil para filtrar los juegos (por defecto, 90).
        """
        self.games = defaultdict(lambda: {'name': '', 'count': 0})
        self.percentile = percentile
        self.middleware = Middleware(input_queues, [], output_exchanges, instance_id, 
                                     self._callBack, self._finCallBack)

    def start(self):
        """
        Inicia el acumulador.
        """
        self.middleware.start()
        logging.info("PercentileAccumulator started")
    
    def process_game(self, message):
        """
        Procesa cada mensaje (juego) recibido y acumula las reseñas positivas y negativas.
        """
        try:
            print("Games dict: ", self.games, flush=True)
            game_id = message[REVIEWS_APP_ID_POS]
            print(f"Game ID: {game_id}", flush=True)
            text = message[REVIEWS_TEXT_POS][1:-1]
            print(f"Game text: {text}", flush=True)
            if game_id in self.games:
                self.games[game_id]['count'] += 1
            else:
                self.games[game_id] = {
                    'name': message[1][1:-1],
                    'count': 1
                }
        except Exception as e:
            logging.error(f"Error in process_game: {e}")
    
    def calculate_90th_percentile(self):
        """
        Calcula los juegos dentro del percentil 90 de reseñas negativas.
        """
        try:
            # Ordenar los juegos por la cantidad de reseñas negativas de forma descendente
            sorted_games = sorted(self.games.items(), key=lambda x: x[1]['count'], reverse=True)
            total_games = len(sorted_games)
            cutoff_index = int(total_games * (self.percentile / 100))

            # Seleccionar los juegos dentro del percentil 90
            top_percentile_games = sorted_games[:cutoff_index]
            for game_id, game_data in top_percentile_games:
                self.middleware.send(json.dumps({
                    'game_id': game_id,
                    'name': game_data['name'],
                    'count': game_data['count'],
                }))
                logging.info(f"Juego en el percentil 90 enviado: {game_data['name']}")
        except Exception as e:
            logging.error(f"Error al calcular el percentil 90: {e}")
    
    def _finCallBack(self, data):
        """
        Callback para manejar el mensaje de fin.

        :param data: Datos recibidos.
        """
        logging.info("Fin de la transmisión, calculando percentil 90 de reseñas negativas")
        self.calculate_90th_percentile()
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
            logging.error(f"Error en PercentileAccumulator callback: {e}")