import json
import logging
from datetime import datetime
from common.game import Game
from common.middleware import Middleware
from common.healthcheck import HealthCheckServer

class RangeFilter:
    def __init__(self, start_year, end_year, input_queues, output_exchanges, instance_id):
        self.middleware = Middleware(input_queues, [], output_exchanges, instance_id, self._callBack, self._finCallBack)
        
        self.start_year = start_year
        self.end_year = end_year
        self.packet_id = 0
        
    def _callBack(self, data):
        aux = data.strip().split("\n")
        packet_id = aux[0]
        print(f"Recibido paquete con ID: {packet_id}", flush=True)
        json_row = json.loads(aux[1])
        game = Game.decode(json_row)
        logging.debug(f"Mensaje decodificado: {game}")

        filtered_game = self.filter_by_range(game)
        if filtered_game:
            game_str = json.dumps(filtered_game.getData())
            game_str = f'{packet_id}\n{game_str}\n'
            self.middleware.send(game_str)
            logging.info(f"Paquete enviado: {packet_id}")
            logging.info(f"Juego filtrado enviado:{filtered_game}")
        else:
            logging.info("Juego no cumple con el filtro de rango.")

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
        Si la cadena de fecha no tiene un formato válido, intenta extraer solo el año.
        """
        try:
            # Intentar con el formato corto del mes
            release_date = datetime.strptime(release_date_str, "%b %d, %Y")
            return release_date.year
        except ValueError:
            try:
                # Intentar con el formato largo del mes
                release_date = datetime.strptime(release_date_str, "%B %d, %Y")
                return release_date.year
            except ValueError:
                # Si no coincide con ningún formato, intentar extraer el año directamente
                logging.warning(f"Formato de fecha inesperado: '{release_date_str}', intentando extraer el año.")
                # Verificar si solo contiene un año
                if release_date_str.strip().isdigit():
                    return int(release_date_str.strip())
                else:
                    logging.error(f"No se pudo extraer el año de la fecha: '{release_date_str}'")
                    raise

        
    def _finCallBack(self, data):
        logging.info("Fin de la transmisión de datos")
        self.middleware.send(data = data)
        

    def start(self):
        HealthCheckServer().start_in_thread()
        self.middleware.start()
        
