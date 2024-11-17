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
from common.fault_manager import FaultManager  # Importar FaultManager

# Configuración de logging
logging.basicConfig(level=logging.INFO)

class GamesCounter:
    def __init__(self, input_queues, output_exchanges, instance_id):
        logging.info(f"Iniciando GamesCounter con ID de instancia: {instance_id}")
        # Inicializar el FaultManager
        self.fault_manager = FaultManager(storage_dir="fault_data/games_counter")
        # Cargar el estado existente si existe
        initial_state = self.fault_manager.get(f"platforms_counter_{instance_id}")
        if initial_state is None:
            logging.info("No se encontró estado previo, inicializando estado vacío.")
            self.platform_counts = defaultdict(lambda: {'Windows': 0, 'Mac': 0, 'Linux': 0})
        else:
            logging.info(f"Estado cargado desde FaultManager: {initial_state}")
            self.platform_counts = defaultdict(lambda: {'Windows': 0, 'Mac': 0, 'Linux': 0}, {instance_id: initial_state})
        
        self.middleware = Middleware(input_queues, [], output_exchanges, instance_id, self._callBack, self._finCallBack)

    def counterGames(self, game):
        try:
            game_name = game.name
            windows = game.windows
            mac = game.mac
            linux = game.linux
            client_id = game.client_id
            
            logging.info(f"Procesando juego '{game_name}' del cliente {client_id}")

            # Convertir a booleano de forma robusta
            windows = self._convert_to_boolean(windows)
            mac = self._convert_to_boolean(mac)
            linux = self._convert_to_boolean(linux)

            # Verificar si los valores ya eran booleanos
            if windows:
                self.platform_counts[client_id]['Windows'] += 1
                logging.info(f"Juego '{game_name}' soporta Windows. Total: {self.platform_counts[client_id]['Windows']}")
            if mac:
                self.platform_counts[client_id]['Mac'] += 1
                logging.info(f"Juego '{game_name}' soporta Mac. Total: {self.platform_counts[client_id]['Mac']}")
            if linux:
                self.platform_counts[client_id]['Linux'] += 1
                logging.info(f"Juego '{game_name}' soporta Linux. Total: {self.platform_counts[client_id]['Linux']}")

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
            logging.info(f"Mensaje recibido para procesamiento: {data}")
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
            logging.info(f"Fin de transmisión recibido del cliente {client_id}")
            response = {
                "supported_platforms": {
                    f"client_id_{client_id}": [
                        {"platform": "Windows", "game_count": self.platform_counts[client_id]['Windows']},
                        {"platform": "Mac", "game_count": self.platform_counts[client_id]['Mac']},
                        {"platform": "Linux", "game_count": self.platform_counts[client_id]['Linux']}
                    ]
                }
            }
            logging.info(f"Enviando respuesta al cliente {client_id}: {response}")
            self.middleware.send(json.dumps(response, indent=4))
            # Obtener el número de paquete (debe ser parte del estado o del mensaje)
            package_number = getattr(fin, 'package_number', None)  # Asumiendo que Fin contiene package_number
            if package_number is None:
                logging.warning(f"No se encontró package_number en el mensaje Fin para cliente {client_id}")
                package_number = 0

            # Actualizar el estado en FaultManager
            self.fault_manager.update(f"platforms_counter_{client_id}", self.platform_counts[client_id]['Windows'], package_number)
            self.fault_manager.update(f"platforms_counter_{client_id}", self.platform_counts[client_id]['Mac'], package_number)
            self.fault_manager.update(f"platforms_counter_{client_id}", self.platform_counts[client_id]['Linux'], package_number)
            logging.info(f"Estado actualizado en FaultManager para el cliente {client_id}")
        except Exception as e:
            logging.error(f"Error al procesar el mensaje de fin: {e}")
    
    def start(self):
        """
        Inicia el middleware.
        """
        logging.info("Iniciando el middleware para GamesCounter")
        self.middleware.start()
