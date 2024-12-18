import json
import logging
from collections import defaultdict
from common.game_review import GameReview
from common.middleware import Middleware
from common.packet_fin import Fin
from common.middleware import Middleware
from common.healthcheck import HealthCheckServer
from common.fault_manager import FaultManager

class GameNamesAccumulator:
    def __init__(self, input_queues, output_exchanges, instance_id, reviews_low_limit, previous_language_nodes):
        """
        Inicializa el acumulador de nombres de juegos con los parámetros especificados.

        :param input_queues: Diccionario de colas de entrada.
        :param output_exchanges: Lista de exchanges de salida.
        :param instance_id: ID de instancia para identificar colas únicas.
        :param reviews_low_limit: Límite de reseñas para enviar un juego.
        """
        # Diccionario para almacenar juegos por client_id
        self.games_by_client = defaultdict(lambda: defaultdict(int))
        self.sent_games_by_client = defaultdict(list)
        self.reviews_low_limit = reviews_low_limit
        self.fault_manager = FaultManager("../persistence/")
        self.last_packet_id = []
        self.datasent_by_client = defaultdict(bool)
        self.init_state()
        self.middleware = Middleware(
            input_queues,
            [],
            output_exchanges,
            instance_id,
            self._callBack,
            self._finCallBack,
            self.fault_manager,
        )
        self.total_fin = int(previous_language_nodes)
        self.received_fin:dict = {}
        self.data_to_store = ''
        self.counter = 0
        
    def start(self):
        """
        Inicia el acumulador.
        """
        self.middleware.start()
        logging.info("GameNamesAccumulator started")
    
    def process_game(self, game, packet_id):
        """
        Procesa cada mensaje (juego) recibido y acumula las reseñas positivas y negativas por cliente.
        Si el número de reseñas supera el límite definido, envía el nombre del juego.
        """
        try:
            # Validación de datos de entrada
            if not hasattr(game, 'client_id'):
                raise ValueError(f"Game object missing client_id attribute: {game._dict_}")
            if not hasattr(game, 'game_id'):
                raise ValueError(f"Game object missing game_id attribute: {game._dict_}")
            if not hasattr(game, 'game_name'):
                raise ValueError(f"Game object missing game_name attribute: {game._dict_}")

            # Conversión con manejo de errores explícito
            try:
                client_id = int(game.client_id)
            except (ValueError, TypeError) as e:
                raise ValueError(f"Invalid client_id format: {game.client_id}. Error: {str(e)}")

            try:
                game_id = int(game.game_id)
            except (ValueError, TypeError) as e:
                raise ValueError(f"Invalid game_id format: {game.game_id}. Error: {str(e)}")


            # Inicialización de estructuras de datos con logging detallado
            if client_id not in self.games_by_client:
                self.games_by_client[client_id] = {}
            
            if client_id not in self.sent_games_by_client:
                self.sent_games_by_client[client_id] = []

            games = self.games_by_client[client_id]
            sent_games = self.sent_games_by_client[client_id]

            # Procesamiento del juego con logging detallado
            if game_id in games:
                games[game_id]["count"] += 1
            else:
                if game_id not in sent_games:
                    games[game_id] = {"name": game.game_name, "count": 1}

            if (
                game_id in games and
                games[game_id]["count"] > self.reviews_low_limit and
                game_id not in sent_games
            ):
                message = {
                    
                    "game_exceeding_limit": {
                        f"client_id {client_id}": [{"game_name": games[game_id]["name"]}]
                    }
                }
                self.middleware.send(json.dumps(message))

                sent_games.append(game_id)
                games.pop(game_id)
                self.datasent_by_client[client_id] = True
            
            game_data = {
                'game_id': game_id,
                'game_name': game.game_name,
                'packet_id': packet_id
            }
            self.data_to_store += json.dumps(game_data) + "\n"

            
        except Exception as e:
            logging.error(f"Error in process_game: {str(e)}")


    def get_games(self, client_id):
        """
        Obtiene los juegos acumulados para un cliente específico.

        :param client_id: ID del cliente.
        :return: Diccionario de juegos acumulados.
        """
        return self.games_by_client.get(client_id, {})

    def _finCallBack(self, data):
        """
        Callback para manejar el mensaje de fin.
        Procesa el fin específico para el cliente asociado al `client_id` recibido.
        """
        try:
            fin = Fin.decode(data)
            client_id = int(fin.client_id)
            logging.info(f"Fin de la transmisión recibido para el cliente {client_id}")
            if client_id not in self.received_fin:
                self.received_fin[client_id] = 1
            else:
                self.received_fin[client_id] += 1
            if self.received_fin[client_id] == self.total_fin:
                logging.info(f"Fin de la transmisión recibido para el cliente {client_id} y todos los nodos de lenguaje")
                if not self.datasent_by_client[client_id]:
                    message = {"game_exceeding_limit": {"client_id " + str(client_id): []}}
                    self.middleware.send(json.dumps(message))
                message2 ={"final_check_low_limit": {"client_id " +  str(client_id): True}}
                self.middleware.send(json.dumps(message2))
                self.fault_manager.delete_key(f'game_names_accumulator_{str(client_id)}')
            
        except Exception as e:
            logging.error(f"Error al procesar el mensaje de fin: {e}")

    def _callBack(self, data):
        """
        Callback para procesar los mensajes recibidos.
        """
        try:
            aux = data.strip().split("\n")
            packet_id = aux[0]
            self.data_to_store = ''
            if packet_id in self.last_packet_id:
                logging.info(f"Paquete {packet_id} ya ha sido procesado, saltando...")
                self.last_packet_id.remove(packet_id)
                return
            self.counter += 1
            logging.info(f"Contador de paquetes: {self.counter}")
            for row in aux[1:]:
                game = GameReview.decode(json.loads(row))
                self.process_game(game, packet_id)
            self.fault_manager.append(f'game_names_accumulator_{game.client_id}', self.data_to_store)
        except Exception as e:
            logging.error(f"Error en GameNamesAccumulator callback: {e}")


    def init_state(self):
        
        for key in self.fault_manager.get_keys("game_names_accumulator"):
            client_id = int(key.split("_")[3])
            data = self.fault_manager.get(key)
            data = data.strip().split("\n")
            
            logging.info(f"Restaurando estado para el cliente {client_id}")
            if client_id not in self.games_by_client:
                self.games_by_client[client_id] = {}
            
            if client_id not in self.sent_games_by_client:
                self.sent_games_by_client[client_id] = []
            
            self.last_packet_id.append(json.loads(data[-1])['packet_id'])
            try:
                for game_data in data:
                    if not game_data.strip():
                        continue
                    game = json.loads(game_data)
                    game_id = game["game_id"]
                    game_name = game["game_name"]
                    if game_id in self.games_by_client[client_id]:
                        self.games_by_client[client_id][game_id]["count"] += 1
                    else:
                        self.games_by_client[client_id][game_id] = {"name": game_name, "count": 1}
                logging.info(f'Estado del cliente {client_id} restaurado')
                logging.info(f'Juegos acumulados: {self.games_by_client[client_id]}')
            
            except Exception as e:
                logging.error(f"Error al inicializar el estado: {e}")
                

            for game_id, game_data in self.games_by_client[client_id].items():
                if game_data["count"] > self.reviews_low_limit:
                    logging.info(f"Enviando juego acumulado por límite de reseñas: {game_data['name']}")
                    self.sent_games_by_client[client_id].append(game_id)
                    self.datasent_by_client[client_id] = True
            
            # Eliminar juegos enviados de games_by_client
            for game_id in self.sent_games_by_client[client_id]:
                del self.games_by_client[client_id][game_id]

        logging.info(f"Último packet_id de cada cliente: {self.last_packet_id}")