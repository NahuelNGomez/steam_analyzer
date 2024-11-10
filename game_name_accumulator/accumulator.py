import json
import logging
import sys
from collections import defaultdict
from common.game_review import GameReview
from common.middleware import Middleware
from common.packet_fin import Fin
import gc
import weakref

class GameNamesAccumulator:
    def __init__(self, input_queues, output_exchanges, instance_id, reviews_low_limit, previous_language_nodes):
        """
        Inicializa el acumulador de nombres de juegos con los parámetros especificados.
        """
        # Usar diccionarios regulares en lugar de defaultdict para mejor control de memoria
        self.games_by_client = {}
        self.sent_games_by_client = {}
        self.reviews_low_limit = reviews_low_limit
        self.middleware = Middleware(
            input_queues,
            [],
            output_exchanges,
            instance_id,
            self._callBack,
            self._finCallBack,
        )
        self.datasent_by_client = {}
        self.total_fin = int(previous_language_nodes)
        self.received_fin = {}

    def _initialize_client_data(self, client_id):
        """
        Inicializa las estructuras de datos para un cliente específico solo cuando sea necesario.
        """
        if client_id not in self.games_by_client:
            self.games_by_client[client_id] = {}
        if client_id not in self.sent_games_by_client:
            self.sent_games_by_client[client_id] = []
        if client_id not in self.datasent_by_client:
            self.datasent_by_client[client_id] = False

    def _cleanup_client_data(self, client_id):
        """
        Limpia completamente los datos de un cliente específico.
        """
        # Limpiar explícitamente todas las estructuras internas
        if client_id in self.games_by_client:
            for game_id in list(self.games_by_client[client_id].keys()):
                self.games_by_client[client_id][game_id].clear()
            self.games_by_client[client_id].clear()
            del self.games_by_client[client_id]

        if client_id in self.sent_games_by_client:
            self.sent_games_by_client[client_id].clear()
            del self.sent_games_by_client[client_id]

        if client_id in self.datasent_by_client:
            del self.datasent_by_client[client_id]

        if client_id in self.received_fin:
            del self.received_fin[client_id]

        # Forzar recolección de basura específica
        gc.collect()

    def process_game(self, game):
        """
        Procesa cada mensaje (juego) recibido con manejo optimizado de memoria.
        """
        try:
            if not all(hasattr(game, attr) for attr in ['client_id', 'game_id', 'game_name']):
                raise ValueError(f"Game object missing required attributes: {game._dict_}")

            client_id = int(game.client_id)
            game_id = int(game.game_id)

            self._initialize_client_data(client_id)
            
            games = self.games_by_client[client_id]
            sent_games = self.sent_games_by_client[client_id]

            if game_id in games:
                games[game_id]["count"] += 1
            elif game_id not in sent_games:
                # Minimizar la estructura de datos almacenada
                games[game_id] = {"name": str(game.game_name), "count": 1}

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
                # Limpiar inmediatamente los datos del juego
                games.pop(game_id)
                self.datasent_by_client[client_id] = True

        except Exception as e:
            logging.error(f"Error in process_game: {str(e)}")
            logging.error(f"Full game object: {game._dict_}")

    def _finCallBack(self, data):
        """
        Callback para manejar el mensaje de fin con limpieza mejorada de memoria.
        """
        try:
            fin = Fin.decode(data)
            client_id = int(fin.client_id)
            
            if client_id not in self.received_fin:
                self.received_fin[client_id] = 1
            else:
                self.received_fin[client_id] += 1

            if self.received_fin[client_id] == self.total_fin:
                # Enviar mensajes finales si es necesario
                if not self.datasent_by_client.get(client_id, False):
                    self.middleware.send(json.dumps({
                        "game_exceeding_limit": {"client_id " + str(client_id): []}
                    }))
                
                self.middleware.send(json.dumps({
                    "final_check_low_limit": {"client_id " + str(client_id): True}
                }))

                # Registrar tamaños antes de la limpieza
                games_size_before = sys.getsizeof(self.games_by_client.get(client_id, {}))
                sent_games_size_before = sys.getsizeof(self.sent_games_by_client.get(client_id, []))
                logging.info(f"Antes de liberar recursos - Juegos del cliente {client_id}: {games_size_before} bytes")
                logging.info(f"Antes de liberar recursos - Juegos enviados del cliente {client_id}: {sent_games_size_before} bytes")

                # Realizar limpieza completa
                self._cleanup_client_data(client_id)

                # Registrar tamaños después de la limpieza
                games_size_after = sys.getsizeof(self.games_by_client.get(client_id, {}))
                sent_games_size_after = sys.getsizeof(self.sent_games_by_client.get(client_id, []))
                logging.info(f"Después de liberar recursos - Juegos del cliente {client_id}: {games_size_after} bytes")
                logging.info(f"Después de liberar recursos - Juegos enviados del cliente {client_id}: {sent_games_size_after} bytes")

                # Forzar limpieza adicional
                gc.collect()

        except Exception as e:
            logging.error(f"Error al procesar el mensaje de fin: {e}")

    def start(self):
        """
        Inicia el acumulador.
        """
        self.middleware.start()
        logging.info("GameNamesAccumulator started")

    def _callBack(self, data):
        """
        Callback para procesar los mensajes recibidos.
        """
        try:
            game = GameReview.decode(json.loads(data))
            self.process_game(game)
        except Exception as e:
            logging.error(f"Error en GameNamesAccumulator callback: {e}")