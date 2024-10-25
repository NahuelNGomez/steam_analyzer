import json
import logging
from collections import defaultdict
from common.game_review import GameReview
from common.middleware import Middleware
from common.packet_fin import Fin


class GameNamesAccumulator:
    def __init__(self, input_queues, output_exchanges, instance_id, reviews_low_limit):
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
        self.middleware = Middleware(
            input_queues,
            [],
            output_exchanges,
            instance_id,
            self._callBack,
            self._finCallBack,
        )
        self.datasent_by_client = defaultdict(bool)

    def start(self):
        """
        Inicia el acumulador.
        """
        self.middleware.start()
        logging.info("GameNamesAccumulator started")

    def process_game(self, game):
        """
        Procesa cada mensaje (juego) recibido y acumula las reseñas positivas y negativas por cliente.
        Si el número de reseñas supera el límite definido, envía el nombre del juego.
        """
        try:
            client_id = game.client_id  # Obtener el client_id
            game_id = game.game_id

            games = self.games_by_client[client_id]
            sent_games = self.sent_games_by_client[client_id]

            if game_id in games:
                games[game_id]["count"] += 1
            else:
                if games[game_id] not in sent_games:
                    games[game_id] = {"name": game.game_name, "count": 1}

            if (
                games[game_id]["count"] > self.reviews_low_limit
                and game_id not in sent_games
            ):
                message = {
                    f"game_exceeding_limit": {
                        "client id "
                        + str(client_id): {{"game_name": games[game_id]["name"]}}
                    }
                }
                self.middleware.send(json.dumps(message))
                sent_games.append(game_id)
                games.pop(game_id)
                self.datasent_by_client[client_id] = True

        except Exception as e:
            logging.error(f"Error in process_game: {e}")

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
            client_id = fin.client_id
            logging.info(f"Fin de la transmisión recibido para el cliente {client_id}")

            if not self.datasent_by_client[client_id]:
                message = {"game_exceeding_limit": {"client_id " + client_id: []}}
                self.middleware.send(json.dumps(message))

            message2 ={"final_check_low_limit": {"client_id " + client_id: True}}
            self.middleware.send(json.dumps(message2))

        except Exception as e:
            logging.error(f"Error al procesar el mensaje de fin: {e}")

    def _callBack(self, data):
        """
        Callback para procesar los mensajes recibidos.
        """
        try:
            game = GameReview.decode(json.loads(data))
            logging.debug(f"Mensaje decodificado: {game}")
            self.process_game(game)

        except Exception as e:
            logging.error(f"Error en GameNamesAccumulator callback: {e}")
