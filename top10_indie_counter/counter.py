import json
import logging
from common.game import Game
from common.middleware import Middleware
from common.utils import split_complex_string
from common.constants import GAMES_APP_ID_POS, GAMES_NAME_POS, GAMES_AVERAGE_PLAYTIME_FOREVER_POS
from datetime import datetime

class Top10IndieCounter:
    def __init__(self, input_queues, output_exchanges, instance_id):
        self.middleware = Middleware(
            input_queues=input_queues,
            output_queues=[],
            output_exchanges=output_exchanges,
            intance_id=instance_id,
            callback=self._process_callback,
            eofCallback=self._eof_callback
        )
        self.game_playtimes = {i: {"nombre": None, "tiempo": None} for i in range(10)}

    def get_games(self):
        """
        Returns the top 10 games dictionary.
        """
        # Ordenar los juegos por tiempo de juego descendente
        sorted_games = sorted(
            self.game_playtimes.items(),
            key=lambda item: item[1]["tiempo"],
            reverse=True
        )
        # Obtener los top 10
        top10 = sorted_games[:10]
        # Formatear la respuesta
        return {
            "top_10_indie_games_2010s": [
                {
                    "rank": idx + 1,
                    "name": game["nombre"],
                    "average_playtime_hours": round(game["tiempo"], 2)
                } for idx, (game_id, game) in enumerate(top10)
            ]
        }


    def process_game(self, game):
        """
        Processes each message (game) received and adds it to the top 10 list if applicable.
        """
        try:
            game_id = game.id
            name = game.name
            playtime = int(game.apf)

            menor_puesto = min(
                (k for k, v in self.game_playtimes.items() if v["tiempo"] is not None),
                key=lambda k: self.game_playtimes[k]["tiempo"],
                default=None,
            )

            puesto_vacio = next(
                (k for k, v in self.game_playtimes.items() if v["tiempo"] is None),
                None,
            )

            if puesto_vacio is not None:
                self.game_playtimes[puesto_vacio] = {"nombre": name, "tiempo": playtime}
            elif menor_puesto is not None and playtime > self.game_playtimes[menor_puesto]["tiempo"]:
                self.game_playtimes[menor_puesto] = {"nombre": name, "tiempo": playtime}

        except Exception as e:
            logging.error(f"Error in process_game: {e}")

    def _process_callback(self, data):
        """
        Callback function to process messages.
        """
        json_row = json.loads(data)
        game = Game.decode(json_row)
        logging.debug(f"Decoded message: {game}")
        self.process_game(game)
        logging.info(f"Processed message: {game}")

    def _eof_callback(self, data):
        """
        Callback function for handling end of file (EOF) messages.
        """
        logging.info("End of file received. Sending top 10 indie games data.")
        top10_games = json.dumps(self.get_games(), indent=4)
        self.middleware.send(top10_games)
        logging.info("Top 10 indie games data sent.")


    def start(self):
        """
        Start middleware to begin consuming messages.
        """
        self.middleware.start()
