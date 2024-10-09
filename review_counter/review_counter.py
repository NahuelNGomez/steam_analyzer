import json
import logging
from common.middleware import Middleware
from common.utils import split_complex_string
from common.constants import GAMES_APP_ID_POS, GAMES_NAME_POS, GAMES_AVERAGE_PLAYTIME_FOREVER_POS
from datetime import datetime

class Top5ReviewCounter:
    def __init__(self, input_queues, output_exchanges, instance_id):
        self.middleware = Middleware(
            input_queues=input_queues,
            output_queues=[],
            output_exchanges=output_exchanges,
            intance_id=instance_id,
            callback=self._process_callback,
            eofCallback=self._eof_callback
        )
        self.games_dict = {}

    def get_games(self):
        """
        Returns the top 5 games with the most positive reviews.
        """
        # Ordenar el diccionario por 'count' en orden descendente y obtener los 5 primeros
        top_5_games = sorted(
            self.games_dict.items(),
            key=lambda x: x[1]['count'],
            reverse=True
        )[:5]
        # Formatear la respuesta
        return {
            "top_5_indie_games_positive_reviews": [
                {
                    "rank": idx + 1,
                    "name": game_data["name"],
                    "positive_review_count": game_data["count"]
                } for idx, (game_id, game_data) in enumerate(top_5_games)
            ],
            "generated_at": datetime.utcnow().isoformat() + "Z"
        }


    def process_game(self, message):
        """
        # Processes each message (game) received and adds it to the top 10 list if applicable.
        # """
        try:
            game_id = message[GAMES_APP_ID_POS]
            name = message[GAMES_NAME_POS][1:-1]
            if game_id in self.games_dict:
                self.games_dict[game_id]['count'] += 1
            else:
                self.games_dict[game_id] = {
                    'name': name,
                    'count': 1
                }

        except Exception as e:
            logging.error(f"Error in process_game: {e}")

    def _process_callback(self, data):
        """
        Callback function to process messages.
        """
    
        message = split_complex_string(data)
        logging.debug(f"Decoded message: {data}")
        self.process_game(message)
        #logging.info(f"Processed message: {message}")

    def _eof_callback(self, data):
        """
        Callback function for handling end of file (EOF) messages.
        """
        logging.info("End of file received. Sending top 5 indie games positive reviews data.")
        top5_games = json.dumps(self.get_games(), indent=4)
        self.middleware.send(top5_games)
        logging.info("Top 5 indie games positive reviews data sent.")


    def start(self):
        """
        Start middleware to begin consuming messages.
        """
        self.middleware.start()
