import json
import logging
import re
from common.middleware import Middleware

APP_ID_POSITION = 0
NAME_POSITION = 1
PLAYTIME_POSITION = 28

def split_complex_string(s):
    pattern = r'''
        (?:\[.*?\])   # Captura arrays entre corchetes
        |             # O
        (?:".*?")     # Captura texto entre comillas dobles
        |             # O
        (?:'.*?')     # Captura texto entre comillas simples
        |             # O
        (?:[^,]+)     # Captura cualquier cosa que no sea una coma
    '''
    return re.findall(pattern, s, re.VERBOSE)

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
        return self.game_playtimes

    def process_game(self, message):
        """
        Processes each message (game) received and adds it to the top 10 list if applicable.
        """
        try:
            game_id = message[APP_ID_POSITION]
            name = message[NAME_POSITION][1:-1]
            playtime = message[PLAYTIME_POSITION]
            playtime_hours = int(playtime) / 60

            print(f"Processing game: {name} ({playtime_hours} hours)...", flush=True)

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
                self.game_playtimes[puesto_vacio] = {"nombre": name, "tiempo": playtime_hours}
            elif menor_puesto is not None and playtime_hours > self.game_playtimes[menor_puesto]["tiempo"]:
                self.game_playtimes[menor_puesto] = {"nombre": name, "tiempo": playtime_hours}

        except Exception as e:
            logging.error(f"Error in process_game: {e}")

    def _process_callback(self, data):
        """
        Callback function to process messages.
        """
        message =split_complex_string(data)
        logging.debug(f"Decoded message: {message}")
        self.process_game(message)
        logging.info(f"Processed message: {message}")

    def _eof_callback(self, data):
        """
        Callback function for handling end of file (EOF) messages.
        """
        logging.info("End of file received. Sending top 10 games data.")
        top10_games = json.dumps(self.get_games())
        self.middleware.send(top10_games)
        logging.info("Top 10 games data sent.")

    def start(self):
        """
        Start middleware to begin consuming messages.
        """
        self.middleware.start()
