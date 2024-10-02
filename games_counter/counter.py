# games_filter/filter.py

import logging
from collections import defaultdict

class GamesCounter:
    def __init__(self):
        self.platform_counts = defaultdict(int)

    def counterGames(self, game):
        try:
            game_name = game.get('Name', 'Unknown')
            windows = game.get('Windows', False)
            mac = game.get('Mac', False)
            linux = game.get('Linux', False)

            if isinstance(windows, str):
                windows = windows.lower() == 'true'
            if isinstance(mac, str):
                mac = mac.lower() == 'true'
            if isinstance(linux, str):
                linux = linux.lower() == 'true'

            if windows:
                self.platform_counts['Windows'] += 1
                logging.info(f"Juego '{game_name}' soporta Windows.")
            if mac:
                self.platform_counts['Mac'] += 1
                logging.info(f"Juego '{game_name}' soporta Mac.")
            if linux:
                self.platform_counts['Linux'] += 1
                logging.info(f"Juego '{game_name}' soporta Linux.")

            logging.info(f"Conteo Actual: {dict(self.platform_counts)}")
        except Exception as e:
            logging.error(f"Error al filtrar el juego: {e}")
