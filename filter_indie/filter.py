# filter_indie/filter.py

import logging
from collections import defaultdict

class FilterIndie:
    def __init__(self):
        pass

    def filter_indie_games(self, message):
        """
        Filtra juegos que tienen el género 'Indie'.
        """
        try:
            genres_str = message.get('Genres', '')
            genres = [genre.strip() for genre in genres_str.split(',')]
            if 'Indie' in genres:
                return message
            return None
        except Exception as e:
            logging.error(f"Error en filter_indie_games: {e}")
            return None
