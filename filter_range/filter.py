# filter_range/filter.py

import logging
from datetime import datetime

class FilterRange:
    def __init__(self, start_year=2010, end_year=2019):
        self.start_year = start_year
        self.end_year = end_year

    def filter_by_range(self, message):
        """
        Filtra juegos publicados entre start_year y end_year.
        """
        try:
            release_date_str = message.get('Release date', '')
            release_year = self.extract_year(release_date_str)
            if self.start_year <= release_year <= self.end_year:
                return message
            return None
        except Exception as e:
            logging.error(f"Error en filter_by_range: {e}")
            return None

    def extract_year(self, release_date_str):
        """
        Extrae el año de una cadena de fecha en formato "MMM DD, YYYY" o "MMMM DD, YYYY".
        """
        try:
            release_date = datetime.strptime(release_date_str, "%b %d, %Y")
            return release_date.year
        except ValueError:
            release_date = datetime.strptime(release_date_str, "%B %d, %Y")
            return release_date.year
