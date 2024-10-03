# top10_indie_counter/counter.py

import logging
from collections import defaultdict
from datetime import datetime

class Top10IndieCounter:
    def __init__(self):
        # Diccionario para almacenar el tiempo total y el conteo de juegos por game_id
        self.game_playtimes = defaultdict(lambda: {'total_time': 0, 'count': 0})
        self.games_info = {}  # Para almacenar información básica de los juegos

    def process_game(self, message):
        """
        Procesa cada mensaje (juego) recibido.
        """
        try:
            game_id = message.get('AppID')
            name = message.get('Name')
            genres_str = message.get('Genres', '')
            release_date_str = message.get('Release date', '')
            playtime = message.get('Average playtime forever')  # En minutos

            # Convertir playtime a horas si es necesario
            if isinstance(playtime, (int, float)):
                playtime_hours = playtime / 60  # Asumiendo que está en minutos
            else:
                playtime_hours = 0

            # Convertir géneros a lista
            genres = [genre.strip() for genre in genres_str.split(',')]

            # Filtrar por género 'Indie'
            if 'Indie' not in genres:
                return

            # Filtrar por década de 2010 (2010-2019)
            try:
                release_year = self.extract_year(release_date_str)
                if not (2010 <= release_year <= 2019):
                    return
            except:
                return  # Si no se puede extraer el año, omitir el juego

            # Acumular el tiempo de juego
            if playtime_hours > 0:
                self.game_playtimes[game_id]['total_time'] += playtime_hours
                self.game_playtimes[game_id]['count'] += 1

                # Almacenar información básica del juego
                if game_id not in self.games_info:
                    self.games_info[game_id] = {
                        'name': name
                    }

        except Exception as e:
            logging.error(f"Error en process_game: {e}")

    def extract_year(self, release_date_str):
        """
        Extrae el año de una cadena de fecha en formato "MMM DD, YYYY".
        Ejemplo: "Oct 21, 2008" -> 2008
        """
        try:
            release_date = datetime.strptime(release_date_str, "%b %d, %Y")
            return release_date.year
        except ValueError:
            # Intentar con otro formato si es necesario
            release_date = datetime.strptime(release_date_str, "%B %d, %Y")
            return release_date.year

    def calculate_top10(self, top_n=10):
        """
        Calcula el tiempo promedio de juego y devuelve los top N juegos.
        """
        try:
            # Calcular el tiempo promedio de juego para cada juego
            avg_playtimes = []
            for game_id, data in self.game_playtimes.items():
                total_time = data['total_time']
                count = data['count']
                avg_time = total_time / count if count else 0
                avg_playtimes.append({
                    'AppID': game_id,
                    'Name': self.games_info.get(game_id, {}).get('name', 'Desconocido'),
                    'Average Playtime (Hours)': round(avg_time, 2)
                })

            # Ordenar los juegos por tiempo promedio de juego descendente
            sorted_games = sorted(avg_playtimes, key=lambda x: x['Average Playtime (Hours)'], reverse=True)

            # Seleccionar los top N juegos
            top_games = sorted_games[:top_n]

            return top_games

        except Exception as e:
            logging.error(f"Error en calculate_top10: {e}")
            return []
