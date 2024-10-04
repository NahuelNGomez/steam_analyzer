# top10_indie_counter/counter.py

import logging
from collections import defaultdict
from datetime import datetime


class Top10IndieCounter:
    def __init__(self):
        # Diccionario para almacenar el tiempo total y el conteo de juegos por game_id
        self.game_playtimes = {i: {"nombre": None, "tiempo": None} for i in range(10)}
        
    def get_games(self):
        """
        Recibe un diccionario con información de los juegos.
        """
        return self.game_playtimes

    def process_game(self, message):
        """
        Procesa cada mensaje (juego) recibido y lo añade al top 10 si corresponde.
        """
        try:
            # Extraer los datos del mensaje
            game_id = message.get("AppID")
            name = message.get("Name")
            playtime = message.get("Average playtime forever")  # En minutos
            playtime_hours = int(playtime) / 60
            
            print(f"Procesando juego2: {name} ({playtime_hours} horas)...", flush=True)
            # Encuentra el puesto con el menor tiempo registrado
            menor_puesto = min(
                (k for k, v in self.game_playtimes.items() if v["tiempo"] is not None),
                key=lambda k: self.game_playtimes[k]["tiempo"],
                default=None
            )

            # Si hay un puesto vacío, úsalo
            puesto_vacio = next((k for k, v in self.game_playtimes.items() if v["tiempo"] is None), None)

            # Si hay un puesto vacío, o el nuevo juego tiene mayor tiempo que el menor registrado, reemplaza
            if puesto_vacio is not None:
                self.game_playtimes[puesto_vacio] = {"nombre": name, "tiempo": playtime_hours}
            elif menor_puesto is not None and playtime_hours > self.game_playtimes[menor_puesto]["tiempo"]:
                self.game_playtimes[menor_puesto] = {"nombre": name, "tiempo": playtime_hours}

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
                total_time = data["total_time"]
                count = data["count"]
                avg_time = total_time / count if count else 0
                avg_playtimes.append(
                    {
                        "AppID": game_id,
                        "Name": self.games_info.get(game_id, {}).get(
                            "name", "Desconocido"
                        ),
                        "Average Playtime (Hours)": round(avg_time, 2),
                    }
                )

            # Ordenar los juegos por tiempo promedio de juego descendente
            sorted_games = sorted(
                avg_playtimes, key=lambda x: x["Average Playtime (Hours)"], reverse=True
            )

            # Seleccionar los top N juegos
            top_games = sorted_games[:top_n]

            return top_games

        except Exception as e:
            logging.error(f"Error en calculate_top10: {e}")
            return []
