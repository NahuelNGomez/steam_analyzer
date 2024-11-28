import json
import logging
from collections import defaultdict
from common.game_review import GameReview
from common.middleware import Middleware
from common.packet_fin import Fin
from common.healthcheck import HealthCheckServer
from common.fault_manager import FaultManager

class PercentileAccumulator:
    def __init__(self, input_queues, output_exchanges, instance_id, percentile=90):
        """
        Inicializa el acumulador de nombres de juegos con los parámetros especificados.

        :param input_queues: Diccionario de colas de entrada.
        :param output_exchanges: Lista de exchanges de salida.
        :param instance_id: ID de instancia para identificar colas únicas.
        :param percentile: Percentil para filtrar los juegos (por defecto, 90).
        """
        self.games_by_client = defaultdict(lambda: defaultdict(lambda: {'name': '', 'count': 0}))
        self.percentile = percentile
        self.last_packet_id = []
        self.fault_manager = FaultManager('../persistence/')
        self.init_state()
        self.middleware = Middleware(input_queues, [], output_exchanges, instance_id, 
                                     self._callBack, self._finCallBack, 1, "fanout", "direct")

    def start(self):
        """
        Inicia el acumulador.
        """
        self.middleware.start()
        logging.info("PercentileAccumulator started")
    
    def process_game(self, game, packet_id):
        """
        Procesa cada mensaje (juego) recibido y acumula las reseñas positivas y negativas para cada cliente.
        """
        try:
            game_id = game.game_id
            client_id = int(game.client_id)

            games = self.games_by_client[client_id]

            if game_id in games:
                games[game_id]['count'] += 1
            else:
                games[game_id] = {
                    'name': game.game_name,
                    'count': 1
                }
            # Guardar el estado en formato JSON
            game_data = {
                'game_id': game_id,
                'game_name': game.game_name,
                'packet_id': packet_id
            }
            self.fault_manager.append(f"percentile_{client_id}", json.dumps(game_data))
        except Exception as e:
            logging.error(f"Error in process_game: {e}")
            
    def calculate_90th_percentile(self, client_id):
        """
        Calcula los juegos dentro del percentil 90 de reseñas negativas para un cliente específico.
        """
        try:
            games = self.games_by_client.get(client_id, {})
            # Ordenar los juegos por la cantidad de reseñas negativas de forma ascendente (de menor a mayor)
            sorted_games = sorted(games.items(), key=lambda x: x[1]['count'], reverse=False)
            total_games = len(sorted_games)
            cutoff_index = int((total_games) * (self.percentile / 100))
            
            # Si el total de juegos es menor al cutoff, ajustar el índice
            cutoff_index = min(cutoff_index, total_games - 1)
            
            # Obtener el valor del count del juego que marca el corte
            cutoff_count = sorted_games[cutoff_index][1]['count']

            # Seleccionar todos los juegos dentro del percentil 90 (incluyendo los que tienen el mismo count)
            top_percentile_games = [game for game in sorted_games if game[1]['count'] >= cutoff_count]

            # Crear una lista de juegos con el formato deseado
            negative_count_percentile_list = []

            for game_id, game_data in top_percentile_games:
                game_entry = {
                    'game_id': game_id,
                    'name': game_data['name']
                }
                negative_count_percentile_list.append(game_entry)
                logging.info(f"Juego en el percentil 90 añadido para cliente {client_id}: {game_data['name']}")

            negative_count_percentile_list = sorted(negative_count_percentile_list, key=lambda x: int(x['game_id']))

            response = {
                "negative_count_percentile": {
                    f"client_id {client_id}": negative_count_percentile_list
                }
            }
            self.middleware.send(json.dumps(response))
            self.games_by_client[client_id].clear()
            
        except Exception as e:
            logging.error(f"Error al calcular el percentil 90 para cliente {client_id}: {e}")
            
            
    def _finCallBack(self, data):
        """
        Callback para manejar el mensaje de fin.

        :param data: Datos recibidos.
        """
        logging.info("Fin de la transmisión, calculando percentil 90 de reseñas negativas para cada cliente")
        # Calcular el percentil 90 para cada cliente
        fin_msg = Fin.decode(data)
        client_id = int(fin_msg.client_id)
        self.calculate_90th_percentile(client_id)
        self.fault_manager.delete_key(f"percentile_{client_id}")
        if client_id in self.games_by_client:
            del self.games_by_client[client_id]
    
    def _callBack(self, data):
        """
        Callback para procesar los mensajes recibidos.

        :param data: Datos recibidos.
        """
        try:
            aux = data.strip().split("\n")
            packet_id = aux[0]
            if packet_id in self.last_packet_id:
                logging.info(f"Paquete {packet_id} ya ha sido procesado, saltando...")
                self.last_packet_id.remove(packet_id)
                return
            game_review  = GameReview.decode(json.loads(aux[1]))
            # logging.info(f"Mensaje recivido, packet_id: {packet_id}")
            self.process_game(game_review, packet_id)
            
        except Exception as e:
            logging.error(f"Error en PercentileAccumulator callback: {e}")


    def init_state(self):
        """
        Inicializa el estado de games_by_client desde los datos persistidos en fault_manager.
        """
        for key in self.fault_manager.get_keys("percentile"):
            client_id = int(key.split("_")[1])
            state = self.fault_manager.get(key)

            logging.info(f"Inicializando estado para client_id {client_id}")
            if state:
                game_entries = state.strip().split("\n")
                
                # if client_id not in self.games_by_client:
                #     self.games_by_client[client_id] = defaultdict(lambda: {'name': '', 'count': 0})
                self.last_packet_id.append(json.loads(game_entries[-1])['packet_id'])
                for entry in game_entries:
                    try:
                        game_data = json.loads(entry)
                        game_id = game_data['game_id']
                        game_name = game_data['game_name']
                        
                        if game_id in self.games_by_client[client_id]:
                            self.games_by_client[client_id][game_id]['count'] += 1
                        else:
                            self.games_by_client[client_id][game_id] = {'name': game_name, 'count': 1}
                    except json.JSONDecodeError as e:
                        logging.error(f"Error al decodificar entrada: {entry}. Detalles: {e}")
                
        
                logging.info(f"Estado inicializado para client_id {client_id}: {self.games_by_client[client_id]}")
        logging.info(f"Último packet_id de cada cliente: {self.last_packet_id}")

