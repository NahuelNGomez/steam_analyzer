import json
import logging
from common.game import Game
from common.middleware import Middleware
from common.utils import split_complex_string
from common.packet_fin import Fin
<<<<<<< HEAD
from common.healthcheck import HealthCheckServer
from common.fault_manager import FaultManager
=======
>>>>>>> doctor_improvements

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
        #self.game_playtimes = {i: {"nombre": None, "tiempo": None} for i in range(10)}
        self.game_playtimes_by_client = {}
        self.fault_manager = FaultManager(storage_dir="../persistence/")
        self._init_state()
        self.last_processed_packet = None
        
    def _init_state(self):
        for key in self.fault_manager.get_keys('top10_indie_counter'):
            client_id = int(key.split("_")[3])
            state = self.fault_manager.get(key)
            if state is not None:
                data, packet_id = state.rsplit("|", 1)
                self.game_playtimes_by_client[client_id] = json.loads(data)
                print(f"Estado cargado desde FaultManager: {self.game_playtimes_by_client[client_id]}", flush=True)
                self.last_processed_packet = packet_id
                logging.info('packet id cargado: %s', self.last_processed_packet)
        
    def get_games(self, client_id):
        """
        Returns the top 10 games dictionary.
        """
        game_playtimes = self.game_playtimes_by_client.get(client_id, {})

        # Ordenar los juegos por tiempo de juego descendente
        sorted_games = sorted(
            game_playtimes.items(),
            key=lambda item: item[1]["tiempo"],
            reverse=True
        )
        # Obtener los top 10
        top10 = sorted_games[:10]
        # Formatear la respuesta
        return {
            "top_10_indie_games_2010s": {
                "client_id " + str(client_id): [
                    {
                        "rank": idx + 1,
                        "name": game["nombre"],
                        "average_playtime_hours": round(game["tiempo"], 2)
                    } for idx, (game_id, game) in enumerate(top10)
                ]
         }
        }


    def process_game(self, game, packet_id):
        """
        Processes each message (game) received and adds it to the top 10 list if applicable, based on client_id.
        """
        try:
            
            client_id = game.client_id  # Obtener el client_id del juego
            game_id = game.id
            name = game.name
            playtime = int(game.apf)

            # Inicializar los tiempos de juego para este cliente si no existen
            if client_id not in self.game_playtimes_by_client:
                self.game_playtimes_by_client[client_id] = {i: {"nombre": None, "tiempo": None} for i in range(10)}

            game_playtimes = self.game_playtimes_by_client[client_id]
            aux_game_playtimes = game_playtimes
            
            menor_puesto = min(
                (k for k, v in game_playtimes.items() if v["tiempo"] is not None),
                key=lambda k: game_playtimes[k]["tiempo"],
                default=None,
            )

            puesto_vacio = next(
                (k for k, v in game_playtimes.items() if v["tiempo"] is None),
                None,
            )

            if puesto_vacio is not None:
                game_playtimes[puesto_vacio] = {"nombre": name, "tiempo": playtime}
            
            elif menor_puesto is not None and playtime > game_playtimes[menor_puesto]["tiempo"]:
                game_playtimes[menor_puesto] = {"nombre": name, "tiempo": playtime}

            serialized_dict = json.dumps(self.game_playtimes_by_client[client_id])
            value_to_store = f'{serialized_dict}|{packet_id}'
            self.fault_manager.update(f'top10_indie_counter_{client_id}', value_to_store)
            
            
        except Exception as e:
            logging.error(f"Error in process_game: {e}")

    def _process_callback(self, data):
        """
        Callback function to process messages.
        """
        aux = data.strip().split("\n")
        packet_id = aux[0]
        logging.debug(f"Received packet with ID: {packet_id}")
        
        if packet_id == self.last_processed_packet:
            logging.info(f"Packet {packet_id} has already been processed, skipping...")
            return
        
        json_row = json.loads(aux[1])
        game = Game.decode(json_row)
        self.process_game(game, packet_id)

    def _eof_callback(self, data):
        """
        Callback function for handling end of file (EOF) messages.
        """
        try:
            logging.info("End of file received. Sending top 10 indie games data for each client.")
            # Enviar el top 10 para cada cliente
            fin_msg = Fin.decode(data)
            client_id = int(fin_msg.client_id)
            self.middleware.send(json.dumps(self.get_games(client_id)))
            self.fault_manager.delete_key(f"top10_indie_counter_{client_id}")
            if client_id in self.game_playtimes_by_client:
                del self.game_playtimes_by_client[client_id]
            logging.info(f"Top 10 indie games data sent and memory cleared for client {client_id}.")
        except Exception as e:
            logging.error(f"Error in _eof_callback: {e}")

    def start(self):
        """
        Start middleware to begin consuming messages.
        """
        self.middleware.start()
