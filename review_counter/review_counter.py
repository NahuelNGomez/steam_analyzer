import json
import logging
from common.game_review import GameReview
from common.middleware import Middleware
from common.packet_fin import Fin

class Top5ReviewCounter:
    def __init__(self, input_queues, output_exchanges, instance_id):
        self.middleware = Middleware(
            input_queues=input_queues,
            output_queues=[],
            output_exchanges=output_exchanges,
            intance_id=instance_id,
            callback=self._process_callback,
            eofCallback=self._eof_callback,
            exchange_input_type="direct",
            
        )
        # Diccionario para almacenar los datos por client_id
        self.games_dict_by_client = {}
        self.remaining_fin : dict = {}

    def get_games(self, client_id):
        """
        Returns the top 5 games with the most positive reviews for a specific client.
        """
        games_dict = self.games_dict_by_client.get(client_id, {})

        top_5_games = sorted(
            games_dict.items(),
            key=lambda x: x[1]['count'],
            reverse=True
        )[:5]
        return {
            "top_5_indie_games_positive_reviews": {
                "client_id " + str(client_id): [
                    {
                        "rank": idx + 1,
                        "name": game_data["name"],
                        "positive_review_count": game_data["count"]
                    } for idx, (game_id, game_data) in enumerate(top_5_games)
                ]
            }
        }

    def process_game(self, game_review):
        """
        Processes each game_review received and adds it to the top 5 list, based on client_id.
        """
        try:
            client_id = game_review.client_id  # Obtener el client_id del game_review
            game_id = game_review.game_id
            name = game_review.game_name

            # Inicializar el diccionario de juegos para este cliente si no existe
            if client_id not in self.games_dict_by_client:
                self.games_dict_by_client[client_id] = {}

            games_dict = self.games_dict_by_client[client_id]

            if game_id in games_dict:
                games_dict[game_id]['count'] += 1
            else:
                games_dict[game_id] = {
                    'name': name,
                    'count': 1
                }
        except Exception as e:
            logging.error(f"Error in process_game: {e}")

    def _process_callback(self, data):
        """
        Callback function to process messages.
        """
        batch = data.split("\n")
        for row in batch:
            if not row.strip():
                continue
            json_data = json.loads(row)
            game_review = GameReview.decode(json_data)
            self.process_game(game_review)

    def _eof_callback(self, data):
        """
        Callback function for handling end of file (EOF) messages.
        """
        fin_msg = Fin.decode(data)
        client_id = fin_msg.client_id
        if client_id not in self.remaining_fin:
            self.remaining_fin[client_id] = 3
        else:
            self.remaining_fin[client_id] -= 1
        if self.remaining_fin[client_id] > 0:
            return
        logging.info("End of file received. Sending top 5 indie games positive reviews data for each client.")
        
        top_5_games = self.get_games(client_id)
        
        self.middleware.send(json.dumps(top_5_games))
        # for client_id in self.games_dict_by_client:
        #     top5_games = json.dumps(self.get_games(client_id), indent=4)
        #     self.middleware.send(top5_games)
        logging.info(f"Top 5 indie games positive reviews data sent for client {client_id}.")
    
        

    def start(self):
        """
        Start middleware to begin consuming messages.
        """
        self.middleware.start()
