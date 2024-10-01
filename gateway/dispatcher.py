# gateway/dispatcher.py
import json
import logging
from rabbitMqHandler import RabbitMQHandler

class Dispatcher:
    def __init__(self, config):
        self.rabbitmq = RabbitMQHandler(config)
        self.games_queue = config['rabbitmq_GAMES_QUEUE']
        self.reviews_queue = config['rabbitmq_REVIEWS_QUEUE']

    def dispatch(self, data, data_type):
        """
        data: Lista de diccionarios representando filas del CSV
        data_type: 'games' o 'reviews'
        """
        queue = self.games_queue if data_type == 'games' else self.reviews_queue
        for row in data:
            message = json.dumps(row)
            self.rabbitmq.send_message(queue, message)
            logging.debug(f"Dispatched message to {queue}: {message}")
