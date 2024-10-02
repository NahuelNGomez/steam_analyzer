# gateway/dispatcher.py
import json
import logging
from rabbitMqHandler import RabbitMQHandler

class Dispatcher:
    def __init__(self, config):
        self.rabbitmq = RabbitMQHandler(config)
        self.games_queue = config['rabbitmq_GAMES_QUEUE']
        self.reviews_queue = config['rabbitmq_REVIEWS_QUEUE']
        self.result_queue = config['rabbitmq_RESULT_QUEUE']

    def dispatch(self, data, data_type):
        """
        data: Lista de diccionarios representando filas del CSV
        data_type: 'games' o 'reviews'
        """
        queue = self.games_queue if data_type == 'games' else self.reviews_queue
        for row in data:
            message = json.dumps(row)
            logging.debug(f"Enviando mensaje a {queue}: {message}...")
            self.rabbitmq.send_message(queue, message)
            logging.debug(f"Dispatched message to {queue}: {message}")
            
    def dispatchFin(self):
        message = 'fin\n\n'
        logging.info("Enviando mensaje de fin a las colas...")
        self.rabbitmq.send_message(self.games_queue, message)
        self.rabbitmq.send_message(self.reviews_queue, message)
        logging.info("Fin de los mensajes de juegos y reviews.")
    
    def get_data(self):

        queue = self.result_queue
        return self.rabbitmq.get_messages(queue)
