# gateway/dispatcher.py
import json
import logging
import os
from rabbitMqHandler import RabbitMQHandler
from common.middleware import Middleware

input_queues: dict = json.loads(os.getenv("INPUT_QUEUES")) or {}
output_exchanges = json.loads(os.getenv("OUTPUT_EXCHANGES")) or []
instance_id = os.getenv("INSTANCE_ID", 0)

class Dispatcher:
    def __init__(self, config):
        self.middleware = Middleware(input_queues, [], output_exchanges, instance_id, self.get_data, self.dispatchFin)
        self.middleware.start()

    def dispatch(self, data, data_type):
        """
        data: Lista de diccionarios representando filas del CSV
        data_type: 'games' o 'reviews'
        """
        for row in data:
            message = json.dumps(row)
            logging.debug(f"Enviando mensaje {message}...")
            self.middleware.send_message(data = message)
            logging.debug(f"Dispatched message {message}")
            
    def dispatchFin(self):
        message = 'fin\n\n'
        logging.info("Enviando mensaje de fin a las colas...")
        self.middleware.send(data = message)
        self.middleware.send(data = message)
        logging.info("Fin de los mensajes de juegos y reviews.")
    
    def get_data(self, data):

        logging.info("Getted data!", data)
