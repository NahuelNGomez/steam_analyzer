# gateway/rabbitMqHandler.py

import pika
import logging
import configparser
import time

class RabbitMQHandler:
    def __init__(self, config, retries=5, delay=5):
        self.host = config['rabbitmq_HOST']
        self.port = int(config['rabbitmq_PORT'])
        self.exchange = config.get('rabbitmq_EXCHANGE', '')
        self.exchange_type = config.get('rabbitmq_EXCHANGE_TYPE', 'direct')
        self.connection = None
        self.channel = None
        self.retries = retries
        self.delay = delay
        self.connect()
    
    def connect(self):
        attempt = 0
        while attempt < self.retries:
            try:
                self.connection = pika.BlockingConnection(
                    pika.ConnectionParameters(host=self.host, port=self.port)
                )
                self.channel = self.connection.channel()
                self.channel.exchange_declare(exchange=self.exchange, exchange_type=self.exchange_type)
                logging.info("Conectado a RabbitMQ.")
                return
            except Exception as e:
                attempt += 1
                logging.error(f"Error al conectar a RabbitMQ: {e}. Intento {attempt} de {self.retries}")
                time.sleep(self.delay)
        logging.critical("No se pudo conectar a RabbitMQ después de varios intentos.")
        raise ConnectionError("Failed to connect to RabbitMQ")
    
    def send_message(self, queue_name, message):
        try:
            self.channel.queue_declare(queue=queue_name, durable=True)
            self.channel.basic_publish(
                exchange=self.exchange,
                routing_key=queue_name,
                body=message,
                properties=pika.BasicProperties(
                    delivery_mode=2,  # Hacer el mensaje persistente
                )
            )
            logging.debug(f"Mensaje enviado a {queue_name}: {message[:50]}...")  # Mostrar solo los primeros 50 caracteres
        except Exception as e:
            logging.error(f"Error al enviar mensaje a {queue_name}: {e}")
    
    def close(self):
        if self.connection and self.connection.is_open:
            self.connection.close()
            logging.info("Conexión a RabbitMQ cerrada.")
