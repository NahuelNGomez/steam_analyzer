# gateway/rabbitMqHandler.py

import pika
import logging
import time

class RabbitMQHandler:
    def __init__(self, config, retries=5, delay=5):
        self.config = config 
        self.host = config.get('rabbitmq_HOST', 'localhost')
        self.port = int(config.get('rabbitmq_PORT', 5672))
        self.exchange = config.get('rabbitmq_EXCHANGE', '')
        self.exchange_type = config.get('rabbitmq_EXCHANGE_TYPE', 'direct')
        self.username = config.get('rabbitmq_USER', 'guest')  
        self.password = config.get('rabbitmq_PASS', 'guest')  
        self.virtual_host = config.get('rabbitmq_VIRTUAL_HOST', '/')
        self.ssl = config.getboolean('rabbitmq_SSL', False)
        self.connection = None
        self.channel = None
        self.retries = retries
        self.delay = delay
        self.connect()
    
    def connect(self):
        attempt = 0
        while attempt < self.retries:
            try:
                credentials = pika.PlainCredentials(self.username, self.password)
                
                if self.ssl:
                    ssl_options = pika.SSLOptions(
                        pika.ssl.create_default_context(cafile=self.config.get('rabbitmq_CA_FILE', None)),
                        self.host
                    )
                    parameters = pika.ConnectionParameters(
                        host=self.host,
                        port=self.port,
                        credentials=credentials,
                        virtual_host=self.virtual_host,
                        ssl=True,
                        ssl_options=ssl_options
                    )
                else:
                    parameters = pika.ConnectionParameters(
                        host=self.host,
                        port=self.port,
                        credentials=credentials,
                        virtual_host=self.virtual_host
                    )
                
                self.connection = pika.BlockingConnection(parameters)
                self.channel = self.connection.channel()
                self.channel.exchange_declare(exchange=self.exchange, exchange_type=self.exchange_type)
                logging.info("Conectado a RabbitMQ.")
                return
            except pika.exceptions.ProbableAuthenticationError as e:
                attempt += 1
                logging.error(f"Error de autenticación al conectar a RabbitMQ: {e}. Intento {attempt} de {self.retries}")
            except pika.exceptions.AMQPConnectionError as e:
                attempt += 1
                logging.error(f"Error de conexión AMQP al conectar a RabbitMQ: {e}. Intento {attempt} de {self.retries}")
            except Exception as e:
                attempt += 1
                logging.error(f"Error inesperado al conectar a RabbitMQ: {e}. Intento {attempt} de {self.retries}")
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
