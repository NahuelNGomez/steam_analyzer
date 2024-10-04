# gateway/rabbitMqHandler.py

import pika
import logging
import time


class RabbitMQHandler:
    def __init__(self, config, retries=5, delay=5):
        self.config = config  # Almacenar config como un atributo de instancia
        self.host = config.get("rabbitmq_HOST", "rabbitmq")
        self.port = int(config.get("rabbitmq_PORT", 5672))
        self.exchange = config.get("rabbitmq_EXCHANGE", "dispatcher_exchange")
        self.ssl = config.getboolean("rabbitmq_SSL", False)
        self.connection = None
        self.channel = None
        self.retries = retries
        self.delay = delay
        self.connect()

    def connect(self):
        attempt = 0
        while attempt < self.retries:
            try:
                parameters = pika.ConnectionParameters(
                        host=self.host,
                        port=self.port,
                )
                self.connection = pika.BlockingConnection(parameters)
                self.channel = self.connection.channel()
                self.channel.exchange_declare(
                    exchange=self.exchange, exchange_type='direct'
                )
                logging.info("Conectado a RabbitMQ.")
                return
            except pika.exceptions.ProbableAuthenticationError as e:
                attempt += 1
                logging.error(
                    f"Error de autenticación al conectar a RabbitMQ: {e}. Intento {attempt} de {self.retries}"
                )
            except pika.exceptions.AMQPConnectionError as e:
                attempt += 1
                logging.error(
                    f"Error de conexión AMQP al conectar a RabbitMQ: {e}. Intento {attempt} de {self.retries}"
                )
            except Exception as e:
                attempt += 1
                logging.error(
                    f"Error inesperado al conectar a RabbitMQ: {e}. Intento {attempt} de {self.retries}"
                )
            time.sleep(self.delay)
        logging.critical("No se pudo conectar a RabbitMQ después de varios intentos.")
        raise ConnectionError("Failed to connect to RabbitMQ")

    def send_message(self, queue_name, message):
        try:
            # Declarar la cola - Si ya existe, no se creará una nueva
            self.channel.queue_declare(queue=queue_name, durable=True)

            # Enlazar la cola al intercambio con la clave de enrutamiento correspondiente
            self.channel.queue_bind(
                queue=queue_name, exchange=self.exchange, routing_key=queue_name
            )

            # Publicar el mensaje
            self.channel.basic_publish(
                exchange=self.exchange,
                routing_key=queue_name,
                body=message,
                properties=pika.BasicProperties(
                    delivery_mode=2,  # Hacer el mensaje persistente
                ),
            )
            logging.debug(
                f"Mensaje enviado a {queue_name}: {message[:50]}..."
            )  # Mostrar solo los primeros 50 caracteres
        except Exception as e:
            logging.error(f"Error al enviar mensaje a {queue_name}: {e}")

    def close(self):
        if self.connection and self.connection.is_open:
            self.connection.close()
            logging.info("Conexión a RabbitMQ cerrada.")
    

    def get_messages(self, queue_name):
        message_body = None  # Variable para almacenar el mensaje

        def callback(ch, method, properties, body):
            nonlocal message_body  # Permitir modificar la variable externa
            print(" [x] Received %r" % body, flush=True)
            message_body = body  # Guardar el mensaje
            ch.stop_consuming()  # Detener el consumo sin cancelar la cola


        # Declarar la cola (asegurarse de que existe)
        self.channel.queue_declare(queue=queue_name, durable=True)

        # Consumir mensajes usando el callback
        self.channel.basic_consume(
            queue=queue_name, on_message_callback=callback, auto_ack=True
        )
        
        # Iniciar el consumo y esperar hasta que se reciba un mensaje
        self.channel.start_consuming()
    
        return message_body  # Retornar el mensaje después de detener el consumo
