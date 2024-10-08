import pika
import logging
import time
from typing import Callable

RABBITMQ_HOST = 'rabbitmq'
RABBITMQ_PORT = 5672

REQUEUE = 2

class Middleware:
    def __init__(
        self,
        input_queues: dict[str, str] = {},
        output_queues: list[str] = [],
        output_exchanges: list[str] = [],
        intance_id: int = None,
        callback: Callable = None,
        eofCallback: Callable = None,
    ):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq',port=5672))
        self.channel = self.connection.channel()
        self.input_queues: dict[str, str] = {}
        self.output_queues = output_queues
        self.output_exchanges = output_exchanges
        self.intance_id = intance_id
        self.callback = callback
        self.eofCallback = eofCallback
        self._init_input_queues(input_queues)
        self._init_output_queues()

    def _init_input_queues(self, input_queues):
        for queue, exchange in input_queues.items():
            queue_name = f"{queue}_{self.intance_id}"

            self.channel.queue_declare(queue=queue_name, durable=True)
            if exchange:
                self.channel.exchange_declare(
                    exchange=exchange, exchange_type="fanout"
                )  # Cambiar a una variable
                print(f"Binding {queue_name} to {exchange}", flush=True)
                self.channel.queue_bind(exchange=exchange, queue=queue_name)

            callback_wrapper = self._create_callback_wrapper(
                self.callback, self.eofCallback
            )

            self.channel.basic_consume(
                queue=queue_name, on_message_callback=callback_wrapper, auto_ack=False
            )

            if queue_name not in self.input_queues:
                self.input_queues[queue_name] = exchange

    def _init_output_queues(self):
        for queue in self.output_queues:
            self.channel.queue_declare(queue=queue, durable=True)

        for exchange in self.output_exchanges:
            self.channel.exchange_declare(exchange=exchange, exchange_type="fanout")
            
    def send_to_requeue(self, queue: str, data: str):
        self.channel.basic_publish(exchange='', routing_key='positive_review_queue_1', body=data)
        logging.debug("Sent to requeue %s: %s", queue, data)

    def _create_callback_wrapper(self, callback, eofCallback):

        def callback_wrapper(ch, method, properties, body):
            response = 0
            print(f'[x] Recibido {body}', flush=True)
            mensaje_str = body.decode('utf-8')
            if mensaje_str == 'fin\n\n':
                eofCallback(body)
            else:
                response = callback(mensaje_str)
            if response == 2:
                self.send_to_requeue(method.routing_key, body)
            self.ack(method.delivery_tag)

        return callback_wrapper
    
    def ack(self, delivery_tag):
        self.channel.basic_ack(delivery_tag=delivery_tag)

    def start(self):
        logging.info("Middleware Started!")
        try:
            if self.input_queues:
                self.channel.start_consuming()
        except OSError:
            logging.debug("Middleware shutdown")
        except pika.exceptions.ConnectionClosedByBroker:
            logging.debug("Connection closed")

    def send(self, data: str, instance_id: int = None):
        suffix = f"_{instance_id}" if instance_id is not None else ""
        for queue in self.output_queues:
            self.send_to_queue(f"{queue}_{suffix}", data)

        for exchange in self.output_exchanges:
            self.channel.basic_publish(exchange=exchange, routing_key="", body=data)
            logging.debug("Sent to exchange %s: %s", exchange, data)
