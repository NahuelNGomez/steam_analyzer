import pika
import logging
import time
from typing import Callable

RABBITMQ_HOST = 'rabbitmq'
RABBITMQ_PORT = 5672


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

    def _create_callback_wrapper(self, callback, eofCallback):

        def callback_wrapper(ch, method, properties, body):
            if body == b"fin\n\n":
                eofCallback(body)
            else:
                callback(body)

        return callback_wrapper

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
