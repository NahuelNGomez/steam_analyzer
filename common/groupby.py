# common/groupby_processor.py

import pika
import json
import logging

class GenericGroupByProcessor:
    def __init__(self, input_queue, output_queue, group_by_key, aggregation_func):
        """
        :param input_queue: Name of the input queue.
        :param output_queue: Name of the output queue.
        :param group_by_key: The key to group by.
        :param aggregation_func: Function to apply to each group.
        """
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.group_by_key = group_by_key
        self.aggregation_func = aggregation_func
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
        self.channel = self.connection.channel()

        # Declare queues
        self.channel.queue_declare(queue=self.input_queue, durable=True)
        self.channel.queue_declare(queue=self.output_queue, durable=True)

        # Initialize data structures
        self.groups = {}
        self.eof_received = False

    def start(self):
        self.channel.basic_consume(queue=self.input_queue, on_message_callback=self.callback)
        logging.info(f"Starting GenericGroupByProcessor consuming from {self.input_queue}")
        self.channel.start_consuming()

    def callback(self, ch, method, properties, body):
        message = json.loads(body)
        if message.get('type') == 'EOF':
            self.eof_received = True
            ch.basic_ack(delivery_tag=method.delivery_tag)
            self.process_groups()
            return

        group_key = message.get(self.group_by_key)
        if group_key is None:
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        if group_key not in self.groups:
            self.groups[group_key] = []
        self.groups[group_key].append(message)
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def process_groups(self):
        for group_key, messages in self.groups.items():
            aggregated_result = self.aggregation_func(group_key, messages)
            self.publish_aggregated_result(aggregated_result)
        # Send EOF to output queue
        self.channel.basic_publish(
            exchange='',
            routing_key=self.output_queue,
            body=json.dumps({'type': 'EOF'}),
            properties=pika.BasicProperties(
                delivery_mode=2,
            )
        )
        self.channel.stop_consuming()

    def publish_aggregated_result(self, result):
        self.channel.basic_publish(
            exchange='',
            routing_key=self.output_queue,
            body=json.dumps(result),
            properties=pika.BasicProperties(
                delivery_mode=2,
            )
        )
