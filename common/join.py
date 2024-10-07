# common/join_processor.py

import pika
import json
import logging
from threading import Thread
from collections import defaultdict

class GenericJoinProcessor:
    def __init__(self, input_queues, output_queue, join_key, join_type='inner'):
        """
        :param input_queues: List of input queue names.
        :param output_queue: Name of the output queue.
        :param join_key: The key to join on.
        :param join_type: Type of join ('inner', 'left', 'right', 'full').
        """
        self.input_queues = input_queues
        self.output_queue = output_queue
        self.join_key = join_key
        self.join_type = join_type.lower()
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
        self.channel = self.connection.channel()

        # Declare queues
        for queue in self.input_queues:
            self.channel.queue_declare(queue=queue, durable=True)
        self.channel.queue_declare(queue=self.output_queue, durable=True)

        # Initialize data structures
        self.buffers = {queue: defaultdict(list) for queue in self.input_queues}
        self.eos_received = {queue: False for queue in self.input_queues}
        self.all_eos_received = False

    def start(self):
        threads = []
        for queue in self.input_queues:
            t = Thread(target=self.consume_queue, args=(queue,))
            t.start()
            threads.append(t)
        for t in threads:
            t.join()
        # After all threads finish
        self.channel.close()
        self.connection.close()

    def consume_queue(self, queue_name):
        self.channel.basic_consume(queue=queue_name, on_message_callback=lambda ch, method, properties, body: self.callback(queue_name, ch, method, properties, body))
        self.channel.start_consuming()

    def callback(self, queue_name, ch, method, properties, body):
        message = json.loads(body)
        if message.get('type') == 'EOF':
            self.eos_received[queue_name] = True
            ch.basic_ack(delivery_tag=method.delivery_tag)
            self.check_eos()
            return

        join_value = message.get(self.join_key)
        if join_value is None:
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        self.buffers[queue_name][join_value].append(message)
        self.attempt_join(join_value)
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def attempt_join(self, join_value):
        # Check if join_value exists in all buffers
        if self.join_type == 'inner':
            if all(join_value in self.buffers[queue] for queue in self.input_queues):
                # Perform Cartesian product of messages with same join_value
                from itertools import product
                message_lists = [self.buffers[queue][join_value] for queue in self.input_queues]
                for messages in product(*message_lists):
                    joined_message = {}
                    for m in messages:
                        joined_message.update(m)
                    self.publish_joined_message(joined_message)
                # Remove the join_value from buffers to free memory
                for queue in self.input_queues:
                    del self.buffers[queue][join_value]
        # Implement other join types (left, right, full) as needed

    def publish_joined_message(self, message):
        self.channel.basic_publish(
            exchange='',
            routing_key=self.output_queue,
            body=json.dumps(message),
            properties=pika.BasicProperties(
                delivery_mode=2,
            )
        )

    def check_eos(self):
        if all(self.eos_received.values()):
            # All input queues have sent EOF
            # Handle remaining messages based on join type
            if self.join_type in ('left', 'full'):
                # For left and full joins, process remaining messages
                self.process_remaining()
            # Send EOF to output queue
            self.channel.basic_publish(
                exchange='',
                routing_key=self.output_queue,
                body=json.dumps({'type': 'EOF'}),
                properties=pika.BasicProperties(
                    delivery_mode=2,
                )
            )
            for queue in self.input_queues:
                self.channel.stop_consuming()
            logging.info("All input queues have sent EOF. Join processing completed.")

    def process_remaining(self):
        # Implement logic to process remaining messages based on join type
        pass
