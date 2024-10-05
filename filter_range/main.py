# filter_indie/main.py
import json
import logging
import os
import time
from filter import FilterRange
import configparser

def send_message(queue, message, connection):
    channel = connection.channel()
    channel.queue_declare(queue=queue, durable=True)
    channel.basic_publish(exchange='', routing_key=queue, body=message)
    print(f'filter_indie envió {message} a la cola {queue}.', flush=True)
    

def main():
    logging.basicConfig(level=getattr(logging, os.getenv("LOGGING_LEVEL", "DEBUG")),
                        format='%(asctime)s - %(levelname)s - %(message)s')
    input_queues: dict = json.loads(os.getenv("INPUT_QUEUES")) or {}
    output_exchanges = json.loads(os.getenv("OUTPUT_EXCHANGES")) or []
    instance_id = json.loads(os.getenv("INSTANCE_ID") or '0')

    filter_range = FilterRange(2010, 2019, input_queues, output_exchanges, instance_id)
    filter_range.start()

if __name__ == '__main__':
    main()
