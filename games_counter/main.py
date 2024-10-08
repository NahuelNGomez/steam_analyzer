
# games_counter/main.py
import os
import pika
import json
import logging
import time
from collections import defaultdict
from counter import GamesCounter
import configparser

def send_message(queue, message, connection):
    channel = connection.channel()
    channel.queue_declare(queue=queue, durable=True)
    channel.basic_publish(exchange='', routing_key=queue, body=message)
    print(f'games counter envió result por la queue.', flush=True)
    
def main():
    logging.basicConfig(level=getattr(logging, os.getenv("LOGGING_LEVEL", "DEBUG")),
                        format='%(asctime)s - %(levelname)s - %(message)s')

    input_queues= json.loads(os.getenv("INPUT_QUEUES")) or {}
    output_exchanges = json.loads(os.getenv("OUTPUT_EXCHANGES")) or []
    instance_id = json.loads(os.getenv("INSTANCE_ID") or '0')
    gamesCounter = GamesCounter(input_queues, output_exchanges, instance_id)
    gamesCounter.start()

if __name__ == '__main__':
    main()
