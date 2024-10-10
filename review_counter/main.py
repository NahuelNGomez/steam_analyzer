import json
import logging
import os
import time
from review_counter import Top5ReviewCounter
import configparser

def send_message(queue, message, connection):
    channel = connection.channel()
    channel.queue_declare(queue=queue, durable=True)
    channel.basic_publish(exchange='', routing_key=queue, body=message)
    print(f'genreFilter envió {message} a la cola {queue}.', flush=True)
    
def main():
    logging.basicConfig(level=getattr(logging, os.getenv("LOGGING_LEVEL", "DEBUG")),
                        format='%(asctime)s - %(levelname)s - %(message)s')
    input_queues: dict = json.loads(os.getenv("INPUT_QUEUES")) or {}
    output_exchanges = json.loads(os.getenv("OUTPUT_EXCHANGES")) or []
    instance_id = json.loads(os.getenv("INSTANCE_ID") or '0')
    top5ReviewCounter = Top5ReviewCounter(input_queues, output_exchanges, instance_id)
    top5ReviewCounter.start()

if __name__ == '__main__':
    main()
