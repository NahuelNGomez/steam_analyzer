import json
import logging
import os
from counter import Top10IndieCounter

def load_config():
    import configparser
    config = configparser.ConfigParser()
    config.read('config.ini')
    return config['DEFAULT']

def main():
    config = load_config()
    logging.basicConfig(
        level=getattr(logging, config.get('LOGGING_LEVEL', 'INFO').upper()),
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    input_queues = os.getenv("INPUT_QUEUES")
    input_queues = json.loads(input_queues) if input_queues else {}
    
    output_queues = os.getenv("OUTPUT_QUEUES")
    output_queues = json.loads(output_queues) if output_queues else []

    output_exchanges = os.getenv("OUTPUT_EXCHANGES")
    output_exchanges = json.loads(output_exchanges) if output_exchanges else []

    instance_id = os.getenv("INSTANCE_ID")
    instance_id = json.loads(instance_id) if instance_id else 0

    counter = Top10IndieCounter(input_queues=input_queues, output_exchanges=output_exchanges, instance_id=instance_id)
    counter.start()

if __name__ == '__main__':
    main()
