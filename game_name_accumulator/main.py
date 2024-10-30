import json
import logging
import os
import time
from accumulator import GameNamesAccumulator
import configparser

def main():
    logging.basicConfig(level=getattr(logging, os.getenv("LOGGING_LEVEL", "DEBUG")),
                        format='%(asctime)s - %(levelname)s - %(message)s')

    input_queues = json.loads(os.getenv("INPUT_QUEUES", "[]"))
    output_exchanges = json.loads(os.getenv("OUTPUT_EXCHANGES")) or []
    reviews_low_limit = int(os.getenv("REVIEWS_LOW_LIMIT", "10"))
    instance_id = os.getenv("INSTANCE_ID", '1')
    previous_language_nodes = os.getenv("PREVIOUS_LANGUAGE_NODES", "1")
    gameNamesAccumulator = GameNamesAccumulator(input_queues, 
    output_exchanges, instance_id, reviews_low_limit, previous_language_nodes)
    gameNamesAccumulator.start()

if __name__ == '__main__':
    main()
