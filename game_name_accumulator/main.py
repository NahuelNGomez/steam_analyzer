import json
import logging
import os
from accumulator import GameNamesAccumulator
import threading
from common.healthcheck import HealthCheckServer

def main():
    logging.basicConfig(level=getattr(logging, os.getenv("LOGGING_LEVEL", "DEBUG")),
                        format='%(asctime)s - %(levelname)s - %(message)s')

    input_queues = json.loads(os.getenv("INPUT_QUEUES", "[]"))
    output_exchanges = json.loads(os.getenv("OUTPUT_EXCHANGES")) or []
    reviews_low_limit = int(os.getenv("REVIEWS_LOW_LIMIT", "10"))
    instance_id = os.getenv("INSTANCE_ID", '1')
    previous_language_nodes = os.getenv("PREVIOUS_LANGUAGE_NODES", "1")
    gameNamesAccumulator = GameNamesAccumulator(input_queues, output_exchanges, instance_id, reviews_low_limit, previous_language_nodes)

    t1 = threading.Thread(target=gameNamesAccumulator.start)
    t1.start()
    HealthCheckServer([t1]).start()
    t1.join()

if __name__ == '__main__':
    main()
