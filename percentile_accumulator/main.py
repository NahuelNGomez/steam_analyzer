import json
import logging
import os
import time
from accumulator import PercentileAccumulator
import configparser

def main():
    logging.basicConfig(level=getattr(logging, os.getenv("LOGGING_LEVEL", "DEBUG")),
                        format='%(asctime)s - %(levelname)s - %(message)s')

    input_queues = json.loads(os.getenv("INPUT_QUEUES", "[]"))
    output_exchanges = json.loads(os.getenv("OUTPUT_EXCHANGES")) or []
    reviews_low_limit = int(os.getenv("PERCENTILE", "90"))
    instance_id = os.getenv("INSTANCE_ID", '1')
    percentileAccumulator = PercentileAccumulator(input_queues, 
    output_exchanges, instance_id, reviews_low_limit)
    percentileAccumulator.start()

if __name__ == '__main__':
    main()
