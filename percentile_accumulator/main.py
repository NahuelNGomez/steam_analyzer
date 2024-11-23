import json
import logging
import os
from accumulator import PercentileAccumulator
import threading
from common.healthcheck import HealthCheckServer

def main():
    logging.basicConfig(level=getattr(logging, os.getenv("LOGGING_LEVEL", "DEBUG")),
                        format='%(asctime)s - %(levelname)s - %(message)s')

    input_queues = json.loads(os.getenv("INPUT_QUEUES", "[]"))
    output_exchanges = json.loads(os.getenv("OUTPUT_EXCHANGES")) or []
    reviews_low_limit = int(os.getenv("PERCENTILE", "90"))
    instance_id = os.getenv("INSTANCE_ID", '1')
    percentileAccumulator = PercentileAccumulator(input_queues, 
    output_exchanges, instance_id, reviews_low_limit)
  
    t1 = threading.Thread(target=percentileAccumulator.start)
    t1.start()
    HealthCheckServer([t1]).start()
    t1.join()

if __name__ == '__main__':
    main()
