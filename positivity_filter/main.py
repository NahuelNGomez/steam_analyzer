import json
import logging
import os
import time
from filter import PositivityFilter
import configparser
import threading
from common.healthcheck import HealthCheckServer
    
def main():
    logging.basicConfig(level=getattr(logging, os.getenv("LOGGING_LEVEL", "DEBUG")),
                        format='%(asctime)s - %(levelname)s - %(message)s')
    input_queues: dict = json.loads(os.getenv("INPUT_QUEUES")) or {}
    output_exchanges = json.loads(os.getenv("OUTPUT_EXCHANGES")) or []
    instance_id = json.loads(os.getenv("INSTANCE_ID") or '0')
    positivity = int(os.getenv("POSITIVITY"))
    
    positivityFilter = PositivityFilter(input_queues, positivity, output_exchanges, instance_id,'direct')

    t1 = threading.Thread(target=positivityFilter.start)
    t1.start()
    HealthCheckServer([t1]).start()
    t1.join()

if __name__ == '__main__':
    main()
