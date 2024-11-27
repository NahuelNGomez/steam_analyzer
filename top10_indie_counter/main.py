import json
import logging
import os
from counter import Top10IndieCounter
import threading
from common.healthcheck import HealthCheckServer

def main():
    logging.basicConfig(
        level=getattr(logging, os.getenv("LOGGING_LEVEL", "DEBUG")),
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

    t1 = threading.Thread(target=counter.start)
    t1.start()
    HealthCheckServer([t1]).start()
    t1.join()

if __name__ == '__main__':
    main()
