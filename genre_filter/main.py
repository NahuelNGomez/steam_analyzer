import json
import logging
import os
from filter import GenreFilter
from common.healthcheck import HealthCheckServer
import threading

def main():
    logging.basicConfig(level=getattr(logging, os.getenv("LOGGING_LEVEL", "DEBUG")),
                        format='%(asctime)s - %(levelname)s - %(message)s')
    input_queues: dict = json.loads(os.getenv("INPUT_QUEUES")) or {}
    output_exchanges = json.loads(os.getenv("OUTPUT_EXCHANGES")) or []
    instance_id = json.loads(os.getenv("INSTANCE_ID") or '0')
    genre = os.getenv("GENRE")
    
    gamesGenreFilter = GenreFilter(input_queues, output_exchanges, instance_id, genre)

    t1 = threading.Thread(target=gamesGenreFilter.start)
    t1.start()
    HealthCheckServer([t1]).start()
    t1.join()


if __name__ == '__main__':
    main()
