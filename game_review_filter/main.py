import json
import logging
import os
from filter import GameReviewFilter
from common.healthcheck import HealthCheckServer
import threading

def main():
    logging.basicConfig(level=getattr(logging, os.getenv("LOGGING_LEVEL", "DEBUG")),
                        format='%(asctime)s - %(levelname)s - %(message)s')
    input_game_queue = json.loads(os.getenv("INPUT_GAMES_QUEUE", "[]"))
    input_review_queue = json.loads(os.getenv("INPUT_REVIEWS_QUEUE", "[]"))
    output_exchanges = json.loads(os.getenv("OUTPUT_EXCHANGES")) or []
    previous_review_nodes = json.loads(os.getenv("PREVIOUS_REVIEW_NODES", "[]"))
    amount_of_language_filters = int(os.getenv("AMOUNT_OF_LANGUAGE_FILTERS", "0"))
    instance_id = '1'
    gameReviewFilter = GameReviewFilter(input_game_queue,input_review_queue, output_exchanges, [], instance_id,previous_review_nodes, amount_of_language_filters)

    gameReviewFilter.start()
    threads_to_check = [gameReviewFilter.games_receiver, gameReviewFilter.reviews_receiver]
    HealthCheckServer(threads_to_check).start()

if __name__ == '__main__':
    main()
