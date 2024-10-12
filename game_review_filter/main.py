import json
import logging
import os
import time
from filter import GameReviewFilter
import configparser

def main():
    logging.basicConfig(level=getattr(logging, os.getenv("LOGGING_LEVEL", "DEBUG")),
                        format='%(asctime)s - %(levelname)s - %(message)s')
    input_game_queue = json.loads(os.getenv("INPUT_GAMES_QUEUE", "[]"))
    input_review_queue = json.loads(os.getenv("INPUT_REVIEWS_QUEUE", "[]"))
    output_exchanges = json.loads(os.getenv("OUTPUT_EXCHANGES")) or []
    previous_review_nodes = json.loads(os.getenv("PREVIOUS_REVIEW_NODES", "[]"))
    instance_id = '1'
    gameReviewFilter = GameReviewFilter(input_game_queue,input_review_queue, output_exchanges, [], instance_id,previous_review_nodes)
    gameReviewFilter.start()

if __name__ == '__main__':
    main()
