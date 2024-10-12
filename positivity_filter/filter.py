import json
import logging
from common.middleware import Middleware
from common.review import Review
from common.utils import split_complex_string
from common.constants import REVIEWS_APP_ID_POS, REVIEWS_APP_NAME_POS, REVIEWS_SCORE_POS

class PositivityFilter:
    def __init__(self, input_queue, positivity, output_exchange, instance_id,  input_type):
        self.positivity = positivity
        self.middleware = Middleware(input_queues=input_queue, output_queues=[], output_exchanges=output_exchange, intance_id=instance_id, callback=self._callback, eofCallback=self._finCallback, exchange_input_type=input_type)
        self.counter = 0
        self.id_counter = 0
        self.name_counter = 0
        self.name = "The Plan"

    def start(self):
        self.middleware.start()
        logging.info("FilterPositivity started")
        
    def _callback(self, message):
        try:
            batch = message.split("\n")

            for row in batch:
                if not row.strip():
                        continue
                result_review = Review.decode(json.loads(row))
                review_score = result_review.review_score
                if int(review_score) == self.positivity: # Cast?
                    if result_review.app_name == self.name:
                        self.name_counter += 1
                        print(f"Game name The Plan found {self.name_counter} - times", flush=True)
                    self.middleware.send(json.dumps(result_review.getData()))
                else:
                    if result_review.game_id == '250600' or result_review.game_id == 250600 or int(result_review.game_id) == 250600:
                        self.id_counter += 1
                        print(f"Game ID 250600 found {self.id_counter}-{review_score} - times", flush=True)

        except Exception as e:
            logging.error(f"Error in FilterPositivity callback: {e}")
            
    def _finCallback(self, message):
        self.middleware.send(message)
        logging.info("FilterPositivity finished")
        