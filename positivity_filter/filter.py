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
                self.counter += 1
                print("Juegos procesados: ", self.counter, flush=True)
                
                review_score = result_review.review_score
                if int(review_score) == self.positivity: # Cast?
                    self.middleware.send(json.dumps(result_review.getData()))

        except Exception as e:
            logging.error(f"Error in FilterPositivity callback: {e}")
            
    def _finCallback(self, message):
        self.middleware.send(message)
        logging.info("FilterPositivity finished")
        