import json
import logging
from common.middleware import Middleware
from common.utils import split_complex_string
from common.constants import REVIEWS_SCORE_POS

class PositivityFilter:
    def __init__(self, input_queue, positivity, output_exchange, instance_id):
        self.positivity = positivity
        self.middleware = Middleware(input_queue, [], output_exchange, instance_id, self._callback, self._finCallback)

    def start(self):
        self.middleware.start()
        logging.info("FilterPositivity started")
        
    def _callback(self, message):
        try:
            result_review = split_complex_string(message)
            review_score = result_review[REVIEWS_SCORE_POS]

            if int(review_score) == self.positivity:
                print(f"Sending + review: {result_review}", flush=True)
                self.middleware.send(",".join(result_review))
            
        except Exception as e:
            logging.error(f"Error in FilterPositivity callback: {e}")
            
    def _finCallback(self, message):
        self.middleware.send_message(message)
        logging.info("FilterPositivity finished")
        