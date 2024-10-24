import json
import logging
from common.middleware import Middleware
from common.packet_fin import Fin
from common.review import Review
from common.utils import split_complex_string
from common.constants import REVIEWS_APP_ID_POS, REVIEWS_APP_NAME_POS, REVIEWS_SCORE_POS


class PositivityFilter:
    def __init__(
        self, input_queue, positivity, output_exchange, instance_id, input_type
    ):
        self.positivity = positivity
        self.middleware = Middleware(
            input_queues=input_queue,
            output_queues=[],
            output_exchanges=output_exchange,
            intance_id=instance_id,
            callback=self._callback,
            eofCallback=self._finCallback,
            exchange_input_type=input_type,
        )
        self.batch_counter = 0

    def start(self):
        self.middleware.start()
        logging.info("FilterPositivity started")

    def _callback(self, message):
        try:
            batch = message.split("\n")
            finalList = ""
            

            for row in batch:
                if not row.strip():
                    continue
                result_review = Review.decode(json.loads(row))

                review_score = result_review.review_score
                if int(review_score) == self.positivity:
                    finalList += f"{json.dumps(result_review.getData())}\n"
            self.batch_counter += 1
            self.middleware.send(finalList)

        except Exception as e:
            logging.error(f"Error in FilterPositivity callback: {e}")

    def _finCallback(self, message):
        if not message:
            raise ValueError("Message is empty or None")
        print("Fin de la transmisión, enviando data", message, flush=True)
        json_row = Fin.decode(message)
        fin_message = Fin(self.batch_counter, json_row.client_id)
        #fin_message = "fin\n\n" + str(self.batch_counter)
        
        self.middleware.send(fin_message.encode())
        logging.info("FilterPositivity finished")
