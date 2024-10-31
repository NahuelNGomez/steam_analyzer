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
        self.batch_counter: dict = {}
        self.expected_batches: dict = {}
        if len(output_exchange[0].split("_")) > 2:
            self.instance_id = int(output_exchange[0].split("_")[-1])
        else:
            self.instance_id = 1
        self.null_counts = {}
        
    def start(self):
        self.middleware.start()
        logging.info("FilterPositivity started")

    def _callback(self, message):
        try:
            batch = message.split("\n")
            finalList = []

            for row in batch:
                if not row.strip():
                    continue
                result_review = Review.decode(json.loads(row))
                review_score = result_review.review_score
                
                if int(review_score) == self.positivity:
                    finalList.append(json.dumps(result_review.getData()))

            client_id = int(Review.decode(json.loads(batch[0])).client_id)

            if client_id not in self.batch_counter:
                # print(f"Client id {client_id} not in batch_counter", flush=True)
                self.batch_counter[client_id] = 0
            if client_id not in self.expected_batches:
                # print(f"Client id {client_id} not in expected_batches", flush=True)
                self.expected_batches[client_id] = 0
            if client_id not in self.null_counts:
                # print(f"Client id {client_id} not in null_counts", flush=True)
                self.null_counts[client_id] = 0
                
            if finalList:
                # Unir todas las líneas con \n solo cuando hay contenido válido
                finalList = "\n".join(finalList)
                self.middleware.send(finalList)
                self.batch_counter[client_id] += 1
            else:
                self.null_counts[client_id] += 1
                # print(f"Null count: {self.null_counts[client_id]}", flush=True)
                                
            # print(f"Batch counter: {self.batch_counter[client_id]}", flush=True)
            if ((self.batch_counter[client_id] + self.null_counts[client_id]) >= self.expected_batches[client_id]) and self.expected_batches[client_id] !=0:
                self.middleware.send(Fin(self.batch_counter[client_id], client_id).encode())

        except Exception as e:
            print(f"Error processing batch: {e}", flush=True)

    def _finCallback(self, message):
        if not message:
            raise ValueError("Message is empty or None")
        
        json_row = Fin.decode(message)
        # fin_message = Fin(self.batch_counter[int(json_row.client_id)], json_row.client_id)
        # print("llega batch id: ", json_row.batch_id,flush=True)
        if self.positivity == 1:
            remainder = int(json_row.batch_id) % 4
            division_result = int(json_row.batch_id) // 4
            self.expected_batches[int(json_row.client_id)] = division_result + (1 if remainder >= self.instance_id else 0)
        else:
            self.expected_batches[int(json_row.client_id)] = int(json_row.batch_id)
        print("Fin de la transmisión, enviando data", self.expected_batches[int(json_row.client_id)], flush=True)
        if int(json_row.client_id) not in self.batch_counter:
            print(f"Client id {json_row.client_id} not in batch_counter", flush=True)
            #self.middleware.send(message)
            return
        
        if (self.batch_counter[int(json_row.client_id)] + self.null_counts[int(json_row.client_id)]) >= self.expected_batches[int(json_row.client_id)]:
            self.middleware.send(Fin(self.batch_counter[int(json_row.client_id)], int(json_row.client_id)).encode())
        logging.info("FilterPositivity finished")
