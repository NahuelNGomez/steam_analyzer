import json
import logging
from common.middleware import Middleware
from common.packet_fin import Fin
from common.review import Review
from common.utils import split_complex_string
from common.constants import REVIEWS_APP_ID_POS, REVIEWS_APP_NAME_POS, REVIEWS_SCORE_POS
import gc

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

    def _cleanup_client_data(self, client_id):
        """Limpia todos los datos relacionados con un cliente específico y libera la memoria"""
        try:
            if client_id in self.batch_counter:
                del self.batch_counter[client_id]
            if client_id in self.expected_batches:
                del self.expected_batches[client_id]
            if client_id in self.null_counts:
                del self.null_counts[client_id]
                
            # Forzamos la liberación de memoria
            
            gc.collect()
            
            logging.info(f"Cleaned up data for client {client_id}")
        except Exception as e:
            logging.error(f"Error cleaning up data for client {client_id}: {e}")

    def _callback(self, message):
        try:
            batch = message.split("\n")
            finalList = []
            client_id = None

            for row in batch:
                if not row.strip():
                    continue
                result_review = Review.decode(json.loads(row))   #mandar los msj por csv
                review_score = result_review.review_score
                
                if int(review_score) == self.positivity:
                    finalList.append(json.dumps(result_review.getData()))

            if client_id not in self.batch_counter:
                self.batch_counter[client_id] = 0
            if client_id not in self.expected_batches:
                self.expected_batches[client_id] = 0
            if client_id not in self.null_counts:
                self.null_counts[client_id] = 0
                
            if finalList:
                finalList = "\n".join(finalList)
                self.middleware.send(finalList)
                self.batch_counter[client_id] += 1
                # Liberamos la memoria de finalList explícitamente
                del finalList
            else:
                self.null_counts[client_id] += 1
                                
            if ((self.batch_counter[client_id] + self.null_counts[client_id]) >= self.expected_batches[client_id]) and self.expected_batches[client_id] != 0:
                self.middleware.send(Fin(self.batch_counter[client_id], client_id).encode())
                self._cleanup_client_data(client_id)

            # Liberamos la memoria del batch explícitamente
            del batch

        except Exception as e:
            print(f"Error processing batch: {e}", flush=True)

    def _finCallback(self, message):
        if not message:
            raise ValueError("Message is empty or None")
        
        json_row = Fin.decode(message)
        client_id = int(json_row.client_id)
        
        if self.positivity == 1:
            remainder = int(json_row.batch_id) % 4
            division_result = int(json_row.batch_id) // 4
            self.expected_batches[client_id] = division_result + (1 if remainder >= self.instance_id else 0)
        else:
            self.expected_batches[client_id] = int(json_row.batch_id)
            
        print("Fin de la transmisión, enviando data", self.expected_batches[client_id], flush=True)
        
        if client_id not in self.batch_counter:
            print(f"Client id {client_id} not in batch_counter", flush=True)
            return
        
        if (self.batch_counter[client_id] + self.null_counts[client_id]) >= self.expected_batches[client_id]:
            self.middleware.send(Fin(self.batch_counter[client_id], client_id).encode())
            self._cleanup_client_data(client_id)
            
        # Liberamos la memoria del mensaje explícitamente
        del message
        del json_row
            
        logging.info("FilterPositivity finished")