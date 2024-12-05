import pika
import logging
import time
from common.fault_manager import FaultManager
from typing import Callable
from datetime import datetime, timedelta
import threading
import json

RABBITMQ_HOST = "rabbitmq"
RABBITMQ_PORT = 5672

REQUEUE = 2

class Middleware:
    def __init__(
        self,
        input_queues: dict[str, str] = {},
        output_queues: list[str] = [],
        output_exchanges: list[str] = [],
        intance_id: int = None,
        callback: Callable = None,
        eofCallback: Callable = None,
        faultManager: FaultManager = None,
        amount_output_instances: int = 1,
        exchange_output_type: str = "fanout",
        exchange_input_type: str = "fanout",
    ):
        self.exchange_output_type = exchange_output_type
        self.echange_input_type = exchange_input_type   
        self.amount_output_instances = amount_output_instances
        self.connection = self._connect_with_retries()
        self.channel = self.connection.channel()
        self.channel.basic_qos(prefetch_count=1)
        self.input_queues: dict[str, str] = {}
        for queue, exchange in input_queues.items():
            self.input_queues_aux = queue 
        self.output_queues = output_queues
        self.output_exchanges = output_exchanges
        self.intance_id = intance_id
        self.fault_manager = faultManager
        self.processed_packets = []
        self.init_state()
        self.callback = callback
        self.eofCallback = eofCallback
        self.auto_ack = False #sacarlo
        self._init_input_queues(input_queues)
        self._init_output_queues()
        self.start_persistence_cleaner()
        
    def _connect_with_retries(self, retries=5, delay=5):
        for attempt in range(retries):
            try:
                connection = pika.BlockingConnection(
                    pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT)
                )
                logging.info("Successfully connected to RabbitMQ")
                return connection
            except pika.exceptions.AMQPConnectionError as e:
                logging.warning(f"Connection attempt {attempt + 1} failed: {e}")
                if attempt < retries - 1:
                    time.sleep(delay)
                else:
                    logging.error("Max retries reached. Could not connect to RabbitMQ.")
                    raise

    def _init_input_queues(self, input_queues):
        for queue, exchange in input_queues.items():
            queue_name = f"{queue}_{self.intance_id}"

            self.channel.queue_declare(queue=queue_name, durable=True)
            if exchange:
                self.channel.exchange_declare(
                    exchange=exchange, exchange_type=self.echange_input_type
                )  # Cambiar a una variable
                logging.info(f"Binding {queue_name} to {exchange}")
                self.channel.queue_bind(exchange=exchange, queue=queue_name)

            callback_wrapper = self._create_callback_wrapper(
                self.callback, self.eofCallback
            )

            self.channel.basic_consume(
                queue=queue_name, on_message_callback=callback_wrapper, auto_ack=self.auto_ack
            )

            if queue_name not in self.input_queues:
                self.input_queues[queue_name] = exchange

    def _init_output_queues(self):
        logging.info("Creating output queues")
        if self.amount_output_instances <= 1:
            for queue in self.output_queues:
                self.channel.queue_declare(queue=queue, durable=True)
        if self.amount_output_instances > 1:
                for queue in self.output_queues:
                    logging.info(f"Creating output queues {queue}_0")
                    self.channel.queue_declare(queue=f"{queue}_0", durable=True)

        for exchange in self.output_exchanges:
            self.channel.exchange_declare(exchange=exchange, exchange_type=self.exchange_output_type)

    def send_to_requeue_positive(self, queue: str, data: str):
        self.channel.basic_publish(
            exchange="", routing_key="positive_review_queue_1", body=data
        )
        logging.debug("Sent to requeue %s: %s", queue, data)

    def send_to_requeue_negative(self, queue: str, data: str):
        self.channel.basic_publish(
            exchange="", routing_key="negative_review_queue_1", body=data
        )
        logging.debug("Sent to requeue %s: %s", queue, data)

    def _create_callback_wrapper(self, callback, eofCallback):

        def callback_wrapper(ch, method, properties, body):
            mensaje_str = body.decode("utf-8")
            
            if self.fault_manager is not None:
                self._callback_with_state(mensaje_str, method, callback, eofCallback)
            else:
                self._do_callback(mensaje_str, callback, eofCallback, method)
                self.ack(method.delivery_tag)

        return callback_wrapper
    
    def _do_callback(self, mensaje_str, callback, eofCallback, method):
        if "fin\n\n" in mensaje_str:
            eofCallback(mensaje_str)
        else:
            callback(mensaje_str)
    
    def _callback_with_state(self, mensaje_str, method, callback, eofCallback):
        mensaje_str_aux = mensaje_str.strip().split("\n")
        packet_id = mensaje_str_aux[0]
        #logging.info(f"Paquete recibido con ID: {packet_id}")
        if packet_id in self.processed_packets and not "fin" in packet_id:
            logging.info(f"Paquete {packet_id} ya ha sido procesado, saltando...")
            self.ack(method.delivery_tag)
            return
        
        self._do_callback(mensaje_str, callback, eofCallback, method)
        
        now = datetime.now()
        #logging.info(f"Paquete {packet_id} procesado a las {now}")
    
        self.fault_manager.append(f"middleware_{self.intance_id}_{self.input_queues_aux}", f'{packet_id}_{now.strftime("%Y%m%d%H%M%S")}')
        self.processed_packets.append(f'{packet_id}')
        self.ack(method.delivery_tag)
        

    def ack(self, delivery_tag):
        self.channel.basic_ack(delivery_tag=delivery_tag)

    def start(self):
        logging.info("Middleware Started!")
        try:
            if self.input_queues:
                self.channel.start_consuming()
        except OSError:
            logging.debug("Middleware shutdown")
        except pika.exceptions.ConnectionClosedByBroker:
            logging.debug("Connection closed")

    def send(self, data: str, instance_id: int = None, routing_key: str = ""):
        if self.amount_output_instances > 1:
            for queue in self.output_queues:
                self.send_to_queue(f"{queue}_0", data)
        for exchange in self.output_exchanges:
            self.channel.basic_publish(exchange=exchange, routing_key=routing_key, body=data)
            #logging.info("Sent to exchange %s: %s - %s", exchange, routing_key,data)

    def send_to_queue(self, queue: str, data: str):
        self.channel.basic_publish(exchange="", routing_key=queue, body=data)
        logging.debug("Sent to queue %s: %s", queue, data)
        
    def stop(self):
        if self.input_queues:
            self.channel.stop_consuming()
        if self.channel:
            self.channel.close()
        self.connection.close()
        logging.info("Middleware stopped consuming messages")
        
    def init_state(self):
        if self.fault_manager is not None:
            for key in self.fault_manager.get_keys(f"middleware_{self.intance_id}_{self.input_queues_aux}"):
                packet_id = self.fault_manager.get(key)
                packets = packet_id.strip().split("\n")
                self.processed_packets = packets
                
                #logging.info(f"Restaurando estado para el paquete {packet_id}")
            #logging.info(f'Paquetes procesados: {self.processed_packets}')
        
    def clean_persistence(self):
        """
        Remove processed packets older than 2 minutes from the persistence directory.
        """
        updated_aux = []
        for key in self.fault_manager.get_keys(f"middleware_{self.intance_id}_{self.input_queues_aux}"):
            state = self.fault_manager.get(key)
            if state is not None:
                now = datetime.now()
                packets = state.strip().split("\n")
                #logging.info(f"Restaurando estado para el paquete {state}")
                for packet in packets:
                    packet_time_str = packet.split("_")[-1]
                    if not packet_time_str:
                        logging.warning(f"Empty packet time string for packet: {packet}")
                        continue
                    packet_time = datetime.strptime(packet_time_str, "%Y%m%d%H%M%S")
                    if now - packet_time <= timedelta(minutes=2):
                        updated_aux.append(packet)
                    else:
                        pass
                        #logging.info(f"Removed outdated packet: {packet}")
                updated_aux_str = '\n'.join(updated_aux)
                self.fault_manager.update(f"middleware_{self.intance_id}_{self.input_queues_aux}", updated_aux_str)
        
            aux = []
            for packet in updated_aux:
                aux.append(packet.split("_")[0])
            self.processed_packets = aux
        
    def start_persistence_cleaner(self):
        if self.fault_manager is not None:
            def cleaner():
                while True:
                    try:
                        time.sleep(60)
                        self.clean_persistence()
                    except Exception as e:
                        logging.error(f"Error while cleaning persistence: {e}")
            cleaner_thread = threading.Thread(target=cleaner)
            cleaner_thread.start()
            logging.info("Persistence cleaner started")
    
        