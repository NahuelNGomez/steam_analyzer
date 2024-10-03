# top10_indie_counter/main.py

import pika
import json
import logging
import time
from counter import Top10IndieCounter
import configparser

def load_config():
    config = configparser.ConfigParser()
    config.read('config.ini')  
    return config['DEFAULT']

def send_message(queue, message, connection):
    channel = connection.channel()
    channel.queue_declare(queue=queue, durable=True)
    channel.basic_publish(exchange='', routing_key=queue, body=message)
    print(f'top10_indie_counter envió result por la cola {queue}.', flush=True)

def main():
    config = load_config()
    logging.basicConfig(level=getattr(logging, config.get('LOGGING_LEVEL', 'INFO').upper()),
                        format='%(asctime)s - %(levelname)s - %(message)s')

    rabbitmq_host = config.get('rabbitmq_HOST', 'rabbitmq')
    rabbitmq_port = int(config.get('rabbitmq_PORT', 5672))
    input_queue = config.get('rabbitmq_INDI_GAMES_IN_RANGE_QUEUE', 'indie_games_in_range')
    results_queue = config.get('rabbitmq_RESULTS_QUEUE', 'results_queue')  # Cola para enviar el resultado
    rabbitmq_user = config.get('rabbitmq_USER', 'admin')
    rabbitmq_pass = config.get('rabbitmq_PASS', 'admin')

    retries = 5
    delay = 10
    attempt = 0
    connection = None

    credentials = pika.PlainCredentials(rabbitmq_user, rabbitmq_pass)

    while attempt < retries:
        try:
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=rabbitmq_host,
                    port=rabbitmq_port,
                    credentials=credentials
                )
            )
            logging.info("Conectado a RabbitMQ.")
            break
        except Exception as e:
            attempt += 1
            logging.error(f"Error al conectar a RabbitMQ: {e}. Intento {attempt} de {retries}")
            time.sleep(delay)
    else:
        logging.critical("No se pudo conectar a RabbitMQ después de varios intentos.")
        return

    channel = connection.channel()
    channel.queue_declare(queue=input_queue, durable=True)
    channel.queue_declare(queue=results_queue, durable=True)
    logging.info(f"Declaradas las colas: {input_queue}, {results_queue}")

    counter = Top10IndieCounter()

    def callback(ch, method, properties, body):
        try:
            logging.debug(f"Recibido mensaje: {body}...")
            print(f'[x] Recibido {body}', flush=True)
            message = json.loads(body.decode('utf-8'))
            logging.debug(f"Mensaje decodificado: {message}")

            if 'fin' in message:
                logging.info("Fin de los mensajes de juegos.")
                print(f"Fin de los mensajes de juegos.", flush=True)
                ch.basic_cancel(consumer_tag=method.consumer_tag)
                return

            counter.process_game(message)
            logging.info(f"Procesado mensaje: {message}")
            print(f"Procesado mensaje: {message}", flush=True)
        except Exception as e:
            logging.error(f"Error al procesar el mensaje: {e}")

    channel.basic_consume(queue=input_queue, on_message_callback=callback, auto_ack=True)
    logging.info(' [*] Esperando mensajes en indie_games_in_range. Para salir presiona CTRL+C')

    try:
        channel.start_consuming()
        logging.info("Consumidor iniciado.")
        print(f'Consumidor termino de consumir.', flush=True)
        send_message('results_queue', counter.calculate_top10(), connection)
        print(f'top10_indie_counter envió result por la cola results_queue.', flush=True)

    except KeyboardInterrupt:
        logging.info('Interrumpido por el usuario.')
        channel.stop_consuming()
    finally:
        connection.close()
        logging.info("Conexión a RabbitMQ cerrada.")

if __name__ == '__main__':
    main()
