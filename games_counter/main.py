# games_counter/main.py

import pika
import json
import logging
import time
from collections import defaultdict
from counter import GamesCounter
import configparser

def load_config():
    config = configparser.ConfigParser()
    config.read('config.ini')  
    return config['DEFAULT']

def main():
    config = load_config()
    logging.basicConfig(level=getattr(logging, config.get('LOGGING_LEVEL', 'INFO').upper()),
                        format='%(asctime)s - %(levelname)s - %(message)s')

    rabbitmq_host = config.get('rabbitmq_HOST', 'rabbitmq')
    rabbitmq_port = int(config.get('rabbitmq_PORT', 5672))
    games_queue = config.get('rabbitmq_GAMES_QUEUE', 'games_queue')
    rabbitmq_user = config.get('rabbitmq_USER', 'guest')
    rabbitmq_pass = config.get('rabbitmq_PASS', 'guest')

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
    channel.queue_declare(queue=games_queue, durable=True)
    logging.info(f"Declarada la cola: {games_queue}")

    counter = GamesCounter()

    def callback(ch, method, properties, body):
        try:
            logging.debug(f"Recibido mensaje: {body}...")
            print(f'[x] Recibido {body}', flush=True)
            message = json.loads(body.decode('utf-8'))
            counter.counterGames(message)  
            logging.info(f"Procesado mensaje: {message}")
            print(f"Procesado mensaje: {message}", flush=True)
        except Exception as e:
            logging.error(f"Error al procesar el mensaje: {e}")

    channel.basic_consume(queue=games_queue, on_message_callback=callback, auto_ack=True)
    logging.info(' [*] Esperando mensajes de juegos. Para salir presiona CTRL+C')
    
    try:
        channel.start_consuming()
        logging.info("Consumidor iniciado.")
        print(f'Consumidor iniciado.', flush=True)
    except KeyboardInterrupt:
        logging.info('Interrumpido por el usuario.')
        channel.stop_consuming()
    finally:
        if connection and connection.is_open:
            connection.close()
            logging.info("Conexión a RabbitMQ cerrada.")

if __name__ == '__main__':
    main()
