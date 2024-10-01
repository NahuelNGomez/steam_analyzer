# games_filter/main.py

import pika
import json
import logging
import time
from collections import defaultdict
from filter import GamesFilter
import configparser

def load_config():
    config = configparser.ConfigParser()
    config.read('config.ini')  
    return config['DEFAULT']

def main():
    config = load_config()
    logging.basicConfig(level=getattr(logging, config.get('LOGGING_LEVEL', 'INFO').upper()))

    rabbitmq_host = config.get('rabbitmq_HOST', 'rabbitmq')
    rabbitmq_port = int(config.get('rabbitmq_PORT', 5672))
    games_queue = config.get('rabbitmq_GAMES_QUEUE', 'games_queue')

    retries = 5
    delay = 5
    attempt = 0
    connection = None

    while attempt < retries:
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_host, port=rabbitmq_port))
            break
        except Exception as e:
            attempt +=1
            logging.error(f"Error al conectar a RabbitMQ: {e}. Intento {attempt} de {retries}")
            time.sleep(delay)
    else:
        logging.critical("No se pudo conectar a RabbitMQ después de varios intentos.")
        return

    channel = connection.channel()
    channel.queue_declare(queue=games_queue, durable=True)

    filter = GamesFilter()

    def callback(ch, method, properties, body):
        try:
            message = json.loads(body.decode('utf-8'))
            filter.filter_game(message)  # Procesar directamente el diccionario del juego
        except Exception as e:
            logging.error(f"Error al procesar el mensaje: {e}")

    channel.basic_consume(queue=games_queue, on_message_callback=callback, auto_ack=True)

    logging.info(' [*] Esperando mensajes de juegos. Para salir presiona CTRL+C')
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        logging.info('Interrumpido por el usuario.')
        channel.stop_consuming()
    finally:
        if connection and connection.is_open:
            connection.close()

if __name__ == '__main__':
    main()
