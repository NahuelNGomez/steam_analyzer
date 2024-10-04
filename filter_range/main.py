# filter_range/main.py

import pika
import json
import logging
import time
from filter import FilterRange
import configparser

def load_config():
    config = configparser.ConfigParser()
    config.read('config.ini')  
    return config['DEFAULT']

def send_message(queue, message, connection):
    channel = connection.channel()
    channel.queue_declare(queue=queue, durable=True)
    channel.basic_publish(exchange='', routing_key=queue, body=message)
    print(f'filter_range envió {message} a la cola {queue}.', flush=True)

def main():
    config = load_config()
    logging.basicConfig(level=getattr(logging, config.get('LOGGING_LEVEL', 'INFO').upper()),
                        format='%(asctime)s - %(levelname)s - %(message)s')

    rabbitmq_host = config.get('rabbitmq_HOST', 'rabbitmq')
    rabbitmq_port = int(config.get('rabbitmq_PORT', 5672))
    input_queue = config.get('rabbitmq_INDI_GAMES_QUEUE', 'indie_games')
    output_queue = config.get('rabbitmq_INDI_GAMES_IN_RANGE_QUEUE', 'indie_games_in_range')

    retries = 5
    delay = 10
    attempt = 0
    connection = None

    while attempt < retries:
        try:
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=rabbitmq_host,
                    port=rabbitmq_port,
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
    channel.queue_declare(queue=output_queue, durable=True)
    logging.info(f"Declaradas las colas: {input_queue}, {output_queue}")

    filter_range = FilterRange(start_year=2010, end_year=2019)

    def callback(ch, method, properties, body):
        try:
            logging.debug(f"Recibido mensaje: {body}...")
            print(f'[x] Recibido {body}', flush=True)
            mensaje_str = body.decode('utf-8')

            if mensaje_str == 'fin\n\n' :
                logging.info("Fin de los mensajes de juegos.")
                print(f"Fin de los mensajes de juegos.", flush=True)
                # Enviar el mensaje de fin a la siguiente cola
                send_message(output_queue, mensaje_str, connection)
                ch.basic_cancel(consumer_tag=method.consumer_tag)
                return
            message = json.loads(mensaje_str)
            logging.debug(f"Mensaje decodificado: {message}")

            filtered_game = filter_range.filter_by_range(message)
            if filtered_game:
                send_message(output_queue, json.dumps(filtered_game), connection)
                logging.info(f"Juego filtrado enviado a {output_queue}: {filtered_game}")
                print(f"Juego filtrado enviado a {output_queue}: {filtered_game}", flush=True)
            else:
                logging.info("Juego no cumple con el rango de fechas.")
                print("Juego no cumple con el rango de fechas.", flush=True)

        except Exception as e:
            logging.error(f"Error al procesar el mensaje: {e}")

    channel.basic_consume(queue=input_queue, on_message_callback=callback, auto_ack=True)
    logging.info(' [*] Esperando mensajes en indie_games para filtrar por rango. Para salir presiona CTRL+C')

    try:
        channel.start_consuming()
        logging.info("Consumidor iniciado.")
    except KeyboardInterrupt:
        logging.info('Interrumpido por el usuario.')
        channel.stop_consuming()
    finally:
        connection.close()
        logging.info("Conexión a RabbitMQ cerrada.")

if __name__ == '__main__':
    main()
