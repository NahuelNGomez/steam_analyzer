import socket
import configparser
import logging
import pika


def load_config(config_file='config.ini'):
    config = configparser.ConfigParser()
    config.read(config_file)
    return config['DEFAULT']

def setup_logging(level):
    numeric_level = getattr(logging, level.upper(), None)
    if not isinstance(numeric_level, int):
        numeric_level = logging.INFO
    logging.basicConfig(level=numeric_level,
                        format='%(asctime)s - %(levelname)s - %(message)s')

def start_server(server_ip, server_port, backlog, channel):
    msg = "Testing de RabbitMQ"
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((server_ip, int(server_port)))
        s.listen(int(backlog))
        logging.info(f"Servidor escuchando en {server_ip}:{server_port}")
        while True:
            conn, addr = s.accept()
            with conn:
                logging.info(f"Conexión establecida desde {addr}")
                while True:
                    data = conn.recv(1024)
                    if not data:
                        break
                    logging.debug(f"Recibido: {data.decode()}")
                    channel.basic_publish(exchange='', routing_key='test_filter', body=data)
                    conn.sendall('ack'.encode())
                    logging.debug(f"Enviado de vuelta: {'ack'.encode()}")

if __name__ == "__main__":
    config = load_config()
    setup_logging(config.get('LOGGING_LEVEL', 'INFO'))
    SERVER_IP = config.get('SERVER_IP', 'localhost')
    SERVER_PORT = config.get('SERVER_PORT', 12345)
    BACKLOG = config.get('SERVER_LISTEN_BACKLOG', 5)

    connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='rabbitmq',port=5672))
    channel = connection.channel()
    channel.queue_declare(queue='test_filter')
    
    start_server(SERVER_IP, SERVER_PORT, BACKLOG, channel)
    connection.close()


