# import socket
# import configparser
# import logging

# def load_config(config_file='config.ini'):
#     config = configparser.ConfigParser()
#     config.read(config_file)
#     return config['DEFAULT']

# def setup_logging(level):
#     numeric_level = getattr(logging, level.upper(), None)
#     if not isinstance(numeric_level, int):
#         numeric_level = logging.INFO
#     logging.basicConfig(level=numeric_level,
#                         format='%(asctime)s - %(levelname)s - %(message)s')
# def start_client(boundary_ip, boundary_port):
#     with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
#         try:
#             s.connect((boundary_ip, int(boundary_port)))
#             logging.info(f"Conectado al servidor en {boundary_ip}:{boundary_port}")
            
#             message = "Mensaje automático desde el cliente"
#             logging.debug(f"Enviado: {message}")
#             s.sendall(message.encode())
            
#             data = s.recv(1024)
#             logging.debug(f"Recibido: {data.decode()}")
#             print(f"Respuesta del servidor: {data.decode()}")
            
#         except ConnectionRefusedError:
#             logging.error(f"No se pudo conectar al servidor en {boundary_ip}:{boundary_port}")

# if __name__ == "__main__":
#     config = load_config()
#     setup_logging(config.get('LOGGING_LEVEL', 'INFO'))
#     BOUNDARY_IP = config.get('BOUNDARY_IP', 'localhost')
#     BOUNDARY_PORT = config.get('BOUNDARY_PORT', 12345)
#     start_client(BOUNDARY_IP, BOUNDARY_PORT)


import pika, sys, os

def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq', port=5672))
    channel = connection.channel()

    channel.queue_declare(queue='test')

    def callback(ch, method, properties, body):
        print(f" [x] Received {body}", flush=True)

    channel.basic_consume(queue='test', on_message_callback=callback, auto_ack=True)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
