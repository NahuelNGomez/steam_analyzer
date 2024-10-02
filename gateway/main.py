# gateway/main.py
import socket
import logging
import configparser
from connectionHandler import ConnectionHandler
from dispatcher import Dispatcher


def load_config(config_file='config.ini'):
    config = configparser.ConfigParser()
    config.read(config_file)
    return config['DEFAULT']

def start_server(config, dispatcher):
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((config['gateway_IP'], int(config['gateway_PORT'])))
    server.listen(int(config['gateway_LISTEN_BACKLOG']))
    logging.info(f"Gateway escuchando en {config['gateway_IP']}:{config['gateway_PORT']}")
    
    while True:
        client_sock, address = server.accept()
        logging.info(f"Conexión aceptada de {address[0]}:{address[1]}")
        handler = ConnectionHandler(client_sock, address, dispatcher)

def main():
    config = load_config()
    logging.basicConfig(level=getattr(logging, config.get('LOGGING_LEVEL', 'INFO').upper()))
    logging.info("Gateway iniciado.")
    
    dispatcher = Dispatcher(config)
    
    start_server(config, dispatcher)

if __name__ == '__main__':
    main()
