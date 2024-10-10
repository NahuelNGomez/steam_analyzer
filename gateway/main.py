# gateway/main.py
import os
from queue import Queue
import socket
import logging
import configparser
from common.middleware import Middleware
from connectionHandler import ConnectionHandler


def load_config(config_file='config.ini'):
    config = configparser.ConfigParser()
    config.read(config_file)
    return config['DEFAULT']

def start_server(config):
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(("", 12345))
    server.listen(int(config['gateway_LISTEN_BACKLOG']))
    
    logging.info(f"Gateway escuchando en {config['gateway_IP']}:{config['gateway_PORT']}")
    amount_of_review_instances = int(os.getenv("AMOUNT_OF_REVIEW_INSTANCE", 1))
    
    while True:
        print("Esperando conexión...", flush=True)
        client_sock, address = server.accept()
        logging.info(f"Conexión aceptada de {address[0]}:{address[1]}")
        handler = ConnectionHandler(client_sock, address,amount_of_review_instances)

def main():
    config = load_config()
    logging.basicConfig(level=getattr(logging, config.get('LOGGING_LEVEL', 'INFO').upper()))
    logging.info("Gateway iniciado.")
    
    start_server(config)

if __name__ == '__main__':
    main()
    
    
    
    
