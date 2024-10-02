import configparser
import logging
from client import Client
import sys

def load_config(config_file='config.ini'):
    config = configparser.ConfigParser()
    config.read(config_file)
    return config['DEFAULT']

def setup_logging(level):
    numeric_level = getattr(logging, level.upper(), None)
    if not isinstance(numeric_level, int):
        numeric_level = logging.INFO
    logging.basicConfig(
        level=numeric_level,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

if __name__ == "__main__":
    config = load_config()
    setup_logging(config.get('LOGGING_LEVEL', 'INFO'))
    
    BOUNDARY_IP = config.get('BOUNDARY_IP', 'gateway')  
    BOUNDARY_PORT = config.get('BOUNDARY_PORT', 12345)
    
    client = Client(BOUNDARY_IP, BOUNDARY_PORT)
    client.start()
