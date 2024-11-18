import subprocess
import logging
from common.healthcheck import HEALTH_CHECK_PORT
import socket
import time

class Doctor:
    def __init__(self):
        pass

    def start(self):
        # result = subprocess.run(['bash', '-c', ''], check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        # logging.info('Command executed. Result={}. Output={}. Error={}'.format(result.returncode, result.stdout, result.stderr))
        # Start health check server

        host_list = ["games_counter"]
        while True:
            for host in host_list:
                logging.info(f"Checking health of {host}")
                self.check_health(host)
            time.sleep(10)
    
    def check_health(self, host):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(3)
            s.connect((host, HEALTH_CHECK_PORT))
            s.send(b'1')
            data = s.recv(1)
            if data == b'1':
                logging.info(f"Health check of {host} OK")
            else:
                logging.error(f"Error checking health of {host}. Invalid response: {data}")
        except Exception as e:
            logging.error(f"Error checking health of {host}: {e}")

