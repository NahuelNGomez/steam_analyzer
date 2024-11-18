import subprocess
import logging
from common.healthcheck import HEALTH_CHECK_PORT
import socket
import time
import re

class Doctor:
    not_include_host_regexes = ["rabbitmq", "doctor\d?", "client\d"]
    def __init__(self):
        result = subprocess.run(["docker", "ps", "-af", "network=tp1_testing_net",  "--format", "{{.Names}}"], check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        self.host_list = result.stdout.decode().split('\n')
        for host in self.host_list:
            for not_include_host in self.not_include_host_regexes:
                if re.search(not_include_host, host):
                    self.host_list.remove(host)

    def start(self):
        logging.info(f"Starting health check for {len(self.host_list)} hosts: {self.host_list}")
        while True:
            for host in self.host_list:
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

