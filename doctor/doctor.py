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
        if len(self.host_list) > 0 and self.host_list[-1] == '':
            self.host_list.pop()

        for host in self.host_list:
            for not_include_host in self.not_include_host_regexes:
                if re.search(not_include_host, host):
                    self.host_list.remove(host)

    def start(self):
        self.check_health_loop()

    def check_health_loop(self):
        logging.info(f"Starting health check for {len(self.host_list)} hosts: {self.host_list}")
        while True:
            time.sleep(10)
            for host in self.host_list:
                logging.info(f"Checking health of {host}")
                res: int = self.check_health(host)
                if res == 0:
                    self.restart_container(host)
                    logging.error(f"Worker {host} is down. Restarting it.")
                elif res == 1:
                    logging.info(f"Health check of {host} OK")
    
    def check_health(self, host):
        retries=0
        while retries < 3:
            try:
                    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    s.settimeout(3)
                    s.connect((host, HEALTH_CHECK_PORT))
                    s.send(b'1')
                    data = s.recv(1)
                    if data != b'1':
                        raise Exception("Invalid data received")
                    return int(data)
            except Exception as e:
                logging.error(f"Error checking health of {host}: {e}. retrying")

            retries+=1
            time.sleep(1)

        return 0
    
    def restart_container(self, container: str):
        try:
            result = subprocess.run(["docker", "start", container], check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            result.check_returncode()
        except Exception as e:
            logging.error(f"Error restarting worker: {e}")

