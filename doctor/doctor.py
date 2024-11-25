import subprocess
import logging
from common.healthcheck import HEALTH_CHECK_PORT
import socket
import time
import re
import threading
from common.healthcheck import HealthCheckServer
import os

VOTE=b'3'
DECISION=b'7'
HEALTH=b'1'

LEADER_PORT=8888

class Doctor:
    not_include_host_regexes = ["rabbitmq", "doctor\d?", "client\d"]
    def __init__(self):
        self.doctors: list[str] = ["doctor0", "doctor1", "doctor2"]
        self.leader_id: int = None
        self.id: int = int(os.getenv("ID", '0'))
        self.prev_leader_id: int = None

        result = subprocess.run(["docker", "ps", "-af", "network=tp1_testing_net",  "--format", "{{.Names}}"], check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        self.host_list = result.stdout.decode().split('\n')
        if len(self.host_list) > 0 and self.host_list[-1] == '':
            self.host_list.pop()

        for host in self.host_list:
            for not_include_host in self.not_include_host_regexes:
                if re.search(not_include_host, host):
                    self.host_list.remove(host)

    def start(self):
        self.leader_id = None
        t1 = threading.Thread(target=self.send_leader_id, args=(self.id,))
        t1.start()

        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind(('0.0.0.0', LEADER_PORT))
        s.listen(1)

        curr_leader_id = self.id

        while True:
            logging.info("Listening to leader messages")
            client_socket, _ = s.accept()
            message_type = client_socket.recv(1)

            if message_type == DECISION:
                self.leader_id = int.from_bytes(client_socket.recv(4), byteorder='big')

                logging.info(f"DECISION. Leader elected: {self.doctors[self.leader_id]}")
                t1 = threading.Thread(target=self.check_health_loop_leader)
                t1.start()
            elif message_type == VOTE:
                leader_id_recv = int.from_bytes(client_socket.recv(4), byteorder='big')
                logging.info(f"VOTE. Id received: {leader_id_recv}")

                if self.leader_id is not None:
                    client_socket.send(self.leader_id.to_bytes(4, byteorder='big'))
                    continue

                if leader_id_recv == self.id:
                    self.leader_id = self.id
                    logging.info(f"Leader elected: {self.doctors[self.leader_id]}")

                    self.send_decision()

                    # if self.prev_leader_id is not None:
                    #     self.restart_container(self.doctors[self.prev_leader_id])

                    self.check_health_thread = threading.Thread(target=self.check_health_loop, args=(self.host_list,))
                    self.check_health_thread.start()
                    continue

                if leader_id_recv > curr_leader_id:
                    curr_leader_id = leader_id_recv

                self.send_leader_id(curr_leader_id)

            elif message_type == HEALTH:
                logging.info(f"HEALTH message received")
                if self.leader_id == self.id:
                    # if self.check_health_thread:
                    #     is_alive = self.check_health_thread.is_alive()
                    client_socket.send(self.leader_id.to_bytes(4, byteorder='big'))
            else:
                logging.error(f"Invalid message type: {message_type}")

            client_socket.close()

    def check_health_loop_leader(self):
        leader_hostname = self.doctors[self.leader_id]
        logging.info(f"Starting health check for leader: {leader_hostname}")
        while True:
            time.sleep(5)
            logging.info(f"Checking health of {leader_hostname}")
            res: int = self.check_health(leader_hostname, port=LEADER_PORT)
            if res == 0:
                logging.error(f"Leader {leader_hostname} is down. Sending vote to next doctor.")
                self.leader_id = None
                self.send_leader_id(self.id)
                return 0
            elif res == 1:
                logging.info(f"Health check of {leader_hostname} OK")

    def send_decision(self):
        """
        send the decision made to the other doctors
        """
        for i, doctor in enumerate(self.doctors):
            if i == self.id:
                continue
            
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect((doctor, LEADER_PORT))
                s.send(DECISION)
                s.send(self.leader_id.to_bytes(4, byteorder='big'))
                s.close()
            except Exception as e:
                logging.error(f"Error sending decision to {doctor}: {e}")

    def send_leader_id(self, leader_id: str):
        """
        send `leader_id` to the next doctor alive in the ring
        """
        time.sleep(5)

        i = 1
        while True:
            next_doctor = self.doctors[(self.id+i)%len(self.doctors)]
            if next_doctor == self.id:
                return
            try:
                logging.info(f"Sending id {leader_id} to {next_doctor}")
                self.__socket_send_leader_id(next_doctor, leader_id)
                return
            except Exception as e:
                logging.error(f"Error sending id to {next_doctor}: {e}")
                i+=1

    def __socket_send_leader_id(self, host: str, data: int):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((host, LEADER_PORT))

        s.send(VOTE)
        s.send(data.to_bytes(4, byteorder='big'))

        s.close()

    def check_health_loop(self, hosts: list[str]): 
        logging.info(f"Starting health check for {len(hosts)} hosts: {self.host_list}")
        while True:
            time.sleep(15)
            for host in self.host_list:
                logging.info(f"Checking health of {host}")
                res: int = self.check_health(host)
                if res == 0:
                    self.restart_container(host)
                    logging.error(f"Worker {host} is down. Restarting it.")
                elif res == 1:
                    logging.info(f"Health check of {host} OK")
    
    def check_health(self, host, port=HEALTH_CHECK_PORT):
        retries=0
        while retries < 3:
            try:
                    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    s.settimeout(3)
                    s.connect((host, port))
                    s.send(HEALTH)
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
            result = subprocess.run(["docker", "stop", container], check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            result = subprocess.run(["docker", "start", container], check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            result.check_returncode()
        except Exception as e:
            logging.error(f"Error restarting worker: {e}")

