import subprocess
import logging
from common.healthcheck import HEALTH_CHECK_PORT
import socket
import time
import threading
import os

VOTE=b'3'
DECISION=b'7'
HEALTH=b'1'
LEADER=b'9'

class Doctor:
    def __init__(self):
        doctors = int(os.getenv("NUM_DOCTORS", '1'))
        self.doctors: list[str] = [f"doctor{i}" for i in range(int(doctors))]

        self.id: int = int(os.getenv("ID", '0'))
        self.host_list = os.getenv("WORKERS", "").split(",")
        self.timeout = int(os.getenv("TIMEOUT", '15'))

        self.leader_id: int = None
        self.leader_id_lock = threading.Lock()

        self.prev_leader_id: int = None
        self.curr_leader_id = self.id

        self.participating = False


    def start(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind(('0.0.0.0', HEALTH_CHECK_PORT))
        s.listen(1)

        if self.id == 0:
            logging.info(f"SENDING VOTE to next becaus i'm 0: {self.id}")
            self.participating = True
            self.send_message(VOTE, self.id)

        while True:
            logging.info("Listening to leader messages")
            client_socket, _ = s.accept()
            message_type = client_socket.recv(1)

            if message_type == VOTE:
                doctor_id_recv = int.from_bytes(client_socket.recv(4), byteorder='big')
                logging.info(f"VOTE. Id received: {doctor_id_recv}")

                if not self.participating:
                    if self.leader_id == self.id:
                        logging.info(f"I am the leader. sending LEADER to everyone.")
                        self.send_message(LEADER, self.id)
                    else:
                        self.participating = True
                        self.send_message(VOTE, max(doctor_id_recv, self.id))
                else:
                    if doctor_id_recv == self.id:
                        logging.info(f"I am the leader. sending DECISION to everyone.")
                        self.send_message(DECISION, self.id)
                        self.participating = False

                        self.leader_id_lock.acquire()
                        self.leader_id = self.id
                        self.leader_id_lock.release()

                        self.check_health_thread = threading.Thread(target=self.check_health_loop)
                        self.check_health_thread.start()

                        self.check_health_thread2 = threading.Thread(target=self.send_leader_loop)
                        self.check_health_thread2.start()

                    elif doctor_id_recv > self.id:
                        self.send_message(VOTE, doctor_id_recv)
            elif message_type == DECISION:
                if self.participating:

                    self.leader_id_lock.acquire()
                    self.leader_id = int.from_bytes(client_socket.recv(4), byteorder='big')
                    self.leader_id_lock.release()

                    self.participating = False
                    logging.info(f"DECISION. Leader id received: {self.leader_id}")
                    if self.leader_id != self.id:
                        self.send_message(DECISION, self.leader_id)
                        self.check_health_thread = threading.Thread(target=self.check_health_loop_leader)
                        self.check_health_thread.start()
            elif message_type == HEALTH:
                logging.info(f"HEALTH message received")

                if self.leader_id == self.id:
                    assert self.check_health_loop
                    if self.check_health_thread.is_alive():
                        logging.info("Leader thread is alive")
                        client_socket.send(b'1')
                    else:
                        client_socket.send(b'0')
                else:
                    client_socket.send(b'1')
            elif message_type == LEADER:
                if self.leader_id is None:

                    self.leader_id_lock.acquire()
                    self.leader_id = int.from_bytes(client_socket.recv(4), byteorder='big')
                    self.leader_id_lock.release()

                    logging.info(f"LEADER message received. Leader id received: {self.leader_id}. Setting leader. starting health check thread.")
                    self.check_health_thread = threading.Thread(target=self.check_health_loop_leader)
                    self.check_health_thread.start()
                
                if self.leader_id != self.id:
                    self.send_message(LEADER, self.leader_id)
            else:
                logging.error(f"Invalid message type: {message_type}")
            client_socket.close()

    def send_leader_loop(self):
        while True:
            self.send_message(LEADER, self.id)
            time.sleep(self.timeout/3)

    def check_health_loop_leader(self):
        current_leader = self.leader_id
        leader_hostname = self.doctors[current_leader]
        logging.info(f"Starting health check for leader: {leader_hostname}")

        while True:
            time.sleep(self.timeout/3)
            logging.info(f"Checking health of {leader_hostname}")
            res: int = self.check_health(leader_hostname, port=HEALTH_CHECK_PORT)
            if res == 0:
                logging.error(f"Leader {leader_hostname} is down.")

                self.leader_id_lock.acquire()
                self.leader_id = None
                self.leader_id_lock.release()

                i = 1
                while True:
                    next_to_leader = (current_leader+i)%len(self.doctors)
                    logging.info(f"{next_to_leader} should start the election")

                    if self.id == next_to_leader:
                        logging.info(f"{next_to_leader} its me, so i send the VOTE to {next_to_leader+1}")
                        self.send_message(VOTE, self.id)
                        break

                    res = self.check_health(self.doctors[next_to_leader])
                    if res == 0:
                        logging.error(f"Doctor {next_to_leader} is down.")
                        i+=1
                    else:
                        break

                return 0
            elif res == 1:
                logging.info(f"Health check of {leader_hostname} OK")

    def send_message(self, message_type, leader_id: str):
        """
        send `leader_id` to the next doctor alive in the ring
        """
        i = 1
        while True:
            next_doctor = self.doctors[(self.id+i)%len(self.doctors)]
            if next_doctor == self.id:
                return -1

            try:
                self.__socket_send(next_doctor, message_type, leader_id)
                return
            except Exception as e:
                logging.error(f"Error sending id to {next_doctor}: {e}")

            i+=1

    def __socket_send(self, host: str, message_type: int, data: int):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((host, HEALTH_CHECK_PORT))

        s.send(message_type)
        if data:
            s.send(data.to_bytes(4, byteorder='big'))

        s.close()

    def check_health_loop(self): 
        logging.info(f"Starting health check for {len(self.host_list)} hosts: {self.host_list}")
        l = self.host_list + self.doctors 
        l.remove(self.doctors[self.id])
        while True:
            time.sleep(self.timeout)
            for host in l:
                # logging.info(f"Checking health of {host}")
                res: int = self.check_health(host)
                if res == 0:
                    self.restart_container(host)
                    # logging.error(f"Worker {host} is down. Restarting it.")
                elif res == 1:
                    pass
    
    def check_health(self, host, port=HEALTH_CHECK_PORT):
        retries=0
        while retries < 3:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                    s.settimeout(3)
                    s.connect((host, port))
                    s.send(HEALTH)
                    data = s.recv(1)
                    if data != b'1':
                        raise Exception("Invalid data received")
                    return 1
            except Exception as e:
                logging.error(f"Error checking health of {host}: {e}. retrying")
            finally:
                s.close()

            retries+=1
            time.sleep(self.timeout/10)

        return 0
    
    def restart_container(self, container: str):
        try:
            logging.info(f"Restarting worker: {container}")
            result = subprocess.run(["docker", "stop", container], check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            result = subprocess.run(["docker", "start", container], check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            result.check_returncode()
        except Exception as e:
            logging.error(f"Error restarting worker: {e}")

