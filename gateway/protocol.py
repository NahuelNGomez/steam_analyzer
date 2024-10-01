# protocol.py

import struct

class Protocol:
    def __init__(self, socket):
        self.socket = socket

    def send_message(self, message):
        # Codificar el mensaje a bytes
        message_bytes = message.encode('utf-8')
        # Obtener la longitud del mensaje
        message_length = len(message_bytes)
        # Empaquetar la longitud en 4 bytes en big-endian
        length_prefix = struct.pack('>I', message_length)
        # Enviar el prefijo de longitud seguido del mensaje
        self.socket.sendall(length_prefix + message_bytes)

    def receive_message(self):
        # Recibir los primeros 4 bytes para obtener la longitud
        raw_length = self._recv_all(4)
        if not raw_length:
            return None
        # Desempaquetar la longitud
        message_length = struct.unpack('>I', raw_length)[0]
        # Recibir el mensaje completo basado en la longitud
        message = self._recv_all(message_length)
        return message.decode('utf-8') if message else None

    def _recv_all(self, n):
        data = b''
        while len(data) < n:
            packet = self.socket.recv(n - len(data))
            if not packet:
                return None
            data += packet
        return data
