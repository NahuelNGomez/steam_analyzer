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
        
        # Asegurarse de enviar todo el prefijo de longitud
        self._send_all(length_prefix)
        # Asegurarse de enviar todo el mensaje
        self._send_all(message_bytes)

    def receive_message(self):
        # Recibir los primeros 4 bytes para obtener la longitud del mensaje
        raw_length = self._recv_all(4)
        if not raw_length:
            return None  # Conexión cerrada
        # Desempaquetar la longitud
        message_length = struct.unpack('>I', raw_length)[0]
        # Recibir el mensaje completo basado en la longitud
        message = self._recv_all(message_length)
        return message.decode('utf-8') if message else None

    def _send_all(self, data):
        """
        Garantiza que todos los datos sean enviados, manejando "short writes".
        """
        total_sent = 0
        while total_sent < len(data):
            sent = self.socket.send(data[total_sent:])
            if sent == 0:
                raise RuntimeError("La conexión con el socket se ha cerrado inesperadamente")
            total_sent += sent

    def _recv_all(self, n):
        """
        Garantiza que se reciban exactamente n bytes, manejando "short reads".
        """
        data = b''
        while len(data) < n:
            packet = self.socket.recv(n - len(data))
            if not packet:
                return None  # Conexión cerrada
            data += packet
        return data
