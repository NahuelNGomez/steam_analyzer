# gateway/connectionHandler.py

import logging
from protocol import Protocol
from dispatcher import Dispatcher
import csv
import io
import threading

class ConnectionHandler:
    def __init__(self, client_socket, address, dispatcher: Dispatcher):
        self.client_socket = client_socket
        self.address = address
        self.protocol = Protocol(self.client_socket)
        self.dispatcher = dispatcher
        # Thread para manejar la conexión - No lo haría para entrega 1.
        self.thread = threading.Thread(target=self.handle_connection)
        self.thread.daemon = True
        self.thread.start()

    def handle_connection(self):
        logging.info(f"Conexión establecida desde {self.address}")
        try:
            while True:
                data = self.protocol.receive_message()
                if not data:
                    logging.info(f"Cliente {self.address} desconectado")
                    break

                logging.debug(f"Recibido de {self.address}: {data}...")  # Mostrar solo los primeros 50 caracteres

                # Separar tipo de dataset y contenido usando doble salto de línea
                parts = data.split("\n\n", 1)
                if len(parts) < 2:
                    logging.warning("Datos recibidos en formato incorrecto.")
                    self.protocol.send_message("Error: Formato de datos incorrecto")
                    continue

                data_type = parts[0].strip().lower()
                if data_type not in ['games', 'reviews','fin']:
                    logging.warning(f"Tipo de dataset desconocido: {data_type}")
                    self.protocol.send_message(f"Error: Tipo de dataset desconocido '{data_type}'")
                    continue
                
                if data_type == 'fin':
                    self.protocol.send_message("OK - ACK de fin")
                    break

                # Leer el CSV del segundo bloque de datos
                csv_content = parts[1].strip()
                csv_file = io.StringIO(csv_content)
                try:
                    reader = csv.DictReader(csv_file)
                    data_list = [row for row in reader]
                    logging.info(f"{len(data_list)} filas procesadas para {data_type}.")

                    # Enviar los datos procesados al dispatcher
                    self.dispatcher.dispatch(data_list, data_type)

                    # Enviar una respuesta al cliente
                    self.protocol.send_message("OK")
                except Exception as e:
                    logging.error(f"Error al procesar el CSV: {e}")
                    self.protocol.send_message("Error processing data")
            
            logging.info(f"Fin del recibo de datos {self.address}")
            self.dispatcher.dispatchFin()
            recived_data = self.dispatcher.get_data()
            logging.info(f"Datos recibidos: {recived_data.decode('utf-8')}")
            self.protocol.send_message(recived_data.decode('utf-8'))
            self.protocol.send_message('close\n\n')
            

        except Exception as e:
            logging.error(f"Error en la conexión con {self.address}: {e}")
        finally:
            self.client_socket.close()
            logging.info("Conexión cerrada.")
