import os
import json
import struct
import uuid
import threading
from typing import Any, List, Optional, Dict
import logging

#TRANSLATOR_FILE = "translator.json"
#TRANSLATOR_LOCK = threading.Lock()

LENGTH_BYTES = 4
AUX_FILE = '_aux'
KEYS_INDEX_KEY_PREFIX = 'keys_index'
SEPARATOR = b'\xFF\xFF\xFF'

logging.basicConfig(level=logging.INFO)

class FaultManager:
    def __init__(self, storage_dir: str = "../persitence/", extension: str = ""):
        self.storage_dir = storage_dir
        os.makedirs(self.storage_dir, exist_ok=True)
        self.key_index_prefix = KEYS_INDEX_KEY_PREFIX + extension
        # Del tipo {"client1": {"nombre": "Alice", "edad": 30}}
        self._keys_index: dict[str, dict[str, str]] = {}
        self.locks = {}  # Diccionario para mantener los bloqueos por archivo
        self.locks_lock = threading.Lock()  # Lock para el diccionario de locks
        
        self.init_state()
        
    def _get_lock(self, path: str) -> threading.Lock:
        with self.locks_lock:
            if path not in self.locks:
                self.locks[path] = threading.Lock()
            return self.locks[path]
            
    def init_state(self):
        self._keys_index = {}
        for file_name in os.listdir(self.storage_dir):
            if file_name.startswith(self.key_index_prefix):
                with open(f'{self.storage_dir}/{file_name}', 'rb') as f:
                    while True:
                        try:
                            length_bytes = f.read(4)
                            if not length_bytes or len(length_bytes) < 4:
                                break
                                
                            length = struct.unpack('>I', length_bytes)[0]
                            data = f.read(length)
                            
                            if len(data) != length:
                                logging.warning("Datos truncados detectados")
                                break
                                
                            separator = f.read(len(SEPARATOR))
                            if separator != SEPARATOR:
                                logging.warning("Separador inválido detectado")
                                break
                                
                            line = data.decode()
                            parsed_line = json.loads(line)
                            
                            if len(parsed_line) == 2:
                                key, internal_key = parsed_line
                                self._keys_index[key] = internal_key
                                
                        except Exception as e:
                            logging.error(f"Error procesando línea: {e}")
        
        print(self._keys_index)

    def _append(self, path: str, text: str):
        lock = self._get_lock(path)
        with lock:
            try:
                data = text.encode()
                length = len(data)
                
                # Formato: [longitud(4 bytes)][datos][separador]
                length_bytes = struct.pack('>I', length)
                final_data = length_bytes + data + SEPARATOR
                
                with open(path, 'ab') as f:
                    f.write(final_data)
                    f.flush()
                    #os.fsync(f.fileno())
            except Exception as e:
                logging.error(f"Error appending to {path}: {e}")
                
    # def _append(persistent_path, temp_path):
    #     with open(persistent_path, 'ab') as persistent_file, open(temp_path, 'rb') as temp_file:
    #         # Lock the persistent file
    #         fcntl.flock(persistent_file, fcntl.LOCK_EX)
    #         # Append data from the temporary file
    #         while chunk := temp_file.read(4096):  # Read in chunks to handle large files
    #             persistent_file.write(chunk)
    #         # Unlock the persistent file
    #         fcntl.flock(persistent_file, fcntl.LOCK_UN)            
                
    def append(self, key: str, value: str):
        try:
            path = f'{self.storage_dir}/{self._get_internal_key(key)}'
            self._append(path, value)
        except Exception as e:
            logging.error(f"Error appending value: {value} for key: {key}: {e}")

    def _write(self, path: str, data: str):
        lock = self._get_lock(path)
        with lock:
            try:
                encoded_data = data.encode()
                length = len(encoded_data)
                length_bytes = struct.pack('>I', length)
                final_data = length_bytes + encoded_data + SEPARATOR
                
                temp_path = f'{path}_{AUX_FILE}'
                with open(temp_path, 'wb') as f:
                    f.write(final_data)
                    f.flush()
                    #os.fsync(f.fileno())
                os.replace(temp_path, path)
            except Exception as e:
                logging.error(f"Error writing to {path}: {e}")
                if os.path.exists(temp_path):
                    os.remove(temp_path)


    def _get_internal_key(self, key: str) -> str:
        internal_key = self._keys_index.get(key)
        if internal_key is None:
            logging.info(f"Generating internal key for key: {key}")
            internal_key = str(uuid.uuid4())
            self._keys_index[key] = internal_key
            self._append(f'{self.storage_dir}/{self.key_index_prefix}',
                         json.dumps([key, internal_key]))
        return internal_key


    def delete_key(self, key:str):
        try:
            path = f'{self.storage_dir}/{self._get_internal_key(key)}'
            os.remove(path)
            self._keys_index.pop(key)
            logging.info(f"Key deleted: {key}")
            
            updated_keys = [json.dumps([k, v]) for k, v in self._keys_index.items()]
            if len(updated_keys) == 0:
                logging.info(f"No keys left. Deleting keys index file.")
                os.remove(f'{self.storage_dir}/{self.key_index_prefix}')
            else:
                logging.info(f"Updating keys index file.")
                self._write(f'{self.storage_dir}/{self.key_index_prefix}', '\n'.join(updated_keys))
                
                
        except Exception as e:
            logging.error(f"Error deleting key: {key}: {e}")


    def get_keys(self, prefix: str) -> List[str]:
        keys = [key for key in self._keys_index.keys() if key.startswith(prefix)]
        logging.info(f"Keys found with prefix '{prefix}': {keys}")
        return keys

    def get(self, key: str) -> Optional[str]:
        path = f'{self.storage_dir}/{self._get_internal_key(key)}'
        lock = self._get_lock(path)
        with lock:
            try:
                result = []
                with open(path, 'rb') as f:
                    while True:
                        # Leer longitud
                        length_bytes = f.read(4)
                        if not length_bytes or len(length_bytes) < 4:
                            break
                            
                        # Extraer longitud
                        length = struct.unpack('>I', length_bytes)[0]
                        
                        # Leer datos
                        data = f.read(length)
                        if len(data) != length:
                            logging.warning(f"Datos truncados detectados en {path}")
                            break
                            
                        # Verificar separador
                        separator = f.read(len(SEPARATOR))
                        if separator != SEPARATOR:
                            logging.warning(f"Separador inválido detectado en {path}")
                            break
                            
                        result.append(data.decode())
                        
                return '\n'.join(result)
                
            except Exception as e:
                logging.error(f"Error getting key: {key}: {e}")
                return None

    def update(self, key: str, value: str):
        try:
            path = f'{self.storage_dir}/{self._get_internal_key(key)}'
            self._write(path, value)
        except Exception as e:
            logging.error(f"Error updating key: {key}: {e}")
