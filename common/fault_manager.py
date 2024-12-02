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

logging.basicConfig(level=logging.INFO)

class FaultManager:
    def __init__(self, storage_dir: str = "../persitence/"):
        self.storage_dir = storage_dir
        os.makedirs(self.storage_dir, exist_ok=True)

        # Del tipo {"client1": {"nombre": "Alice", "edad": 30}}
        self._keys_index: dict[str, dict[str, str]] = {}
        
        
        self._init_state()
        

    def _init_state(self):
        for file_name in os.listdir(self.storage_dir):
            if file_name.startswith(KEYS_INDEX_KEY_PREFIX):
                self._keys_index = {}   
                with open(f'{self.storage_dir}/{file_name}', 'rb') as f:
                    while True:
                        # Leer los primeros 4 bytes (longitud)
                        length_bytes = f.read(4)
                        if not length_bytes:
                            break  # Fin del archivo
                        length = struct.unpack('>I', length_bytes)[0]
                        
                        # Leer los siguientes `length` bytes (datos en JSON)
                        data = f.read(length).decode('unicode_escape')
                        
                        # Parsear la línea en JSON
                        key, internal_key = json.loads(data)
                        
                        # Agregar al diccionario
                        self._keys_index[key] = internal_key
                        
                        # Leer el separador '\n'
                        f.read(1)  # Salta el salto de línea
        print(self._keys_index)
    # Text incluye el package_number
    def _append(self, path: str, text: str):
        try:
            data = text.encode('unicode_escape')
            
            # (big-endian)
            data += b'\n'
            length_bytes = len(data).to_bytes(
                LENGTH_BYTES, byteorder='big')
            with open(path, 'ab') as f:
                f.write(length_bytes + data)
                f.flush()
                
        except Exception as e:
            logging.error(f"Error appending to {path}: {e}")


    def append(self, key: str, value: str):
        try:
            path = f'{self.storage_dir}/{self._get_internal_key(key)}'
            self._append(path, value)
        except Exception as e:
            logging.error(f"Error appending value: {value} for key: {key}: {e}")

    def _write(self, path, data: str):
        try:
            logging.debug(f"Writing to {path}")
            data = data.encode('unicode_escape')
            data += b'\n'
            length_bytes = len(data).to_bytes(
                LENGTH_BYTES, byteorder='big')
            temp_path = f'{self.storage_dir}/{AUX_FILE}'
            with open(temp_path, 'wb') as f:
                f.write(length_bytes + data)
                f.flush()
            os.replace(temp_path, path)
        except Exception as e:
            logging.error(f"Error writing to {path}: {e}")


    def _get_internal_key(self, key: str) -> str:
        internal_key = self._keys_index.get(key)
        if internal_key is None:
            logging.info(f"Generating internal key for key: {key}")
            internal_key = str(uuid.uuid4())
            self._keys_index[key] = internal_key
            self._append(f'{self.storage_dir}/{KEYS_INDEX_KEY_PREFIX}',
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
                os.remove(f'{self.storage_dir}/{KEYS_INDEX_KEY_PREFIX}')
            else:
                logging.info(f"Updating keys index file.")
                self._write(f'{self.storage_dir}/{KEYS_INDEX_KEY_PREFIX}', '\n'.join(updated_keys))
                
                
        except Exception as e:
            logging.error(f"Error deleting key: {key}: {e}")


    def get_keys(self, prefix: str) -> List[str]:
        keys = [key for key in self._keys_index.keys() if key.startswith(prefix)]
        logging.info(f"Keys found with prefix '{prefix}': {keys}")
        return keys

    def get(self, key: str) -> Optional[str]:
        try:
            path = f'{self.storage_dir}/{self._get_internal_key(key)}'
            with open(path, 'rb') as f:
                data = ''
                while (length_bytes := f.read(LENGTH_BYTES)):
                    length = int.from_bytes(length_bytes, byteorder='big')
                    
                    content = f.read(length)
                                        
                    if len(content) == length:
                        data += content.decode('unicode_escape')
                    else:
                        logging.error(f"Error reading key: {key}")
                return data
                    
        except Exception as e:  
            logging.error(f"Error getting key: {key}: {e}")
            return None

    def update(self, key: str, value: str):
        try:
            path = f'{self.storage_dir}/{self._get_internal_key(key)}'
            self._write(path, value)
        except Exception as e:
            logging.error(f"Error updating key: {key}: {e}")
