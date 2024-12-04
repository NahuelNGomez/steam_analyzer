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
        self._keys_index = {}  # Inicializamos el diccionario una vez fuera del loop
        for file_name in os.listdir(self.storage_dir):
            if file_name.startswith(self.key_index_prefix):
                with open(f'{self.storage_dir}/{file_name}', 'r') as f:
                    for line in f:
                        line = line.strip()  # Remover espacios y saltos de línea
                        if not line:  # Saltar líneas vacías
                            continue
                        try:
                            # Parse the line as a JSON list
                            parsed_line = json.loads(line)
                            
                            # Ensure the list has exactly 2 elements
                            if len(parsed_line) == 2:
                                key, internal_key = parsed_line
                                self._keys_index[key] = internal_key
                            else:
                                print(f"Línea no válida: {line}")
                        
                        except json.JSONDecodeError as e:
                            print(f"Error al decodificar JSON: {e}, línea: {line}")
        
        print(self._keys_index)
        
    # def _append(self, path: str, text: str):
    #     try:
    #         data = text.encode()
    #         data += b'\n'
            
    #         # Crear un archivo temporal y escribir los datos existentes + nuevos
    #         temp_path = f'{path}_{AUX_FILE}'
            
    #         # Copiar datos existentes si el archivo existe
    #         if os.path.exists(path):
    #             with open(path, 'rb') as original:
    #                 existing_data = original.read()
    #         else:
    #             existing_data = b''
                
    #         # Escribir datos existentes + nuevos en archivo temporal
    #         with open(temp_path, 'wb') as f:
    #             f.write(existing_data)
    #             f.write(data)
    #             f.flush()
                
    #         # Reemplazar archivo original con temporal
    #         os.replace(temp_path, path)
                    
    #     except Exception as e:
    #         logging.error(f"Error appending to {path}: {e}")
    #         # Limpiar archivo temporal si hubo error
    #         if os.path.exists(temp_path):
    #             os.remove(temp_path)
    def _append(self, path: str, text: str):
        lock = self._get_lock(path)
        with lock:
            try:
                data = text.encode()
                data += b'\n'
                with open(path, 'ab') as f:
                    f.write(data)
                    f.flush()
            except Exception as e:
                logging.error(f"Error appending to {path}: {e}")
                
                
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
                logging.debug(f"Writing to {path}")
                encoded_data = data.encode()
                encoded_data += b'\n'
                temp_path = f'{path}_{AUX_FILE}'
                with open(temp_path, 'wb') as f:
                    f.write(encoded_data)
                    f.flush()
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
                with open(path, 'rb') as f:
                    data = f.read().decode()
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
