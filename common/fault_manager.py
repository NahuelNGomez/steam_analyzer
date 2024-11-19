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
            logging.info(f"Appending to {path} - {str}")
            data = text.encode('unicode_escape')
            length = len(data)
            
            # (big-endian)
            length_bytes = struct.pack('>I', length)
            data += b'\n'
            with open(path, 'ab') as f:
                f.write(length_bytes + data)
                f.flush()
                
        except Exception as e:
            logging.error(f"Error appending to {path}: {e}")


    def append(self, key: str, value: str):
        try:
            path = f'{self.storage_dir}/{self._get_internal_key(key)}'
            logging.info(f"Appending value: {value} for key: {key}")
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
            
            updated_keys = [json.dumps([k, v]) for k, v in self._keys_index.items()]
            if len(updated_keys) == 0:
                os.remove(f'{self.storage_dir}/{KEYS_INDEX_KEY_PREFIX}')
            else:
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
            print("leo el path->", path, flush=True)
            with open(path, 'rb') as f:
                length_bytes = f.read(LENGTH_BYTES)
                if not length_bytes:
                    return None
                length = int.from_bytes(length_bytes, byteorder='big')
                data = f.read(length).decode('unicode_escape')
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


    # def _load_translator(self):
    #     with TRANSLATOR_LOCK:
    #         with open(self.translator_path, 'r') as f:
    #             self.translator = json.load(f)
    #         logging.info(f"Traductor cargado con {len(self.translator)} claves")

    # def _save_translator(self):
    #     with TRANSLATOR_LOCK:
    #         temp_path = self.translator_path + ".tmp"
    #         with open(temp_path, 'w') as f:
    #             json.dump(self.translator, f)
    #         os.replace(temp_path, self.translator_path)
    #         logging.info(f"Traductor guardado con {len(self.translator)} claves")
            
    # def _get_file_path(self, key: str) -> str:
    #     with TRANSLATOR_LOCK:
    #         if key not in self.translator:
    #             file_name = f"{uuid.uuid4()}.txt"
    #             self.translator[key] = file_name
    #             self._save_translator()
    #             logging.info(f"Generado nuevo archivo para la clave '{key}': {self.translator[key]}")
    #         else:
    #             file_name = self.translator[key]
    #     return os.path.join(self.storage_dir, file_name)

    # def delete(self, key: str):
    #     file_path = self._get_file_path(key)
    #     with TRANSLATOR_LOCK:
    #         if key in self.translator:
    #             logging.info(f"Eliminando archivo para la clave '{key}': {self.translator[key]}")
    #             del self.translator[key]
    #             self._save_translator()
    #     if os.path.exists(file_path):
    #         os.remove(file_path)
    #         logging.info(f"Archivo eliminado: {file_path}")
    #     else: 
    #         logging.info(f"Archivo no encontrado para la clave {key}")

    # def get(self, key: str) -> Optional[Dict[str, Any]]:
    #     file_path = self._get_file_path(key)
    #     if not os.path.exists(file_path):
    #         logging.info(f"Archivo no encontrado para la clave {key}")
    #         return None
    #     try:
    #         with open(file_path, 'r') as f:
    #             content = f.read()
    #         logging.info(f"Leído el archivo {file_path}")
    #         # Leer el tamaño de la línea y reconstruir el estado
    #         lines = content.split('\n')
    #         data = {}
    #         for line in lines:
    #             if not line.strip():
    #                 continue
    #             try:
    #                 size_str, json_data = line.split(" ", 1)
    #                 size = int(size_str)
    #                 if len(json_data.encode('utf-8')) != size:
    #                     # Archivo corrupto, omitir esta línea
    #                     logging.info(f"Archivo corrupto para la clave {key}, omitiendo")
    #                     continue
    #                 entry = json.loads(json_data)
    #                 # Suponiendo que cada entry tiene 'package_number' y 'text'
    #                 data = {
    #                     'package_number': entry.get('package_number', 0),
    #                     'text': entry.get('text', '')
    #                 }
    #             except (ValueError, json.JSONDecodeError) as e:
    #                 # Línea malformada, omitir
    #                 logging.info(f"Error al decodificar la línea '{line}': {e}")
    #                 continue
    #         return data if data else None
    #     except Exception as e:
    #         logging.info(f"Error al leer el archivo {file_path}: {e}")
    #         return None

    # def get_keys(self, prefix: str) -> List[str]:
    #     with TRANSLATOR_LOCK:
    #         keys = [key for key in self.translator.keys() if key.startswith(prefix)]
    #     logging.info(f"Claves encontradas con prefijo '{prefix}': {keys}")
    #     return keys

    # # def append(self, key: str, text: str, package_number: int):
    # #     """
    # #     Agrega una nueva entrada asociada a una clave.
    # #     Cada entrada incluye el texto y el número de paquete.
    # #     """
    # #     file_path = self._get_file_path(key)
    # #     try:
    # #         with open(file_path, 'a') as f:
    # #             entry = {
    # #                 'package_number': package_number,
    # #                 'text': text
    # #             }
    # #             json_data = json.dumps(entry)
    # #             f.write(f"{len(json_data.encode('utf-8'))} {json_data}\n")
    # #         logging.info(f"Agregado nuevo valor a la clave {key}")
    # #     except Exception as e:
    # #         logging.info(f"Error al agregar la clave {key}: {e}")

    # def update(self, key: str, text: str, package_number: int):
    #     """
    #     Actualiza una entrada asociada a una clave.
    #     Si no existe, la crea.
    #     Incluye el texto y el número de paquete.
    #     """
    #     file_path = self._get_file_path(key)
    #     temp_path = file_path + ".tmp"
    #     try:
    #         # Leer el contenido existente
    #         data = {}
    #         if os.path.exists(file_path):
    #             with open(file_path, 'r') as f:
    #                 lines = f.readlines()
    #             for line in lines:
    #                 if not line.strip():
    #                     continue
    #                 try:
    #                     size_str, json_data = line.split(" ", 1)
    #                     size = int(size_str)
    #                     if len(json_data.encode('utf-8')) != size:
    #                         logging.info(f"Archivo corrupto para la clave {key}, omitiendo")
    #                         continue
    #                     entry = json.loads(json_data)
    #                     data = {
    #                         'package_number': entry.get('package_number', 0),
    #                         'text': entry.get('text', '')
    #                     }
    #                 except (ValueError, json.JSONDecodeError):
    #                     continue
    #         # Verificar si el paquete es más reciente
    #         if package_number >= data.get('package_number', 0):
    #             # Actualizar el valor
    #             data.update({
    #                 'package_number': package_number,
    #                 'text': text
    #             })
    #             # Escribir en el archivo temporal
    #             with open(temp_path, 'w') as f:
    #                 json_data = json.dumps(data)
    #                 f.write(f"{len(json_data.encode('utf-8'))} {json_data}\n")
    #             # Reemplazar el archivo original de manera atómica
    #             os.replace(temp_path, file_path)
    #             logging.info(f"Actualizado el valor de la clave {key}")
    #         else:
    #             # Ignorar actualizaciones de paquetes más antiguos
    #             logging.info(f"Ignorando actualización de paquete {package_number} para la clave {key}")
    #     except Exception as e:
    #         logging.info(f"Error al actualizar la clave {key}: {e}")
    #         if os.path.exists(temp_path):
    #             os.remove(temp_path)
