import os
import json
import uuid
import threading
from typing import Any, List, Optional, Dict
import logging

TRANSLATOR_FILE = "translator.json"
TRANSLATOR_LOCK = threading.Lock()

"""
El archivo translator.json es una parte fundamental de la implementación del FaultManager. 
Su función es servir como un mapeador o traductor que asocia claves lógicas 
(como nombres de entidades, usuarios, o prefijos específicos) a nombres de archivos físicos 
en el sistema de persistencia. Esto permite gestionar las claves de manera lógica 
y evitar problemas relacionados con las limitaciones de nombres de archivos en el sistema operativo.

Propósito del translator.json
Mapeo Clave -> Archivo:

Dado que los nombres de los archivos están restringidos en longitud y caracteres permitidos, 
se usan UUIDs para garantizar unicidad y compatibilidad.
El translator.json asocia cada clave lógica con un archivo específico que almacena los datos persistentes.
Evitar Colisiones:

Si múltiples nodos o procesos intentan acceder o crear archivos con la misma clave, 
el traductor garantiza que todos apunten al mismo archivo.
"""

logging.basicConfig(level=logging.INFO)

class FaultManager:
    def __init__(self, storage_dir: str = "fault_data"):
        self.storage_dir = storage_dir
        os.makedirs(self.storage_dir, exist_ok=True)
        self.translator_path = os.path.join(self.storage_dir, TRANSLATOR_FILE)
        # Cargar o inicializar el traductor
        if not os.path.exists(self.translator_path):
            with open(self.translator_path, 'w') as f:
                json.dump({}, f)
            logging.info(f"Archivo {self.translator_path} creado")
        else:
            logging.info(f"Cargando archivo {self.translator_path}")
        self._load_translator()

    def _load_translator(self):
        with TRANSLATOR_LOCK:
            with open(self.translator_path, 'r') as f:
                self.translator = json.load(f)
            logging.info(f"Traductor cargado con {len(self.translator)} claves")

    def _save_translator(self):
        with TRANSLATOR_LOCK:
            temp_path = self.translator_path + ".tmp"
            with open(temp_path, 'w') as f:
                json.dump(self.translator, f)
            os.replace(temp_path, self.translator_path)
            logging.info(f"Traductor guardado con {len(self.translator)} claves")
            
    def _get_file_path(self, key: str) -> str:
        with TRANSLATOR_LOCK:
            if key not in self.translator:
                file_name = f"{uuid.uuid4()}.txt"
                self.translator[key] = file_name
                self._save_translator()
                logging.info(f"Generado nuevo archivo para la clave '{key}': {self.translator[key]}")
            else:
                file_name = self.translator[key]
        return os.path.join(self.storage_dir, file_name)

    def delete(self, key: str):
        file_path = self._get_file_path(key)
        with TRANSLATOR_LOCK:
            if key in self.translator:
                logging.info(f"Eliminando archivo para la clave '{key}': {self.translator[key]}")
                del self.translator[key]
                self._save_translator()
        if os.path.exists(file_path):
            os.remove(file_path)
            logging.info(f"Archivo eliminado: {file_path}")
        else: 
            logging.info(f"Archivo no encontrado para la clave {key}")

    def get(self, key: str) -> Optional[Dict[str, Any]]:
        file_path = self._get_file_path(key)
        if not os.path.exists(file_path):
            logging.info(f"Archivo no encontrado para la clave {key}")
            return None
        try:
            with open(file_path, 'r') as f:
                content = f.read()
            logging.info(f"Leído el archivo {file_path}")
            # Leer el tamaño de la línea y reconstruir el estado
            lines = content.split('\n')
            data = {}
            for line in lines:
                if not line.strip():
                    continue
                try:
                    size_str, json_data = line.split(" ", 1)
                    size = int(size_str)
                    if len(json_data.encode('utf-8')) != size:
                        # Archivo corrupto, omitir esta línea
                        logging.info(f"Archivo corrupto para la clave {key}, omitiendo")
                        continue
                    entry = json.loads(json_data)
                    # Suponiendo que cada entry tiene 'package_number' y 'text'
                    data = {
                        'package_number': entry.get('package_number', 0),
                        'text': entry.get('text', '')
                    }
                except (ValueError, json.JSONDecodeError) as e:
                    # Línea malformada, omitir
                    logging.info(f"Error al decodificar la línea '{line}': {e}")
                    continue
            return data if data else None
        except Exception as e:
            logging.info(f"Error al leer el archivo {file_path}: {e}")
            return None

    def get_keys(self, prefix: str) -> List[str]:
        with TRANSLATOR_LOCK:
            keys = [key for key in self.translator.keys() if key.startswith(prefix)]
        logging.info(f"Claves encontradas con prefijo '{prefix}': {keys}")
        return keys

    def append(self, key: str, text: str, package_number: int):
        """
        Agrega una nueva entrada asociada a una clave.
        Cada entrada incluye el texto y el número de paquete.
        """
        file_path = self._get_file_path(key)
        try:
            with open(file_path, 'a') as f:
                entry = {
                    'package_number': package_number,
                    'text': text
                }
                json_data = json.dumps(entry)
                f.write(f"{len(json_data.encode('utf-8'))} {json_data}\n")
            logging.info(f"Agregado nuevo valor a la clave {key}")
        except Exception as e:
            logging.info(f"Error al agregar la clave {key}: {e}")

    def update(self, key: str, text: str, package_number: int):
        """
        Actualiza una entrada asociada a una clave.
        Si no existe, la crea.
        Incluye el texto y el número de paquete.
        """
        file_path = self._get_file_path(key)
        temp_path = file_path + ".tmp"
        try:
            # Leer el contenido existente
            data = {}
            if os.path.exists(file_path):
                with open(file_path, 'r') as f:
                    lines = f.readlines()
                for line in lines:
                    if not line.strip():
                        continue
                    try:
                        size_str, json_data = line.split(" ", 1)
                        size = int(size_str)
                        if len(json_data.encode('utf-8')) != size:
                            logging.info(f"Archivo corrupto para la clave {key}, omitiendo")
                            continue
                        entry = json.loads(json_data)
                        data = {
                            'package_number': entry.get('package_number', 0),
                            'text': entry.get('text', '')
                        }
                    except (ValueError, json.JSONDecodeError):
                        continue
            # Verificar si el paquete es más reciente
            if package_number >= data.get('package_number', 0):
                # Actualizar el valor
                data.update({
                    'package_number': package_number,
                    'text': text
                })
                # Escribir en el archivo temporal
                with open(temp_path, 'w') as f:
                    json_data = json.dumps(data)
                    f.write(f"{len(json_data.encode('utf-8'))} {json_data}\n")
                # Reemplazar el archivo original de manera atómica
                os.replace(temp_path, file_path)
                logging.info(f"Actualizado el valor de la clave {key}")
            else:
                # Ignorar actualizaciones de paquetes más antiguos
                logging.info(f"Ignorando actualización de paquete {package_number} para la clave {key}")
        except Exception as e:
            logging.info(f"Error al actualizar la clave {key}: {e}")
            if os.path.exists(temp_path):
                os.remove(temp_path)
