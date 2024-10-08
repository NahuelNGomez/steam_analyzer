import json
import logging
from collections import defaultdict
from common.middleware import Middleware
from common.utils import split_complex_string
import langid

class LanguageFilter:
    def __init__(self, input_queues, output_exchanges, instance_id):
        """
        Inicializa el filtro con los parámetros especificados.

        :param input_queues: Diccionario de colas de entrada.
        :param output_exchanges: Lista de exchanges de salida.
        :param instance_id: ID de instancia para identificar colas únicas.
        """
        self.middleware = Middleware(input_queues, [], output_exchanges, instance_id, self._callBack, self._finCallBack)
    

    def start(self):
        """
        Inicia el filtro.
        """
        langid.set_languages(['en'])  
        self.middleware.start()
        logging.info("LanguageFilter started")
        
    def _callBack(self, data):
        """
        Callback para procesar los mensajes recibidos.

        :param data: Datos recibidos.
        """
        try:
            result = split_complex_string(data)
            result_text = result[2]
            language, confidence = langid.classify(result_text)
            print(f"Language: {language}, Confidence: {confidence}, Text {result_text}", flush=True)
            if language == 'en':
                self.middleware.send(data)
            else:
                print("Mensaje no es en inglés, no se envía", flush=True)
            
            print(f"Mensaje decodificado: {result}", flush=True)
            
        except Exception as e:
            logging.error(f"Error en LanguageFilter callback: {e}")
    
    def _finCallBack(self, data):
        """
        Callback para manejar el mensaje de fin.

        :param data: Datos recibidos.
        """
        print("Fin de la transmisión, enviando data", data, flush=True)
        #self.middleware.send(data)
        