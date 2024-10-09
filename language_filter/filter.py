import json
import logging
from collections import defaultdict
from common.game_review import GameReview
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
            game_review = GameReview.decode(json.loads(data))
            result_text = game_review.review_text
            language, confidence = langid.classify(result_text)
            print(f"Language: {language}, Confidence: {confidence}, Text {result_text}", flush=True)
            if language == 'en':
                game = GameReview(game_review.game_id, game_review.game_name, None)
                game_str = json.dumps(game.getData())
                self.middleware.send(game_str)
            else:
                print("Mensaje no es en inglés, no se envía", flush=True)
            
            print(f"Mensaje decodificado: {result_text}", flush=True)
            
        except Exception as e:
            logging.error(f"Error en LanguageFilter callback: {e}")
    
    def _finCallBack(self, data):
        """
        Callback para manejar el mensaje de fin.

        :param data: Datos recibidos.
        """
        print("Fin de la transmisión, enviando data", data, flush=True)
        #self.middleware.send(data)
        