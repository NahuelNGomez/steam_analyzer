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
        self.middleware = Middleware(input_queues, [], output_exchanges, instance_id, self._callBack, self._finCallBack, 1, "fanout", "direct" )

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
            aux = data.split('\n')
            packet_id = aux[0]
            
            game_review = GameReview.decode(json.loads(aux[1]))
            result_text = game_review.review_text
            language, confidence = langid.classify(result_text)
            if language == 'en':
                game = GameReview(game_review.game_id, game_review.game_name, None, game_review.client_id)
                game_str = json.dumps(game.getData())
                data_to_send = packet_id + '\n' + game_str + '\n'
                self.middleware.send(data_to_send)
            else:
                logging.info("Mensaje no es en inglés, no se envía")
            
        except Exception as e:
            logging.error(f"Error en LanguageFilter callback: {e}")
    
    def _finCallBack(self, data):
        """
        Callback para manejar el mensaje de fin.

        :param data: Datos recibidos.
        """
        logging.info("LanguageFilter finished")
        self.middleware.send(data)
        
