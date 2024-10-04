import logging
import colorlog

def setup_logging(level):
    """
    Configura el logging con colores según el nivel de los mensajes.
    
    :param level: Nivel de logging (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    """
    numeric_level = getattr(logging, level.upper(), None)
    if not isinstance(numeric_level, int):
        numeric_level = logging.INFO

    log_format = (
        '%(asctime)s - %(log_color)s%(levelname)s%(reset)s - %(message)s'
    )

    colorlog_format = colorlog.ColoredFormatter(
        log_format,
        datefmt='%Y-%m-%d %H:%M:%S',
        reset=True,
        log_colors={
            'DEBUG': 'cyan',
            'INFO': 'green',
            'WARNING': 'yellow',
            'ERROR': 'red',
            'CRITICAL': 'bold_red',
        },
        secondary_log_colors={},
        style='%'
    )

    handler = logging.StreamHandler()
    handler.setFormatter(colorlog_format)

    logging.basicConfig(
        level=numeric_level,
        handlers=[handler]
    )
