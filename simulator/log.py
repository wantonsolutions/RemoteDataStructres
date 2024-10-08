import logging

class CustomFormatter(logging.Formatter):

    grey = "\033[37m"
    green = "\033[32m"
    yellow = "\033[33m"
    red = "\033[91m"
    bold_red = "\033[31m"
    reset = "\033[0m"
    #format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s (%(filename)s:%(lineno)d)"
    #format="%(levelname)s\t %(filename)-15s:%(lineno)-15s %(message)s"
    format="%(levelname)-9s:%(filename)-15s %(message)s"

    FORMATS = {
        logging.DEBUG: grey + format + reset,
        logging.INFO: green + format + reset,
        logging.WARNING: yellow + format + reset,
        logging.ERROR: red + format + reset,
        logging.CRITICAL: bold_red + format + reset
    }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)


def setup_custom_logger(name):
    logger = logging.getLogger(name)
    #logger.setLevel(logging.DEBUG)
    logger.setLevel(logging.INFO)
    ch = logging.StreamHandler()
    ch.setFormatter(CustomFormatter())
    logger.addHandler(ch)
    return logger

def set_off():
    logger = logging.getLogger('root')
    logger.setLevel(logging.CRITICAL)

def set_debug():
    logger = logging.getLogger('root')
    logger.setLevel(logging.DEBUG)